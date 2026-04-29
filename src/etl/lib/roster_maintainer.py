"""
The Glass - Roster Synchronization

Maintains the two membership junctions in the ``core`` schema:

    core.league_rosters  : (league_id, team_id, is_active, ...)
    core.team_rosters    : (team_id,   player_id, is_active, ...)

A roster sync run takes a snapshot of (team_source_id, player_source_id)
pairs from the league's reader source, resolves each to a ``the_glass_id``
via the corresponding ``core.*_profiles`` table, and:

    1. Upserts every (league, team) pair into league_rosters  (is_active=TRUE).
    2. Upserts every (team, player) pair into team_rosters    (is_active=TRUE).
    3. Deactivates rows that existed before but are absent from the snapshot.

The module is source-agnostic: callers (typically the runner) supply the
already-fetched roster pairs.  Source-specific endpoint logic stays in the
respective source client.
"""

import logging
from typing import Any, Dict, Iterable, List, Set, Tuple

from src.core.db import db_connection, quote_col
from src.etl.definitions import (
    CORE_SCHEMA,
    LEAGUES,
    THE_GLASS_ID_COLUMN,
    get_source_id_column,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# League profile bootstrap
# ---------------------------------------------------------------------------

def ensure_league_profile(league_key: str, conn: Any) -> int:
    """Ensure ``core.league_profiles`` has a row for ``league_key`` and return
    its ``the_glass_id``.

    The row's ``key`` column anchors the league_key -> the_glass_id mapping
    and is the only stable reference between config and database.
    """
    if league_key not in LEAGUES:
        raise ValueError(f"Unknown league: {league_key!r}")

    league = LEAGUES[league_key]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {quote_col(THE_GLASS_ID_COLUMN)} "
            f"FROM {CORE_SCHEMA}.league_profiles WHERE key = %s",
            (league_key,),
        )
        row = cur.fetchone()
        if row is not None:
            return int(row[0])

        cur.execute(
            f"INSERT INTO {CORE_SCHEMA}.league_profiles (key, name, abbr) "
            f"VALUES (%s, %s, %s) RETURNING {quote_col(THE_GLASS_ID_COLUMN)}",
            (league_key, league['name'], league['abbr']),
        )
        new_id = int(cur.fetchone()[0])
        logger.info(
            'Registered league %r in core.league_profiles (the_glass_id=%d)',
            league_key, new_id,
        )
        return new_id


# ---------------------------------------------------------------------------
# Source -> the_glass_id resolution
# ---------------------------------------------------------------------------

def _resolve_glass_ids(
    conn: Any,
    profile_table: str,
    source_id_col: str,
    source_ids: Iterable[Any],
) -> Dict[str, int]:
    """Return ``{str(source_id): the_glass_id}`` for the given source IDs."""
    ids = [v for v in source_ids if v is not None]
    if not ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {quote_col(source_id_col)}, {quote_col(THE_GLASS_ID_COLUMN)} "
            f"FROM {profile_table} "
            f"WHERE {quote_col(source_id_col)} = ANY(%s)",
            (ids,),
        )
        return {str(row[0]): int(row[1]) for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Junction upsert primitives
# ---------------------------------------------------------------------------

def _upsert_active(
    conn: Any,
    table: str,
    pk_columns: Tuple[str, str],
    rows: Iterable[Tuple[int, int]],
) -> int:
    """Insert each (a, b) pair as is_active=TRUE; on conflict re-activate."""
    rows = list(rows)
    if not rows:
        return 0
    a_col, b_col = pk_columns
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {table} ({quote_col(a_col)}, {quote_col(b_col)}, is_active)
            VALUES (%s, %s, TRUE)
            ON CONFLICT ({quote_col(a_col)}, {quote_col(b_col)}) DO UPDATE
            SET is_active = TRUE, updated_at = NOW()
            """,
            rows,
        )
    return len(rows)


def _deactivate_missing(
    conn: Any,
    table: str,
    pk_columns: Tuple[str, str],
    scope_filter_col: str,
    scope_filter_value: int,
    keep_pairs: Set[Tuple[int, int]],
) -> int:
    """Mark every active row in ``table`` whose pair is NOT in ``keep_pairs``
    as ``is_active = FALSE``.  Scoped to ``scope_filter_col = scope_filter_value``
    so the function only ever touches one league or one team at a time.
    """
    a_col, b_col = pk_columns
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {quote_col(a_col)}, {quote_col(b_col)}
            FROM {table}
            WHERE {quote_col(scope_filter_col)} = %s AND is_active = TRUE
            """,
            (scope_filter_value,),
        )
        existing = {(int(r[0]), int(r[1])) for r in cur.fetchall()}
        to_deactivate = existing - keep_pairs
        if not to_deactivate:
            return 0
        cur.executemany(
            f"""
            UPDATE {table} SET is_active = FALSE, updated_at = NOW()
            WHERE {quote_col(a_col)} = %s AND {quote_col(b_col)} = %s
            """,
            list(to_deactivate),
        )
    return len(to_deactivate)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sync_rosters(
    league_key: str,
    source_key: str,
    roster_pairs: Iterable[Tuple[Any, Any]],
) -> Dict[str, int]:
    """Apply a roster snapshot to ``core.league_rosters`` and ``core.team_rosters``.

    Args:
        league_key:    LEAGUES key (e.g. ``'nba'``).
        source_key:    SOURCES key (e.g. ``'nba_api'``).
        roster_pairs:  Iterable of ``(team_source_id, player_source_id)`` tuples
                       representing every active roster slot in the league.

    Returns:
        Dict with the per-junction counters
        ``{teams_active, players_active, teams_deactivated, players_deactivated,
           teams_unresolved, players_unresolved}``.
    """
    pairs: List[Tuple[Any, Any]] = [
        (t, p) for t, p in roster_pairs if t is not None and p is not None
    ]

    src_col = get_source_id_column(source_key)
    teams_table = f'{CORE_SCHEMA}.team_profiles'
    players_table = f'{CORE_SCHEMA}.player_profiles'
    league_rosters = f'{CORE_SCHEMA}.league_rosters'
    team_rosters = f'{CORE_SCHEMA}.team_rosters'

    with db_connection() as conn:
        league_glass_id = ensure_league_profile(league_key, conn)

        team_source_ids = {t for t, _ in pairs}
        player_source_ids = {p for _, p in pairs}

        team_map = _resolve_glass_ids(conn, teams_table, src_col, team_source_ids)
        player_map = _resolve_glass_ids(conn, players_table, src_col, player_source_ids)

        teams_unresolved = team_source_ids - set(team_map)
        players_unresolved = player_source_ids - set(player_map)
        if teams_unresolved:
            logger.warning(
                '%d unknown team source_ids (skipping): %s',
                len(teams_unresolved), sorted(teams_unresolved)[:10],
            )
        if players_unresolved:
            logger.warning(
                '%d unknown player source_ids (skipping): %s',
                len(players_unresolved), sorted(players_unresolved)[:10],
            )

        # ---- league_rosters: (league_glass_id, team_glass_id) -----------------
        league_team_pairs: Set[Tuple[int, int]] = {
            (league_glass_id, team_map[str(t)])
            for t in team_source_ids
            if str(t) in team_map
        }
        teams_active = _upsert_active(
            conn, league_rosters, ('league_id', 'team_id'), league_team_pairs,
        )
        teams_deactivated = _deactivate_missing(
            conn, league_rosters, ('league_id', 'team_id'),
            scope_filter_col='league_id',
            scope_filter_value=league_glass_id,
            keep_pairs=league_team_pairs,
        )

        # ---- team_rosters: (team_glass_id, player_glass_id) -------------------
        team_player_pairs: Set[Tuple[int, int]] = set()
        for team_src, player_src in pairs:
            t = team_map.get(str(team_src))
            p = player_map.get(str(player_src))
            if t is not None and p is not None:
                team_player_pairs.add((t, p))

        players_active = _upsert_active(
            conn, team_rosters, ('team_id', 'player_id'), team_player_pairs,
        )

        # Per-team deactivation: each team's slate is independent.
        teams_in_snapshot: Set[int] = {t for t, _ in team_player_pairs}
        players_deactivated = 0
        for team_glass_id in teams_in_snapshot:
            keep = {pair for pair in team_player_pairs if pair[0] == team_glass_id}
            players_deactivated += _deactivate_missing(
                conn, team_rosters, ('team_id', 'player_id'),
                scope_filter_col='team_id',
                scope_filter_value=team_glass_id,
                keep_pairs=keep,
            )

    counts = {
        'teams_active': teams_active,
        'teams_deactivated': teams_deactivated,
        'teams_unresolved': len(teams_unresolved),
        'players_active': players_active,
        'players_deactivated': players_deactivated,
        'players_unresolved': len(players_unresolved),
    }
    logger.info(
        'Roster sync %s/%s: teams active=%d deactivated=%d unresolved=%d | '
        'players active=%d deactivated=%d unresolved=%d',
        league_key, source_key,
        counts['teams_active'], counts['teams_deactivated'], counts['teams_unresolved'],
        counts['players_active'], counts['players_deactivated'], counts['players_unresolved'],
    )
    return counts
