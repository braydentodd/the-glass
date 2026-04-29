# ruff: noqa: E402  -- load_dotenv() must run before src.* imports that read os.getenv at module load.
"""
The Glass - ETL Runner

League-scoped orchestrator that wires source-specific clients and configs
to the source-agnostic core modules:

  - core/plan.py      - call group construction
  - core/extract.py   - field extraction from API responses
  - core/transform.py - type conversion / pipeline engine / aggregation
  - core/load.py      - the_glass_id-aware database writes
  - core/rosters.py   - league_rosters / team_rosters maintenance
  - core/cleanup.py   - per-league retention + cross-league orphan sweep
  - core/ddl.py       - schema bootstrap (core + per-league)

Usage:
    python -m src.etl.runner --league nba                     # full run
    python -m src.etl.runner --league nba --season 2023-24    # specific season
    python -m src.etl.runner --league nba --season-type po    # Playoffs
    python -m src.etl.runner --league nba --entity team
    python -m src.etl.runner --league nba --endpoint leaguedashptstats
    python -m src.etl.runner --league nba --phase rosters
    python -m src.etl.runner --phase orphan                   # cross-league

Phases (per-league unless noted):
    full        discover -> rosters -> backfill -> update -> cleanup -> retention
    discover    populate core profiles for the current season
    rosters     resync core.league_rosters and core.team_rosters
    backfill    fetch stats for every season in the retention window
    update      refresh stats for the current season only
    prune       per-league retention (delete stats older than window)
    orphan      cross-league orphan profile sweep (no league_key needed)
"""

import argparse
import importlib
import logging
import warnings
from typing import Any, Callable, Dict, List, Optional

from dotenv import load_dotenv
load_dotenv()

from src.core.db import db_connection, quote_col
from src.etl.core.cleanup import (
    cleanup_stat_domains,
    prune_orphan_profiles,
    prune_stats_retention,
)
from src.etl.core.config_validation import validate_config
from src.etl.core.ddl import ensure_all
from src.etl.core.executor import ExecutionContext, execute_group
from src.etl.core.load import seed_empty_stats
from src.etl.core.plan import build_call_groups
from src.etl.core.progress_tracker import (
    complete_run,
    fail_run,
    mark_group_completed,
    mark_group_failed,
    mark_group_started,
    resolve_work,
    update_run_completed_groups,
)
from src.etl.core.rosters import ensure_league_profile, sync_rosters
from src.etl.definitions import (
    CORE_SCHEMA,
    ETL_CONFIG,
    LEAGUES,
    SOURCES,
    THE_GLASS_ID_COLUMN,
    get_current_season_for_league,
    get_reader_source,
    get_retained_seasons,
    get_source_id_column,
)

warnings.filterwarnings(
    'ignore',
    message='Failed to return connection to pool',
    module='urllib3',
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# ============================================================================
# DYNAMIC SOURCE LOADING
# ============================================================================

def _load_source(source_key: str):
    """Dynamically import a source's config and client modules."""
    if source_key not in SOURCES:
        raise ValueError(
            f"Unknown source '{source_key}'. Registered: {sorted(SOURCES)}"
        )
    config_mod = importlib.import_module(f'src.etl.sources.{source_key}.config')
    client_mod = importlib.import_module(f'src.etl.sources.{source_key}.client')
    return config_mod, client_mod


# ============================================================================
# CORE-AWARE LOOKUPS
# ============================================================================

def _get_active_team_source_ids(league_key: str, source_key: str) -> Dict[str, int]:
    """Return ``{team_abbr: team_source_id}`` for teams currently active in
    ``core.league_rosters`` for the given league.

    Used by per-team execution strategies that need to iterate over the team
    list (e.g. on/off court endpoints).
    """
    src_col = get_source_id_column(source_key)
    sql = f"""
        SELECT t.abbr, t.{quote_col(src_col)}
          FROM {CORE_SCHEMA}.team_profiles t
          JOIN {CORE_SCHEMA}.league_rosters lr
            ON lr.team_id = t.{quote_col(THE_GLASS_ID_COLUMN)}
          JOIN {CORE_SCHEMA}.league_profiles lp
            ON lp.{quote_col(THE_GLASS_ID_COLUMN)} = lr.league_id
         WHERE lp.key = %s AND lr.is_active = TRUE
         ORDER BY t.abbr
    """
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (league_key,))
            return {
                row[0]: int(row[1])
                for row in cur.fetchall()
                if row[0] and row[1] is not None
            }


# ============================================================================
# SHARED EXECUTION ENGINE
# ============================================================================

def _run_groups(
    run_type: str,
    scope: str,
    entities: List[str],
    seasons: List[str],
    season_type: str,
    season_type_name: str,
    team_ids: Dict[str, int],
    endpoint_filter: Optional[str],
    failed: List[Dict[str, Any]],
    *,
    league_key: str,
    source_key: str,
    endpoints: dict,
    api_field_names: dict,
    api_config: dict,
    make_fetcher: Callable,
) -> int:
    """Execute call groups for a given scope across entities and seasons."""
    total_rows = 0

    for season in seasons:
        for ent in entities:
            groups = build_call_groups(
                ent, season, source_key, endpoints, scope=scope,
            )
            if endpoint_filter:
                groups = [g for g in groups if g['endpoint'] == endpoint_filter]
            if not groups:
                continue

            logger.info(
                '%s: %s %s — %d call groups', run_type, ent, season, len(groups),
            )

            ctx = ExecutionContext(
                entity=ent,
                scope=scope,
                season=season,
                season_type=season_type,
                season_type_name=season_type_name,
                entity_id_field=api_field_names['entity_id'][ent],
                db_schema=league_key,
                source_key=source_key,
                api_fetcher=make_fetcher(season, season_type_name, ent),
                team_ids=team_ids,
                rate_limit_delay=api_config.get('rate_limit_delay', 1.2),
                max_consecutive_failures=api_config.get('max_consecutive_failures', 5),
                id_aliases=api_field_names.get('id_aliases', {}),
            )

            with db_connection() as conn:
                run_id, work_items = resolve_work(
                    conn, league_key, ent, season, season_type, groups, run_type,
                    ETL_CONFIG['auto_resume'],
                )

                entity_rows = 0
                try:
                    for group, progress_id in work_items:
                        mark_group_started(conn, league_key, progress_id)
                        try:
                            rows = execute_group(group, ctx, failed)
                            entity_rows += rows
                            mark_group_completed(conn, league_key, progress_id, rows)
                        except Exception as exc:
                            logger.error('Group %s failed: %s', group['endpoint'], exc)
                            mark_group_failed(conn, league_key, progress_id, str(exc))
                            failed.append({
                                'endpoint': group['endpoint'], 'error': str(exc),
                            })

                    total_rows += entity_rows
                    update_run_completed_groups(conn, league_key, run_id)
                    complete_run(conn, league_key, run_id, entity_rows)
                except Exception as exc:
                    fail_run(conn, league_key, run_id, str(exc))
                    raise

    return total_rows


# ============================================================================
# ETL PHASES
# ============================================================================

def _discover_entities(
    entities: List[str],
    season: str,
    season_type: str,
    season_type_name: str,
    team_ids: Dict[str, int],
    failed: List[Dict[str, Any]],
    **source_kw,
) -> int:
    """Phase: populate core.{entity}_profiles from current-season data."""
    logger.info('Phase: discover')
    return _run_groups(
        'discover', 'entity', entities, [season],
        season_type, season_type_name, team_ids, None, failed,
        **source_kw,
    )


def _sync_rosters_phase(
    league_key: str,
    source_key: str,
    season: str,
    season_type_name: str,
    client_mod: Any,
    failed: List[Dict[str, Any]],
) -> int:
    """Phase: pull a roster snapshot from the source and apply to junctions.

    The source client must expose ``fetch_roster_snapshot(league_key, season,
    season_type_name)`` returning an iterable of ``(team_source_id,
    player_source_id)`` tuples.  Sources that don't support this skip silently.
    """
    logger.info('Phase: rosters')

    fetcher = getattr(client_mod, 'fetch_roster_snapshot', None)
    if fetcher is None:
        logger.warning(
            'Source %r has no fetch_roster_snapshot(); skipping roster sync',
            source_key,
        )
        return 0

    try:
        roster_pairs = fetcher(league_key, season, season_type_name)
    except Exception as exc:
        logger.error('Roster snapshot for %s/%s failed: %s', league_key, source_key, exc)
        failed.append({
            'phase': 'rosters', 'league': league_key, 'error': str(exc),
        })
        return 0

    counts = sync_rosters(league_key, source_key, roster_pairs)
    return counts['teams_active'] + counts['players_active']


def _backfill(
    entities: List[str],
    seasons: List[str],
    season_type: str,
    season_type_name: str,
    team_ids: Dict[str, int],
    endpoint_filter: Optional[str],
    failed: List[Dict[str, Any]],
    **source_kw,
) -> int:
    """Phase: fetch stats for every season in the retention window."""
    logger.info('Phase: backfill (%d seasons)', len(seasons))
    return _run_groups(
        'backfill', 'stats', entities, seasons,
        season_type, season_type_name, team_ids, endpoint_filter, failed,
        **source_kw,
    )


def _update_current(
    entities: List[str],
    season: str,
    season_type: str,
    season_type_name: str,
    team_ids: Dict[str, int],
    endpoint_filter: Optional[str],
    failed: List[Dict[str, Any]],
    **source_kw,
) -> int:
    """Phase: refresh stats for the current season only."""
    logger.info('Phase: update')
    return _run_groups(
        'update', 'stats', entities, [season],
        season_type, season_type_name, team_ids, endpoint_filter, failed,
        **source_kw,
    )


# ============================================================================
# ORCHESTRATOR
# ============================================================================

VALID_PHASES = {'full', 'discover', 'rosters', 'backfill', 'update', 'prune', 'orphan'}


def run_etl(
    league_key: Optional[str] = None,
    phase: str = 'full',
    entity: str = 'all',
    endpoint_filter: Optional[str] = None,
    season: Optional[str] = None,
    season_type: str = 'rs',
) -> None:
    """Main ETL entry point.

    Args:
        league_key:      Registered league key (e.g. ``'nba'``).  Required for
                         every phase except ``'orphan'``.
        phase:           Execution phase (see VALID_PHASES).
        entity:          ``'player'``, ``'team'``, or ``'all'``.
        endpoint_filter: If set, only process this one endpoint.
        season:          Season label.  Defaults to the league's current season.
        season_type:     ``'rs'`` / ``'po'`` / ``'pi'``.
    """
    if phase not in VALID_PHASES:
        raise ValueError(f"Invalid phase '{phase}'. Must be one of {sorted(VALID_PHASES)}")

    # Cross-league orphan sweep is a special case (no league required).
    if phase == 'orphan':
        out = prune_orphan_profiles()
        logger.info('Orphan sweep complete: %s', out)
        return

    if not league_key:
        raise ValueError(f"Phase '{phase}' requires --league")
    if league_key not in LEAGUES:
        raise ValueError(f"Unknown league '{league_key}'. Registered: {sorted(LEAGUES)}")

    source_key = get_reader_source(league_key)
    config_mod, client_mod = _load_source(source_key)

    season_types = config_mod.SEASON_TYPES
    endpoints = config_mod.ENDPOINTS
    endpoints_schema = config_mod.ENDPOINTS_SCHEMA
    api_config = config_mod.API_CONFIG
    api_field_names = config_mod.API_FIELD_NAMES

    season = season or get_current_season_for_league(league_key)
    st_info = season_types.get(season_type, season_types['rs'])
    season_type_name = st_info['name']

    logger.info(
        'ETL starting: league=%s source=%s phase=%s season=%s type=%s entity=%s',
        league_key, source_key, phase, season, season_type_name, entity,
    )

    if hasattr(config_mod, 'validate_provider_config'):
        config_mod.validate_provider_config()
    validate_config(endpoints, endpoints_schema)

    # Schema bootstrap (idempotent).
    ensure_all(league_key)
    with db_connection() as conn:
        ensure_league_profile(league_key, conn)

    entities = ['team', 'player'] if entity == 'all' else [entity]
    season_range = get_retained_seasons(league_key, season)
    team_ids = _get_active_team_source_ids(league_key, source_key)
    failed: List[Dict[str, Any]] = []
    total_rows = 0

    source_kw = dict(
        league_key=league_key,
        source_key=source_key,
        endpoints=endpoints,
        api_field_names=api_field_names,
        api_config=api_config,
        make_fetcher=client_mod.make_fetcher,
    )

    if phase in ('full', 'discover'):
        total_rows += _discover_entities(
            entities, season, season_type, season_type_name, team_ids, failed,
            **source_kw,
        )

    if phase in ('full', 'discover', 'rosters'):
        total_rows += _sync_rosters_phase(
            league_key, source_key, season, season_type_name, client_mod, failed,
        )
        # Seed empty RS stats rows for active rosters; PO/PI seeded only when
        # those season types are explicitly backfilled.
        for ent in entities:
            total_rows += seed_empty_stats(ent, season, 'rs', league_key)

    if phase in ('full', 'backfill'):
        total_rows += _backfill(
            entities, season_range, season_type, season_type_name,
            team_ids, endpoint_filter, failed, **source_kw,
        )

    if phase in ('full', 'update'):
        total_rows += _update_current(
            entities, season, season_type, season_type_name,
            team_ids, endpoint_filter, failed, **source_kw,
        )

    # Domain coherency cleanup -- only after stats writes
    if phase in ('full', 'backfill', 'update') and not endpoint_filter:
        seasons_to_clean = season_range if phase in ('full', 'backfill') else [season]
        for s in seasons_to_clean:
            for ent in entities:
                total_rows += cleanup_stat_domains(league_key, ent, s, season_type)

    if phase in ('full', 'prune'):
        total_rows += prune_stats_retention(league_key, season)

    logger.info('ETL complete: %d total rows written/pruned', total_rows)

    if failed:
        logger.warning('%d failures:', len(failed))
        for f in failed:
            logger.warning('  %s', f)


# ============================================================================
# CLI
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description='The Glass - ETL Pipeline')
    parser.add_argument(
        '--league', type=str, default=None, choices=sorted(LEAGUES),
        help='League key (required for every phase except "orphan")',
    )
    parser.add_argument(
        '--phase', type=str, default='full', choices=sorted(VALID_PHASES),
        help='ETL phase to run (default: full)',
    )
    parser.add_argument('--season', type=str, default=None)
    parser.add_argument('--season-type', type=str, default='rs', choices=['rs', 'po', 'pi'])
    parser.add_argument('--entity', type=str, default='all', choices=['player', 'team', 'all'])
    parser.add_argument('--endpoint', type=str, default=None)
    args = parser.parse_args()

    run_etl(
        league_key=args.league,
        phase=args.phase,
        entity=args.entity,
        endpoint_filter=args.endpoint,
        season=args.season,
        season_type=args.season_type,
    )


if __name__ == '__main__':
    main()
