"""The Glass - Roster Stager

Roster snapshots are merged into the transient staging schema. Promotion into
core profile and roster tables happens in the entity matching phase.
"""

import logging
from typing import Any, Dict, Iterable, List, Tuple

from src.etl.lib.load import merge_staged_entity_rows

logger = logging.getLogger(__name__)


def _normalize_roster_snapshot(
    roster_pairs: Iterable[Tuple[Any, ...]],
) -> List[Tuple[Any, Any, Any]]:
    """Normalize roster snapshots to ``(team_source_id, player_source_id, jersey_num)``."""
    normalized: List[Tuple[Any, Any, Any]] = []
    for entry in roster_pairs:
        if not isinstance(entry, (tuple, list)):
            continue
        if len(entry) < 2:
            continue
        team_source_id, player_source_id = entry[0], entry[1]
        jersey_num = entry[2] if len(entry) >= 3 else None
        if team_source_id is None or player_source_id is None:
            continue
        normalized.append((team_source_id, player_source_id, jersey_num))
    return normalized


def stage_rosters(
    league_key: str,
    source_key: str,
    roster_pairs: Iterable[Tuple[Any, ...]],
    season: str = None,
) -> Dict[str, int]:
    """Merge roster snapshots into ``staging.teams`` and ``staging.players``."""
    pairs = _normalize_roster_snapshot(roster_pairs)
    team_rows: Dict[Any, Dict[str, Any]] = {}
    player_rows: Dict[Any, Dict[str, Any]] = {}

    for team_source_id, player_source_id, jersey_num in pairs:
        team_rows.setdefault(team_source_id, {})
        player_rows[player_source_id] = {
            'team_source_id': str(team_source_id),
            'jersey_num': str(jersey_num) if jersey_num is not None else None,
        }

    teams_staged = merge_staged_entity_rows('team', team_rows, league_key, source_key)
    players_staged = merge_staged_entity_rows('player', player_rows, league_key, source_key)

    counts = {
        'teams_staged': teams_staged,
        'players_staged': players_staged,
    }
    logger.info(
        'Roster stage %s/%s: teams staged=%d | players staged=%d',
        league_key, source_key,
        counts['teams_staged'], counts['players_staged'],
    )
    return counts
