"""
The Glass - Global ETL Pipeline Policy

One shared ETL policy for all leagues and sources.
Keep this intentionally small: define step behavior once, then map phases
to ordered step keys.
"""

from typing import Any, Dict, List


VALID_ETL_PHASES = frozenset({'full', 'discover', 'rosters', 'backfill', 'update', 'prune'})
VALID_ETL_STEP_HANDLERS = frozenset({
    'populate_profiles',
    'sync_rosters',
    'backfill_stats',
    'update_current_stats',
    'normalize_stats_domains',
    'prune_stats_retention',
})
VALID_SEASON_WINDOWS = frozenset({'none', 'current', 'retained', 'historical'})
VALID_SEASON_TYPE_MODES = frozenset({'none', 'regular', 'requested'})
VALID_ENTITY_MATCHER_MODES = frozenset({
    'approve_all',
    'drop_missing_source_id',
    'drop_blocked_source_ids',
})




ENTITY_MATCHER_POLICY: Dict[str, Any] = {
    'default_mode': 'approve_all',
    'entity_rules': {},
}


PIPELINE_STEPS: Dict[str, Dict[str, Any]] = {
    'populate_profiles_discover_retained': {
        'handler': 'populate_profiles',
        'season_window': 'current',
        'season_type_mode': 'regular',
    },
    'populate_profiles_retained': {
        'handler': 'populate_profiles',
        'season_window': 'retained',
        'season_type_mode': 'regular',
    },
    'sync_rosters_current': {
        'handler': 'sync_rosters',
        'season_window': 'current',
        'season_type_mode': 'regular',
    },
    'populate_profiles_discover': {
        'handler': 'populate_profiles',
        'season_window': 'current',
        'season_type_mode': 'regular',
    },
    'populate_profiles_update': {
        'handler': 'populate_profiles',
        'season_window': 'current',
        'season_type_mode': 'regular',
    },
    'backfill_stats': {
        'handler': 'backfill_stats',
        'season_window': 'historical',
        'season_type_mode': 'requested',
    },
    'update_current_stats': {
        'handler': 'update_current_stats',
        'season_window': 'current',
        'season_type_mode': 'requested',
    },
    'normalize_stats_domains_backfill': {
        'handler': 'normalize_stats_domains',
        'season_window': 'historical',
        'season_type_mode': 'requested',
    },
    'normalize_stats_domains_update': {
        'handler': 'normalize_stats_domains',
        'season_window': 'current',
        'season_type_mode': 'requested',
    },
    'prune_stats_retention': {
        'handler': 'prune_stats_retention',
        'season_window': 'current',
        'season_type_mode': 'none',
    },
}


# Phases are ordered execution macros over shared step keys.
PIPELINE_PHASES: Dict[str, List[str]] = {
    'full': [
        'sync_rosters_current',
        'populate_profiles_discover',
        'backfill_stats',
        'update_current_stats',
        'normalize_stats_domains_backfill',
    ],
    'discover': [
        'sync_rosters_current',
        'populate_profiles_discover',
    ],
    'rosters': ['sync_rosters_current'],
    'backfill': [
        'populate_profiles_retained',
        'backfill_stats',
        'normalize_stats_domains_backfill',
    ],
    'update': [
        'populate_profiles_update',
        'update_current_stats',
        'normalize_stats_domains_update',
    ],
    'prune': ['prune_stats_retention'],
}
