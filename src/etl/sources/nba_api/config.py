"""
The Glass - NBA API Source Configuration

Pure data definitions for the ``nba_api`` source: endpoint metadata,
rate limits, season-type mapping, and field-name mappings.

League-level concerns (current season, retention window, calendar flip)
live in :mod:`src.etl.definitions.config.LEAGUES` -- this module is purely
about how to talk to the source itself.
"""

from typing import Any, Dict


# ============================================================================
# SOURCE METADATA
# ============================================================================

SOURCE_META: Dict[str, Any] = {
    'source_key': 'nba_api',
    # Endpoint whose rows define the active roster snapshot for a league.
    # The client's fetch_roster_snapshot() pulls (TEAM_ID, PERSON_ID) tuples
    # from this endpoint.
    'roster_endpoint': 'commonallplayers',
}


SEASON_TYPES = {
    'rs': {'name': 'Regular Season', 'param': 'Regular Season', 'min_season': None},
    'po': {'name': 'Playoffs',       'param': 'Playoffs',       'min_season': None},
    'pi': {'name': 'PlayIn',         'param': 'PlayIn',         'min_season': '2020-21'},
}


# ============================================================================
# API OPERATIONAL SETTINGS
# ============================================================================

API_CONFIG = {
    'rate_limit_delay': 1.2,
    'per_player_rate_limit': 2.5,
    'timeout_default': 30,
    'timeout_bulk': 120,
    'backoff_divisor': 5,
    'cooldown_after_batch_seconds': 30,
    'max_consecutive_failures': 5,

    'roster_batch_size': 175,
    'roster_batch_cooldown': 120,

    'league_id': '00',
    'per_mode_simple': 'Totals',
    'per_mode_time': 'Totals',
    'per_mode_detailed': 'Totals',
    'last_n_games': '0',
    'month': '0',
    'opponent_team_id': '0',
    'period': '0',
}

RETRY_CONFIG = {
    'max_retries': 3,
    'backoff_base': 30,
}


# ============================================================================
# ENDPOINT DEFINITIONS
# ============================================================================

ENDPOINTS: Dict[str, Dict[str, Any]] = {

    # --- Basic stats (since 2003-04) ---

    'leaguedashplayerstats': {
        'min_season': '2003-04',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPlayerStats',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['player'],
    },
    'leaguedashteamstats': {
        'min_season': '2003-04',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashTeamStats',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['team'],
    },

    # --- Player tracking (since 2013-14) ---

    'leaguedashptstats': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPtStats',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'requires_params': ['pt_measure_type'],
        'entity_types': ['player', 'team'],
    },

    # --- Hustle stats (since 2015-16) ---

    'leaguehustlestatsplayer': {
        'min_season': '2015-16',
        'execution_tier': 'league',
        'default_result_set': 'HustleStatsPlayer',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_time',
        'entity_types': ['player'],
    },
    'leaguehustlestatsteam': {
        'min_season': '2015-16',
        'execution_tier': 'league',
        'default_result_set': 'HustleStatsTeam',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_time',
        'entity_types': ['team'],
    },

    # --- Defensive matchup (since 2013-14) ---

    'leaguedashptdefend': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPtDefend',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'requires_params': ['defense_category'],
        'entity_types': ['player'],
    },
    'leaguedashptteamdefend': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPtTeamDefend',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'requires_params': ['defense_category'],
        'entity_types': ['team'],
    },

    # --- Shot tracking league-wide (since 2013-14) ---

    'leaguedashplayerptshot': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPTShots',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['player'],
    },
    'leaguedashteamptshot': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPTShots',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['team'],
    },

    # --- Rebound tracking (since 2013-14) ---

    'playerdashptreb': {
        'min_season': '2013-14',
        'execution_tier': 'player',
        'default_result_set': 'OverallRebounding',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['player'],
    },
    'teamdashptreb': {
        'min_season': '2013-14',
        'execution_tier': 'team',
        'default_result_set': 'OverallRebounding',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['team'],
    },

    # --- Shooting splits (since 2012-13) ---

    'playerdashboardbyshootingsplits': {
        'min_season': '2012-13',
        'execution_tier': 'player',
        'default_result_set': 'ShotTypePlayerDashboard',
        'season_type_param': 'season_type_playoffs',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['player'],
    },
    'teamdashboardbyshootingsplits': {
        'min_season': '2012-13',
        'execution_tier': 'league',
        'default_result_set': 'ShotTypeTeamDashboard',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['team'],
    },

    # --- Player info (all time) ---

    'commonallplayers': {
        'min_season': None,
        'execution_tier': 'league',
        'default_result_set': 'CommonAllPlayers',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['player'],
    },

    'commonplayerinfo': {
        'min_season': None,
        'execution_tier': 'player',
        'default_result_set': 'CommonPlayerInfo',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['player'],
    },

    # --- Draft combine (since 2000-01) ---

    'draftcombineplayeranthro': {
        'min_season': '2000-01',
        'execution_tier': 'league',
        'default_result_set': 'DraftCombinePlayerAnthro',
        'season_param': 'season_year',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['player'],
    },

    # --- On/Off court (since 2007-08) ---

    'teamplayeronoffsummary': {
        'min_season': '2007-08',
        'execution_tier': 'league',
        'default_result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['player'],
    },

    # --- Virtual: team metadata (abbreviation + conference) ---
    # Combines nba_api static teams data with LeagueStandings.
    # No real NBA API class — handled by fetch_team_metadata() in client.py.

    'team_metadata': {
        'min_season': None,
        'execution_tier': 'league',
        'default_result_set': 'TeamMetadata',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['team'],
        'virtual': True,
    },
}


# ============================================================================
# API FIELD NAME MAPPINGS
# ============================================================================

API_FIELD_NAMES = {
    'entity_id':   {'player': 'PLAYER_ID', 'team': 'TEAM_ID'},
    'entity_name': {'player': 'PLAYER_NAME', 'team': 'TEAM_NAME'},
    'special_ids': {'person': 'PERSON_ID'},
    'id_aliases':  {'PLAYER_ID': ['PERSON_ID']},
}


# ============================================================================
# SCHEMA VALIDATORS
# ============================================================================

def validate_provider_config() -> list:
    import logging

    from src.core.config_validation import validate_dict_config, validate_flat_config

    logger = logging.getLogger(__name__)
    errors: list = []

    errors.extend(validate_flat_config(SOURCE_META, SOURCE_META_SCHEMA, 'SOURCE_META'))
    errors.extend(validate_flat_config(API_CONFIG, API_CONFIG_SCHEMA, 'API_CONFIG'))
    errors.extend(validate_flat_config(RETRY_CONFIG, RETRY_CONFIG_SCHEMA, 'RETRY_CONFIG'))
    errors.extend(validate_dict_config(SEASON_TYPES, SEASON_TYPES_SCHEMA, 'SEASON_TYPES'))
    errors.extend(validate_dict_config(ENDPOINTS, ENDPOINTS_SCHEMA, 'ENDPOINTS'))

    if errors:
        for err in errors:
            logger.error('nba_api config validation: %s', err)

    return errors


# ============================================================================
# VALIDATION SCHEMAS  (co-located with the config they describe)
# ============================================================================

VALID_EXECUTION_TIERS = {'league', 'player', 'team', 'team_call'}

SOURCE_META_SCHEMA = {
    'source_key':      {'required': True, 'types': (str,)},
    'roster_endpoint': {'required': True, 'types': (str,)},
}

API_CONFIG_SCHEMA = {
    'rate_limit_delay': {'required': True, 'types': (int, float)},
    'per_player_rate_limit': {'required': True, 'types': (int, float)},
    'timeout_default': {'required': True, 'types': (int, float)},
    'timeout_bulk': {'required': True, 'types': (int, float)},
    'backoff_divisor': {'required': True, 'types': (int, float)},
    'cooldown_after_batch_seconds': {'required': True, 'types': (int, float)},
    'max_consecutive_failures': {'required': True, 'types': (int,)},
    'roster_batch_size': {'required': True, 'types': (int,)},
    'roster_batch_cooldown': {'required': True, 'types': (int, float)},
    'league_id': {'required': True, 'types': (str,)},
    'per_mode_simple': {'required': True, 'types': (str,)},
    'per_mode_time': {'required': True, 'types': (str,)},
    'per_mode_detailed': {'required': True, 'types': (str,)},
    'last_n_games': {'required': True, 'types': (str,)},
    'month': {'required': True, 'types': (str,)},
    'opponent_team_id': {'required': True, 'types': (str,)},
    'period': {'required': True, 'types': (str,)},
}

RETRY_CONFIG_SCHEMA = {
    'max_retries': {'required': True, 'types': (int,)},
    'backoff_base': {'required': True, 'types': (int, float)},
}

ENDPOINTS_SCHEMA = {
    'min_season': {'required': True, 'types': (str, type(None))},
    'execution_tier': {'required': True, 'types': (str,), 'allowed_values': VALID_EXECUTION_TIERS},
    'default_result_set': {'required': True, 'types': (str,)},
    'season_type_param': {'required': True, 'types': (str, type(None))},
    'per_mode_param': {'required': True, 'types': (str, type(None))},
    'entity_types': {'required': True, 'types': (list,), 'list_item_values': {'player', 'team'}},
    'requires_params': {'required': False, 'types': (list,)},
    'virtual': {'required': False, 'types': (bool,)},
}

SEASON_TYPES_SCHEMA = {
    'name': {'required': True, 'types': (str,)},
    'param': {'required': True, 'types': (str,)},
    'min_season': {'required': True, 'types': (str, type(None))},
}
