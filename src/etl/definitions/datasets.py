"""
The Glass - Unified Dataset Registry

Single source of truth for all dataset definitions across every identity.

Each identity (e.g. ``nba_id``, ``internal``) has its own namespace so
dataset names only need to be unique within an identity.  Every entry
carries the same generic orchestrator-level fields plus a ``source_mapping``
dict that holds source-specific wire parameters.

Shape:

    DATASETS[identity_key][dataset_name] -> DatasetDef
    DatasetDef['source'] -> source_module_key (e.g. 'nba_api', 'the_glass_sheets')
    DatasetDef['source_mapping'] -> SourceMappingDef

This mirrors the ``dataset_mapping`` pattern in ``db_columns.py``.
"""

from typing import Dict, List, TypedDict, Union


class SourceMappingDef(TypedDict, total=False):
    """Source-specific wire parameters.  Optional fields vary by source type."""
    class_name: str
    result_set: Union[str, None]
    season_param_format: Union[str, None]
    season_type_param: Union[str, None]
    per_mode_param: Union[str, None]
    requires_params: Union[List[str], None]
    season_param: Union[str, None]
    endpoint: Union[str, None]
    url_suffix: Union[str, None]


class DatasetDef(TypedDict):
    """Generic dataset metadata, uniform across every identity."""
    min_season: Union[str, None]
    execution_tier: str
    source: str
    source_mapping: SourceMappingDef


DATASETS: Dict[str, Dict[str, DatasetDef]] = {

    # ========================================================================
    # NBA API
    # ========================================================================

    'nba_id': {

        # --- Basic stats (since 2003-04) ---

        'player_stats': {
            'min_season': '2003-04',
            'execution_tier': 'per_team',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'leaguedashplayerstats',
                'result_set': 'LeagueDashPlayerStats',
                'season_param_format': 'SSSS-EE',
                'season_type_param': 'season_type_all_star',
                'per_mode_param': 'per_mode_detailed',
            },
        },
        'team_stats': {
            'min_season': '2003-04',
            'execution_tier': 'per_league',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'leaguedashteamstats',
                'result_set': 'LeagueDashTeamStats',
                'season_param_format': 'SSSS-EE',
                'season_type_param': 'season_type_all_star',
                'per_mode_param': 'per_mode_detailed',
            },
        },

        # --- Player tracking (since 2013-14) ---

        'player_tracking': {
            'min_season': '2013-14',
            'execution_tier': 'per_team',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'leaguedashptstats',
                'result_set': 'LeagueDashPtStats',
                'season_param_format': 'SSSS-EE',
                'season_type_param': 'season_type_all_star',
                'per_mode_param': 'per_mode_simple',
                'requires_params': ['pt_measure_type'],
            },
        },
        'team_tracking': {
            'min_season': '2013-14',
            'execution_tier': 'per_league',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'leaguedashptstats',
                'result_set': 'LeagueDashPtStats',
                'season_param_format': 'SSSS-EE',
                'season_type_param': 'season_type_all_star',
                'per_mode_param': 'per_mode_simple',
                'requires_params': ['pt_measure_type'],
            },
        },

        # --- Hustle stats (since 2015-16) ---

        'player_hustle': {
            'min_season': '2015-16',
            'execution_tier': 'per_team',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'leaguehustlestatsplayer',
                'result_set': 'HustleStatsPlayer',
                'season_param_format': 'SSSS-EE',
                'season_type_param': 'season_type_all_star',
                'per_mode_param': 'per_mode_time',
            },
        },
        'team_hustle': {
            'min_season': '2015-16',
            'execution_tier': 'per_league',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'leaguehustlestatsteam',
                'result_set': 'HustleStatsTeam',
                'season_param_format': 'SSSS-EE',
                'season_type_param': 'season_type_all_star',
                'per_mode_param': 'per_mode_time',
            },
        },

        # --- Defensive matchup (since 2013-14) ---

        'player_defense': {
            'min_season': '2013-14',
            'execution_tier': 'per_team',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'leaguedashptdefend',
                'result_set': 'LeagueDashPtDefend',
                'season_param_format': 'SSSS-EE',
                'season_type_param':  'season_type_all_star',
                'per_mode_param': 'per_mode_simple',
                'requires_params': ['defense_category'],
            },
        },
        'team_defense': {
            'min_season': '2013-14',
            'execution_tier': 'per_league',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'leaguedashptteamdefend',
                'result_set': 'LeagueDashPtTeamDefend',
                'season_param_format': 'SSSS-EE',
                'season_type_param': 'season_type_all_star',
                'per_mode_param': 'per_mode_simple',
                'requires_params': ['defense_category'],
            },
        },

        # --- Player info (all time) ---

        'player_info': {
            'min_season': None,
            'execution_tier': 'per_league',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'commonallplayers',
                'result_set': 'CommonAllPlayers',
                'season_param_format': 'SSSS-EE',
            },
        },

        # --- Draft combine (since 2000-01) ---

        'combine_anthro': {
            'min_season': '2000-01',
            'execution_tier': 'per_league',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'draftcombineplayeranthro',
                'result_set': 'DraftCombinePlayerAnthro',
                'season_param_format': 'SSSS-EE',
                'season_param': 'season_year',
            },
        },

        # --- On/Off court (since 2007-08) ---

        'player_on_court': {
            'min_season': '2007-08',
            'execution_tier': 'per_team',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'teamplayeronoffdetails',
                'result_set': 'PlayersOnCourtTeamPlayerOnOffDetails',
                'season_param_format': 'SSSS-EE',
                'season_type_param': 'season_type_all_star',
                'per_mode_param': 'per_mode_detailed',
            },
        },
        'player_off_court': {
            'min_season': '2007-08',
            'execution_tier': 'per_team',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'teamplayeronoffdetails',
                'result_set': 'PlayersOffCourtTeamPlayerOnOffDetails',
                'season_param_format': 'SSSS-EE',
                'season_type_param': 'season_type_all_star',
                'per_mode_param': 'per_mode_detailed',
            },
        },

        # --- Team info (all time) ---

        'team_info': {
            'min_season': None,
            'execution_tier': 'per_team',
            'source': 'nba_api',
            'source_mapping': {
                'class_name': 'teaminfocommon',
                'result_set': 'TeamInfoCommon',
                'season_param_format': 'SSSS-EE',
                'season_type_param': 'season_type_all_star',
            },
        },

        'team_totals': {
            'min_season': '2000-01',
            'execution_tier': 'per_league',
            'source': 'pbp_stats',
            'source_mapping': {
                'result_set': 'PbpTotals',
                'season_param_format': 'SSSS-EE',
                'endpoint': 'get-totals',
                'url_suffix': None,
            },
        },
        'player_totals': {
            'min_season': '2000-01',
            'execution_tier': 'per_team',
            'source': 'pbp_stats',
            'source_mapping': {
                'result_set': 'PbpTotals',
                'season_param_format': 'SSSS-EE',
                'endpoint': 'get-totals',
                'url_suffix': None,
            },
        }
    },

    # ========================================================================
    # The Glass Sheets
    # ========================================================================

    'internal': {
        'players': {
            'min_season': None,
            'execution_tier': 'per_league',
            'source': 'the_glass_sheets',
            'source_mapping': {},
        },
        'teams': {
            'min_season': None,
            'execution_tier': 'per_league',
            'source': 'the_glass_sheets',
            'source_mapping': {},
        }
    }
}


