"""
The Glass - ETL Definitions: Central Configuration

Single source of truth for the data model:
    LEAGUES         - per-league operational settings (calendar, retention, season grammar)
    SOURCES         - data-source registry (readers and writers)
    PROFILE_TABLES  - entity tables (live in the shared 'core' schema)
    STATS_TABLES    - per-league stats tables (live in '<league>' schemas)
    JUNCTION_TABLES - membership tables (live in 'core': league/team and team/player)
    ETL_CONFIG      - non-league-specific runtime settings
    ETL_TABLES      - operational tables for run / progress tracking

Each top-level config dict has a co-located *_SCHEMA used by the validation
engine in src.core.config_validation.

The DDL generator in src.etl.core.ddl synthesizes columns by combining:
    - DB_COLUMNS entries matching (entity, scope) for profile/stats tables
    - dynamic source-id columns from SOURCES for profile tables
    - inline column dicts for junction tables
"""

from typing import Any, Dict


# ============================================================================
# FUNDAMENTAL SCHEMA CONSTANTS
# ============================================================================

CORE_SCHEMA = 'core'
THE_GLASS_ID_COLUMN = 'the_glass_id'
THE_GLASS_ID_TYPE = 'BIGINT'
THE_GLASS_ID_SEQUENCE = 'core.the_glass_id_seq'


# ============================================================================
# ALLOWED VALUE SETS
# ============================================================================

VALID_PG_TYPES = {
    'SERIAL', 'SMALLINT', 'INTEGER', 'BIGINT', 'VARCHAR', 'TEXT', 'CHAR',
    'BOOLEAN', 'TIMESTAMP', 'DATE', 'NUMERIC', 'REAL', 'DOUBLE PRECISION',
}
VALID_ENTITY_TYPES = {'league', 'player', 'team', 'opponent'}
VALID_SCOPES = {'entity', 'stats', 'both'}
VALID_UPDATE_FREQUENCIES = {'daily', 'annual', None}
VALID_REFRESH_MODES = {'null_only', 'always'}
VALID_SOURCE_ROLES = {'reader', 'writer'}
VALID_SCHEMA_KINDS = {'core', 'league'}
VALID_FK_ACTIONS = {'CASCADE', 'RESTRICT', 'SET NULL', 'NO ACTION'}


# ============================================================================
# DB_COLUMNS SCHEMA
# ============================================================================

DB_COLUMNS_SCHEMA: Dict[str, Dict[str, Any]] = {
    'type':                 {'required': True,  'types': (str,)},
    'scope':                {'required': True,  'types': (str,), 'allowed_values': VALID_SCOPES},
    'nullable':             {'required': True,  'types': (bool,)},
    'default':              {'required': True,  'types': (str, int, type(None))},
    'entity_types':         {'required': True,  'types': (list,), 'list_item_values': VALID_ENTITY_TYPES},
    'update_frequency':     {'required': True,  'types': (str, type(None)), 'allowed_values': VALID_UPDATE_FREQUENCIES},
    'domain':               {'required': True,  'types': (str, type(None))},
    'comment':              {'required': True,  'types': (str, type(None))},
    'sources':              {'required': True,  'types': (dict, type(None))},
    'removed_refresh_mode': {'required': False, 'types': (str,),  'allowed_values': VALID_REFRESH_MODES},
}


# ============================================================================
# LEAGUES
# ============================================================================

LEAGUES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'name':                {'required': True, 'types': (str,)},
    'abbr':                {'required': True, 'types': (str,)},
    'season_format':       {'required': True, 'types': (str,)},
    'season_types':        {'required': True, 'types': (list,)},
    'calendar_flip_md':    {'required': True, 'types': (str,)},
    'retention_seasons':   {'required': True, 'types': (int,)},
    'reader_source':       {'required': True, 'types': (str,)},
}

LEAGUES: Dict[str, Dict[str, Any]] = {
    'nba': {
        'name': 'NBA',
        'abbr': 'NBA',
        'season_format': 'YYYY-YY',
        'season_types': ['rs', 'po', 'pi'],
        'calendar_flip_md': '08-01',
        'retention_seasons': 8,
        'reader_source': 'nba_api',
    },
}


# ============================================================================
# SOURCES
# ============================================================================

SOURCES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'leagues':         {'required': True, 'types': (list,)},
    'role':            {'required': True, 'types': (str,), 'allowed_values': VALID_SOURCE_ROLES},
    'entity_id_type':  {'required': True, 'types': (str, type(None)), 'allowed_values': set(VALID_PG_TYPES) | {None}},
    'applies_to':      {'required': True, 'types': (list,)},
}

SOURCES: Dict[str, Dict[str, Any]] = {
    'nba_api': {
        'leagues': ['nba'],
        'role': 'reader',
        'entity_id_type': 'BIGINT',
        'applies_to': ['team', 'player'],
    },
    'the_glass_sheets': {
        'leagues': ['nba'],
        'role': 'writer',
        'entity_id_type': None,
        'applies_to': [],
    },
}


# ============================================================================
# PROFILE TABLES (core schema)
# ============================================================================

PROFILE_TABLES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'entity':       {'required': True, 'types': (str,), 'allowed_values': {'league', 'team', 'player'}},
    'schema_kind':  {'required': True, 'types': (str,), 'allowed_values': {'core'}},
}

PROFILE_TABLES: Dict[str, Dict[str, Any]] = {
    'league_profiles': {
        'entity': 'league',
        'schema_kind': 'core',
    },
    'team_profiles': {
        'entity': 'team',
        'schema_kind': 'core',
    },
    'player_profiles': {
        'entity': 'player',
        'schema_kind': 'core',
    },
}


# ============================================================================
# STATS TABLES (per-league schema)
# ============================================================================

STATS_TABLES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'entity':                {'required': True, 'types': (str,), 'allowed_values': {'team', 'player'}},
    'schema_kind':           {'required': True, 'types': (str,), 'allowed_values': {'league'}},
    'primary_key':           {'required': True, 'types': (list,)},
    'has_opponent_columns':  {'required': True, 'types': (bool,)},
    'foreign_keys':          {'required': True, 'types': (list,)},
}

STATS_TABLES: Dict[str, Dict[str, Any]] = {
    'player_season_stats': {
        'entity': 'player',
        'schema_kind': 'league',
        'primary_key': ['the_glass_id', 'team_id', 'season', 'season_type'],
        'has_opponent_columns': False,
        'foreign_keys': [
            {
                'column': 'the_glass_id',
                'ref_schema': 'core',
                'ref_table': 'player_profiles',
                'ref_column': 'the_glass_id',
                'on_update': 'CASCADE',
                'on_delete': 'CASCADE',
            },
            {
                'column': 'team_id',
                'ref_schema': 'core',
                'ref_table': 'team_profiles',
                'ref_column': 'the_glass_id',
                'on_update': 'CASCADE',
                'on_delete': 'CASCADE',
            },
        ],
    },
    'team_season_stats': {
        'entity': 'team',
        'schema_kind': 'league',
        'primary_key': ['the_glass_id', 'season', 'season_type'],
        'has_opponent_columns': True,
        'foreign_keys': [
            {
                'column': 'the_glass_id',
                'ref_schema': 'core',
                'ref_table': 'team_profiles',
                'ref_column': 'the_glass_id',
                'on_update': 'CASCADE',
                'on_delete': 'CASCADE',
            },
        ],
    },
}


# ============================================================================
# JUNCTION TABLES (core schema)
# ============================================================================

JUNCTION_TABLES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'schema_kind':  {'required': True, 'types': (str,), 'allowed_values': {'core'}},
    'primary_key':  {'required': True, 'types': (list,)},
    'columns':      {'required': True, 'types': (dict,)},
    'foreign_keys': {'required': True, 'types': (list,)},
}

# Standard timestamp columns shared by every junction.
_JUNCTION_TIMESTAMP_COLUMNS: Dict[str, Dict[str, Any]] = {
    'is_active':  {'type': 'BOOLEAN',   'nullable': False, 'default': 'TRUE',              'primary_key': False},
    'created_at': {'type': 'TIMESTAMP', 'nullable': False, 'default': 'CURRENT_TIMESTAMP', 'primary_key': False},
    'updated_at': {'type': 'TIMESTAMP', 'nullable': False, 'default': 'CURRENT_TIMESTAMP', 'primary_key': False},
}

JUNCTION_TABLES: Dict[str, Dict[str, Any]] = {
    'league_rosters': {
        'schema_kind': 'core',
        'primary_key': ['league_id', 'team_id'],
        'columns': {
            'league_id': {'type': 'BIGINT', 'nullable': False, 'default': None, 'primary_key': True},
            'team_id':   {'type': 'BIGINT', 'nullable': False, 'default': None, 'primary_key': True},
            **_JUNCTION_TIMESTAMP_COLUMNS,
        },
        'foreign_keys': [
            {
                'column': 'league_id',
                'ref_schema': 'core',
                'ref_table': 'league_profiles',
                'ref_column': 'the_glass_id',
                'on_update': 'CASCADE',
                'on_delete': 'RESTRICT',
            },
            {
                'column': 'team_id',
                'ref_schema': 'core',
                'ref_table': 'team_profiles',
                'ref_column': 'the_glass_id',
                'on_update': 'CASCADE',
                'on_delete': 'CASCADE',
            },
        ],
    },
    'team_rosters': {
        'schema_kind': 'core',
        'primary_key': ['team_id', 'player_id'],
        'columns': {
            'team_id':   {'type': 'BIGINT', 'nullable': False, 'default': None, 'primary_key': True},
            'player_id': {'type': 'BIGINT', 'nullable': False, 'default': None, 'primary_key': True},
            **_JUNCTION_TIMESTAMP_COLUMNS,
        },
        'foreign_keys': [
            {
                'column': 'team_id',
                'ref_schema': 'core',
                'ref_table': 'team_profiles',
                'ref_column': 'the_glass_id',
                'on_update': 'CASCADE',
                'on_delete': 'CASCADE',
            },
            {
                'column': 'player_id',
                'ref_schema': 'core',
                'ref_table': 'player_profiles',
                'ref_column': 'the_glass_id',
                'on_update': 'CASCADE',
                'on_delete': 'CASCADE',
            },
        ],
    },
}


# ============================================================================
# ETL_CONFIG  (runtime settings, league-agnostic)
# ============================================================================

ETL_CONFIG_SCHEMA: Dict[str, Dict[str, Any]] = {
    'max_retry_attempts':    {'required': True, 'types': (int,)},
    'retry_delay_seconds':   {'required': True, 'types': (int,)},
    'auto_resume':           {'required': True, 'types': (bool,)},
}

ETL_CONFIG: Dict[str, Any] = {
    'max_retry_attempts': 3,
    'retry_delay_seconds': 60,
    'auto_resume': True,
}


# ============================================================================
# ETL OPERATIONAL TABLES  (inline columns; live in each league's schema)
# ============================================================================

ETL_TABLES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'columns':    {'required': True, 'types': (dict,)},
    'unique_key': {'required': False, 'types': (list,)},
}

ETL_TABLES: Dict[str, Dict[str, Any]] = {
    'etl_runs': {
        'columns': {
            'id':              {'type': 'SERIAL',      'primary_key': True,  'nullable': False},
            'run_type':        {'type': 'VARCHAR(20)', 'nullable': False},
            'status':          {'type': 'VARCHAR(20)', 'nullable': False, 'default': "'running'"},
            'started_at':      {'type': 'TIMESTAMP',   'nullable': False, 'default': 'NOW()'},
            'completed_at':    {'type': 'TIMESTAMP',   'nullable': True},
            'season':          {'type': 'VARCHAR(7)',  'nullable': True},
            'season_type':     {'type': 'VARCHAR(3)',  'nullable': True},
            'entity_type':     {'type': 'VARCHAR(10)', 'nullable': True},
            'total_groups':    {'type': 'INTEGER',     'nullable': True, 'default': '0'},
            'completed_groups':{'type': 'INTEGER',     'nullable': True, 'default': '0'},
            'total_rows':      {'type': 'INTEGER',     'nullable': True, 'default': '0'},
            'error_message':   {'type': 'TEXT',        'nullable': True},
        },
    },
    'etl_progress': {
        'columns': {
            'id':              {'type': 'SERIAL',      'primary_key': True,  'nullable': False},
            'run_id':          {'type': 'INTEGER',     'nullable': False},
            'entity_type':     {'type': 'VARCHAR(10)', 'nullable': False},
            'endpoint':        {'type': 'VARCHAR(100)','nullable': False},
            'tier':            {'type': 'VARCHAR(20)', 'nullable': False},
            'column_name':     {'type': 'TEXT',        'nullable': True},
            'status':          {'type': 'VARCHAR(20)', 'nullable': False, 'default': "'pending'"},
            'started_at':      {'type': 'TIMESTAMP',   'nullable': True},
            'completed_at':    {'type': 'TIMESTAMP',   'nullable': True},
            'rows_written':    {'type': 'INTEGER',     'nullable': True, 'default': '0'},
            'error_message':   {'type': 'TEXT',        'nullable': True},
            'retry_count':     {'type': 'INTEGER',     'nullable': True, 'default': '0'},
        },
        'unique_key': ['run_id', 'entity_type', 'endpoint', 'column_name'],
    },
}


# ============================================================================
# DEFAULT TRANSFORMS BY COLUMN TYPE
# ============================================================================

TYPE_TRANSFORMS: Dict[str, str] = {
    'SMALLINT': 'safe_int',
    'INTEGER':  'safe_int',
    'BIGINT':   'safe_int',
    'VARCHAR':  'safe_str',
    'TEXT':     'safe_str',
    'CHAR':     'safe_str',
}
