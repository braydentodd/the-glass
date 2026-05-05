"""
The Glass - Database Table Registry

Unified registry of every table in the database: profiles, stats, junctions,
and operational (run-tracking) tables.  Each entry carries ``kind`` and
``used_by`` metadata so consumers know which pipeline domains touch it.

Operational tables use a ``pipeline`` discriminator column so both ETL and
publish runs share single ``runs`` and ``tasks`` tables rather than
maintaining four mirrored tables.

Column definitions live in ``src.core.definitions.columns.DB_COLUMNS``.
Operational and junction tables reference columns by name; the DDL generator
looks up type, nullable, default, and other metadata from the column registry.

Backward-compatible filtered views (PROFILE_TABLES, STATS_TABLES, etc.)
are built automatically from the master ``TABLES`` dict.
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
VALID_SCHEMA_KINDS = {'core', 'league'}
VALID_FK_ACTIONS = {'CASCADE', 'RESTRICT', 'SET NULL', 'NO ACTION'}


# ============================================================================
# DB_COLUMNS SCHEMA  (validation contract for src.core.definitions.columns.DB_COLUMNS)
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
    'removed_refresh_mode': {'required': False, 'types': (str,), 'allowed_values': VALID_REFRESH_MODES}
}


# ============================================================================
# KIND-SPECIFIC VALIDATION SCHEMAS
# ============================================================================

PROFILE_TABLES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'kind':      {'required': True, 'types': (str,), 'allowed_values': {'profile'}},
    'used_by':   {'required': True, 'types': (list,)},
    'entity':    {'required': True, 'types': (str,), 'allowed_values': {'league', 'team', 'player'}},
    'schema_kind': {'required': True, 'types': (str,), 'allowed_values': {'core'}},
}

STATS_TABLES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'kind':               {'required': True, 'types': (str,), 'allowed_values': {'stats'}},
    'used_by':            {'required': True, 'types': (list,)},
    'entity':             {'required': True, 'types': (str,), 'allowed_values': {'team', 'player'}},
    'schema_kind':        {'required': True, 'types': (str,), 'allowed_values': {'league'}},
    'primary_key':        {'required': True, 'types': (list,)},
    'has_opponent_columns': {'required': True, 'types': (bool,)},
    'foreign_keys':       {'required': True, 'types': (list,)},
}

JUNCTION_TABLES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'kind':       {'required': True, 'types': (str,), 'allowed_values': {'junction'}},
    'used_by':    {'required': True, 'types': (list,)},
    'schema_kind': {'required': True, 'types': (str,), 'allowed_values': {'core'}},
    'primary_key': {'required': True, 'types': (list,)},
    'columns':    {'required': True, 'types': (list,)},
    'foreign_keys': {'required': True, 'types': (list,)},
}

OPERATIONAL_TABLES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'kind':       {'required': True, 'types': (str,), 'allowed_values': {'operational'}},
    'used_by':    {'required': True, 'types': (list,)},
    'schema_kind': {'required': True, 'types': (str,), 'allowed_values': {'league'}},
    'columns':    {'required': True, 'types': (list,)},
    'unique_key': {'required': False, 'types': (list,)},
}

# ============================================================================
# UNIFIED TABLE REGISTRY
# ============================================================================

TABLES: Dict[str, Dict[str, Any]] = {
    # ------------------------------------------------------------------
    # PROFILE TABLES (core schema)
    # ------------------------------------------------------------------
    'league_profiles': {
        'kind': 'profile',
        'used_by': ['etl', 'publish'],
        'entity': 'league',
        'schema_kind': 'core',
    },
    'team_profiles': {
        'kind': 'profile',
        'used_by': ['etl', 'publish'],
        'entity': 'team',
        'schema_kind': 'core',
    },
    'player_profiles': {
        'kind': 'profile',
        'used_by': ['etl', 'publish'],
        'entity': 'player',
        'schema_kind': 'core',
    },
    # ------------------------------------------------------------------
    # STATS TABLES (per-league schema)
    # ------------------------------------------------------------------
    'player_season_stats': {
        'kind': 'stats',
        'used_by': ['etl', 'publish'],
        'entity': 'player',
        'schema_kind': 'league',
        'primary_key': ['the_glass_id', 'team_id', 'season', 'season_type'],
        'has_opponent_columns': False,
        'foreign_keys': [
            {
                'column':     'the_glass_id',
                'ref_schema': 'core',
                'ref_table':  'player_profiles',
                'ref_column': 'the_glass_id',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'team_id',
                'ref_schema': 'core',
                'ref_table':  'team_profiles',
                'ref_column': 'the_glass_id',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
    },
    'team_season_stats': {
        'kind': 'stats',
        'used_by': ['etl', 'publish'],
        'entity': 'team',
        'schema_kind': 'league',
        'primary_key': ['the_glass_id', 'season', 'season_type'],
        'has_opponent_columns': True,
        'foreign_keys': [
            {
                'column':     'the_glass_id',
                'ref_schema': 'core',
                'ref_table':  'team_profiles',
                'ref_column': 'the_glass_id',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
    },
    # ------------------------------------------------------------------
    # JUNCTION TABLES (core schema)
    # ------------------------------------------------------------------
    'league_rosters': {
        'kind': 'junction',
        'used_by': ['etl', 'publish'],
        'schema_kind': 'core',
        'primary_key': ['league_id', 'team_id'],
        'columns': ['league_id', 'team_id', 'is_active', 'created_at', 'updated_at'],
        'foreign_keys': [
            {
                'column':     'league_id',
                'ref_schema': 'core',
                'ref_table':  'league_profiles',
                'ref_column': 'the_glass_id',
                'on_update':  'CASCADE',
                'on_delete':  'RESTRICT',
            },
            {
                'column':     'team_id',
                'ref_schema': 'core',
                'ref_table':  'team_profiles',
                'ref_column': 'the_glass_id',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
    },
    'team_rosters': {
        'kind': 'junction',
        'used_by': ['etl', 'publish'],
        'schema_kind': 'core',
        'primary_key': ['team_id', 'player_id'],
        'columns': ['team_id', 'player_id', 'is_active', 'created_at', 'updated_at'],
        'foreign_keys': [
            {
                'column':     'team_id',
                'ref_schema': 'core',
                'ref_table':  'team_profiles',
                'ref_column': 'the_glass_id',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'player_id',
                'ref_schema': 'core',
                'ref_table':  'player_profiles',
                'ref_column': 'the_glass_id',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
    },
    # ------------------------------------------------------------------
    # OPERATIONAL TABLES (per-league schema)
    #
    # Both ETL and publish pipelines share these two tables.  The
    # ``pipeline`` column ('etl' | 'publish') acts as the discriminator.
    #
    # runs  -- one row per pipeline execution (top-level audit record)
    # tasks -- one row per resumable work unit within a run:
    #            ETL:     dataset + tier + column_name group
    #            publish: tab_name
    #
    # Columns are defined in DB_COLUMNS; table metadata specifies primary_key
    # and unique constraints.  The DDL generator looks up column types from
    # the column registry.
    # ------------------------------------------------------------------
    'runs': {
        'kind': 'operational',
        'used_by': ['etl', 'publish'],
        'schema_kind': 'league',
        'columns': ['pipeline', 'run_type', 'status', 'started_at', 'completed_at', 'season', 'entity_type', 'total_items', 'completed_items', 'total_rows', 'error_message'],
    },
    'tasks': {
        'kind': 'operational',
        'used_by': ['etl', 'publish'],
        'schema_kind': 'league',
        'columns': ['run_id', 'pipeline', 'item_key', 'entity_type', 'status', 'started_at', 'completed_at', 'rows_written', 'error_message', 'retry_count'],
        'unique_key': ['run_id', 'pipeline', 'item_key'],
    },
}


# ============================================================================
# FILTERED VIEWS
# ============================================================================

PROFILE_TABLES = {k: v for k, v in TABLES.items() if v['kind'] == 'profile'}
STATS_TABLES = {k: v for k, v in TABLES.items() if v['kind'] == 'stats'}
JUNCTION_TABLES = {k: v for k, v in TABLES.items() if v['kind'] == 'junction'}
OPERATIONAL_TABLES = {k: v for k, v in TABLES.items() if v['kind'] == 'operational'}
