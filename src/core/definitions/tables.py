"""
The Glass - Database Table Registry

Unified registry of every table in the database: profiles, stats, rosters,
and operational (run-tracking) tables.  Each entry carries ``kind`` and
``used_by`` metadata so consumers know which pipeline domains touch it.

Operational tables use a ``pipeline`` discriminator column so both ETL and
publish runs share single ``runs`` and ``tasks`` tables rather than
maintaining four mirrored tables.

Column definitions live in ``src.core.definitions.columns.DB_COLUMNS``.
Operational and roster tables reference columns by name; the DDL generator
looks up type, nullable, default, and other metadata from the column registry.

Backward-compatible filtered views (PROFILE_TABLES, STATS_TABLES, etc.)
are built automatically from the master ``TABLES`` dict.
"""

from typing import TypedDict, Dict, List, Any, Union


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
VALID_ENTITY_TYPES = {'league', 'player', 'team'}
VALID_SCOPES = {'profile', 'stats', 'roster', 'runs', 'tasks', 'backfill'}
VALID_REFRESH_MODES = {'null_only', 'always'}
VALID_SCHEMA_KINDS = {'core', 'league'}
VALID_FK_ACTIONS = {'CASCADE', 'RESTRICT', 'SET NULL', 'NO ACTION'}
VALID_MANAGED_BY = frozenset({'db', 'execution_context', 'source'})


# ============================================================================
# DB_COLUMNS SCHEMA  (validation contract for src.core.definitions.columns.DB_COLUMNS)
# ============================================================================

class TableDef(TypedDict):
    used_by: List[str]
    entity: Union[str, None]
    schema: Union[str, None]
    primary_key: Union[List[str], None]
    foreign_keys: Union[List[Dict[str, Any]], None]
    scope: Union[str, None]
    unique_constraints: Union[List[List[str]], None]

TABLES: Dict[str, TableDef] = {

    # ------------------------------------------------------------------
    # PROFILE TABLES (core schema)
    # ------------------------------------------------------------------
    'league_profiles': {
        'used_by': ['etl', 'publish'],
        'entity': 'league',
        'schema': 'core',
        'primary_key': ['the_glass_id'],
        'foreign_keys': [],
        'indexes': [],
        'scope': 'profile',
    },
    'team_profiles': {
        'used_by': ['etl', 'publish'],
        'entity': 'team',
        'schema': 'core',
        'primary_key': ['the_glass_id'],
        'foreign_keys': [],
        'indexes': [],
        'scope': 'profile',
    },
    'player_profiles': {
        'used_by': ['etl', 'publish'],
        'entity': 'player',
        'schema': 'core',
        'primary_key': ['the_glass_id'],
        'foreign_keys': [],
        'indexes': [],
        'scope': 'profile',
    },
    # ------------------------------------------------------------------
    # STATS TABLES (per-league schema)
    # ------------------------------------------------------------------
    'player_season_stats': {
        'used_by': ['etl', 'publish'],
        'entity': 'player',
        'schema': 'league',
        'primary_key': ['player_id', 'team_id', 'season', 'season_type'],
        'foreign_keys': [
            {
                'column':     'player_id',
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
        'indexes': [],
        'scope': 'stats',
    },
    'team_season_stats': {
        'used_by': ['etl', 'publish'],
        'entity': 'team',
        'schema': 'league',
        'primary_key': ['team_id', 'season', 'season_type'],
        'foreign_keys': [
            {
                'column':     'team_id',
                'ref_schema': 'core',
                'ref_table':  'team_profiles',
                'ref_column': 'the_glass_id',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'indexes': [],
        'scope': 'stats',
    },
    # ------------------------------------------------------------------
    # ROSTER TABLES (core schema)
    # ------------------------------------------------------------------
    'league_rosters': {
        'used_by': ['etl', 'publish'],
        'entity': 'team',
        'schema': 'core',
        'primary_key': ['league_id', 'team_id'],
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
        'indexes': [
            {'name': 'league_id', 'columns': ['league_id']},
            {'name': 'team_id', 'columns': ['team_id']},
        ],
        'scope': 'roster',
    },
    'team_rosters': {
        'used_by': ['etl', 'publish'],
        'entity': 'player',
        'schema': 'core',
        'primary_key': ['team_id', 'player_id'],
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
        'indexes': [
            {'name': 'team_id', 'columns': ['team_id']},
            {'name': 'player_id', 'columns': ['player_id']},
        ],
        'scope': 'roster',
    },
    # ------------------------------------------------------------------
    # OPERATIONAL TABLES (per-league schema)
    #
    # ETL/publish share runs/tasks. ETL also owns backfill_tracker.
    # The ``pipeline`` column ('etl' | 'publish') acts as discriminator on
    # shared tables.
    #
    # runs              -- one row per pipeline execution (top-level audit)
    # tasks             -- one row per resumable work unit within a run:
    #                       ETL: dataset + tier + column group
    #                       publish: tab_name
    # backfill_tracker  -- per (entity, season, season_type, source) coverage
    #                      signature for historical stats completeness checks.
    #
    # Columns are defined in DB_COLUMNS; table metadata specifies primary_key
    # and unique constraints.  The DDL generator looks up column types from
    # the column registry.
    # ------------------------------------------------------------------
    'runs': {
        'used_by': ['etl', 'publish'],
        'entity': None,
        'schema': 'league',
        'primary_key': ['run_id'],
        'foreign_keys': [],
        'indexes': [
            {'name': 'pipeline_status', 'columns': ['pipeline', 'status']},
        ],
        'scope': 'runs',
    },
    'tasks': {
        'used_by': ['etl', 'publish'],
        'entity': None,
        'schema': 'league',
        'primary_key': ['task_id'],
        'unique_constraints': [['run_id', 'pipeline', 'item_key']],
        'foreign_keys': [
            {
                'column': 'run_id',
                'ref_schema': None,  # Same schema (league)
                'ref_table': 'runs',
                'ref_column': 'run_id',
                'on_update': 'CASCADE',
                'on_delete': 'CASCADE',
            },
        ],
        'indexes': [
            {'name': 'run_id_status', 'columns': ['run_id', 'status']},
        ],
        'scope': 'tasks',
    },
    'backfill_tracker': {
        'used_by': ['etl'],
        'entity': None,
        'schema': 'league',
        'primary_key': ['entity_type', 'season', 'season_type', 'source_key'],
        'foreign_keys': [],
        'indexes': [
            {'name': 'entity_season', 'columns': ['entity_type', 'season', 'season_type']},
        ],
        'scope': 'backfill',
    },
}


# ============================================================================
# FILTERED VIEWS
# ============================================================================

PROFILE_TABLES = {k: v for k, v in TABLES.items() if v['scope'] == 'profile'}
STATS_TABLES = {k: v for k, v in TABLES.items() if v['scope'] == 'stats'}
ROSTER_TABLES = {k: v for k, v in TABLES.items() if v['scope'] == 'roster'}
OPERATIONAL_TABLES = {k: v for k, v in TABLES.items() if v['scope'] in ('runs', 'tasks', 'backfill')}
