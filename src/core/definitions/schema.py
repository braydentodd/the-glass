"""
Shoot the Sheet - Database Schema Registry

Unified registry of every table and shared sequence in the database:
profiles, stats, rosters, and operational (run-tracking) tables.

Operational tables use a ``pipeline`` discriminator column so both ETL and
publish runs share single ``runs`` and ``tasks`` tables rather than
maintaining four mirrored tables.

Column definitions live in ``src.core.definitions.db_columns.DB_COLUMNS``.
Operational and roster tables reference columns by name; the DDL generator
looks up type, nullable, default, and other metadata from the column registry.
"""

from typing import TypedDict, Dict, List, Union


# ============================================================================
# ALLOWED VALUE SETS
# ============================================================================

VALID_PG_TYPES = frozenset({
    'SERIAL', 'SMALLINT', 'INTEGER', 'BIGINT', 'VARCHAR', 'TEXT', 'CHAR',
    'BOOLEAN', 'TIMESTAMP', 'DATE', 'NUMERIC', 'REAL', 'DOUBLE PRECISION',
})
VALID_FK_ACTIONS = frozenset({'CASCADE', 'RESTRICT', 'SET NULL', 'NO ACTION'})
VALID_FK_STRATEGIES = frozenset({'direct', 'profile_lookup'})

# Default transform per PostgreSQL base type, applied when a source does not
# declare its own ``transform`` and is not a pipeline / multi-call shape.
DEFAULT_TYPE_TRANSFORMS: Dict[str, str] = {
    'SMALLINT': 'safe_int',
    'INTEGER':  'safe_int',
    'BIGINT':   'safe_int',
    'VARCHAR':  'safe_str',
    'TEXT':     'safe_str',
    'CHAR':     'safe_str',
}


# ============================================================================
# SEQUENCE REGISTRY
# ============================================================================
# Shared sequences referenced by column defaults via nextval().
# Kept explicit so DDL can validate coverage without regex scanning.
# ============================================================================

class SequenceDef(TypedDict):
    schema: str
    owner_columns: List[str]


SEQUENCES: Dict[str, SequenceDef] = {
    'ops.process_id_seq': {
        'schema': 'ops',
        'owner_columns': ['process_id'],
    },
    'profiles.sts_id_seq': {
        'schema': 'profiles',
        'owner_columns': ['sts_id'],
    },
}


# ============================================================================
# TABLE DEFINITION TYPES
# ============================================================================

class FKDef(TypedDict):
    column: str
    ref_schema: str
    ref_table: str
    ref_column: str
    strategy: str
    on_update: str
    on_delete: str

class IndexDef(TypedDict):
    name: str
    columns: List[str]

class TableDef(TypedDict):
    entity: str
    schema: Union[str, None]
    primary_key: Union[List[str], None]
    foreign_keys: Union[List[FKDef], None]
    unique_constraints: Union[List[List[str]], None]
    indexes: Union[List[IndexDef], None]


# ============================================================================
# TABLE REGISTRY
# ============================================================================

TABLES: Dict[str, TableDef] = {

    # ------------------------------------------------------------------
    # PROFILES SCHEMA
    # ------------------------------------------------------------------
    'leagues': {
        'entity': 'league',
        'schema': 'profiles',
        'primary_key': ['code'],
        'foreign_keys': None,
        'unique_constraints': None,
        'indexes': None
    },
    'countries': {
        'entity': 'country',
        'schema': 'profiles',
        'primary_key': ['code'],
        'foreign_keys': [
            {
                'column': 'sovereign_code',
                'ref_schema': 'profiles',
                'ref_table': 'countries',
                'ref_column': 'code',
                'strategy': 'profile_lookup',
                'on_update': 'CASCADE',
                'on_delete': 'SET NULL'
            }
        ],
        'unique_constraints': None,
        'indexes': None
    },
    'teams': {
        'entity': 'team',
        'schema': 'profiles',
        'primary_key': ['sts_id'],
        'foreign_keys': [
            {
                'column': 'country_code',
                'ref_schema': 'profiles',
                'ref_table': 'countries',
                'ref_column': 'code',
                'strategy': 'profile_lookup',
                'on_update': 'CASCADE',
                'on_delete': 'CASCADE'
            }
        ],
        'unique_constraints': None,
        'indexes': None
    },
    'players': {
        'entity': 'player',
        'schema': 'profiles',
        'primary_key': ['sts_id'],
        'foreign_keys': None,
        'unique_constraints': None,
        'indexes': None
    },
    # ------------------------------------------------------------------
    # STAGING TABLES (carbon copies of parent tables, minus sts_id & FKs)
    # ------------------------------------------------------------------
    'teams_staging': {
        'entity': 'team',
        'schema': 'profiles',
        'primary_key': ['identity', 'identity_code'],
        'foreign_keys': None,
        'unique_constraints': None,
        'indexes': None
    },
    'players_staging': {
        'entity': 'player',
        'schema': 'profiles',
        'primary_key': ['identity', 'identity_code'],
        'foreign_keys': None,
        'unique_constraints': None,
        'indexes': None
    },
    'player_seasons_staging': {
        'entity': 'player',
        'schema': 'stats',
        'primary_key': ['identity', 'identity_code', 'season', 'season_type'],
        'foreign_keys': None,
        'unique_constraints': None,
        'indexes': None
    },
    'team_seasons_staging': {
        'entity': 'team',
        'schema': 'stats',
        'primary_key': ['identity', 'identity_code', 'team_identity_code', 'season', 'season_type'],
        'foreign_keys': None,
        'unique_constraints': None,
        'indexes': None
    },
    'leagues_teams_staging': {
        'entity': 'league',
        'schema': 'rosters',
        'primary_key': ['league_code', 'team_identity_code'],
        'foreign_keys': None,
        'unique_constraints': None,
        'indexes': None
    },
    'teams_players_staging': {
        'entity': 'team',
        'schema': 'rosters',
        'primary_key': ['league_code', 'team_identity_code', 'player_identity_code'],
        'foreign_keys': None,
        'unique_constraints': None,
        'indexes': None
    },
    # ------------------------------------------------------------------
    # ROSTERS SCHEMA
    # ------------------------------------------------------------------
    'leagues_teams': {
        'entity': 'league',
        'schema': 'rosters',
        'primary_key': ['league_code', 'team_id'],
        'foreign_keys': [
            {
                'column':     'league_code',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'code',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'team_id',
                'ref_schema': 'profiles',
                'ref_table':  'teams',
                'ref_column': 'sts_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': None
    },
    'teams_players': {
        'entity': 'team',
        'schema': 'rosters',
        'primary_key': ['league_code', 'team_id', 'player_id'],
        'foreign_keys': [
            {
                'column':     'league_code',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'code',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'team_id',
                'ref_schema': 'profiles',
                'ref_table':  'teams',
                'ref_column': 'sts_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'player_id',
                'ref_schema': 'profiles',
                'ref_table':  'players',
                'ref_column': 'sts_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': None
    },
    'countries_players': {
        'entity': 'country',
        'schema': 'rosters',
        'primary_key': ['player_id', 'country_code'],
        'foreign_keys': [
            {
                'column':     'player_id',
                'ref_schema': 'profiles',
                'ref_table':  'players',
                'ref_column': 'sts_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'country_code',
                'ref_schema': 'profiles',
                'ref_table':  'countries',
                'ref_column': 'code',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': None
    },
    'identities_entities': {
        'entity': 'identity',
        'schema': 'rosters',
        'primary_key': ['identity', 'code', 'entity'],
        'foreign_keys': None,
        'unique_constraints': None,
        'indexes': None
    },
    # ------------------------------------------------------------------
    # STATS SCHEMA
    # ------------------------------------------------------------------
    'player_seasons': {
        'entity': 'player',
        'schema': 'stats',
        'primary_key': ['league_code', 'player_id', 'team_id', 'season', 'season_type'],
        'foreign_keys': [
            {
                'column':     'player_id',
                'ref_schema': 'profiles',
                'ref_table':  'players',
                'ref_column': 'sts_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'team_id',
                'ref_schema': 'profiles',
                'ref_table':  'teams',
                'ref_column': 'sts_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'league_code',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'code',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': [
            {'name': 'season_type_season', 'columns': ['season_type', 'season']},
        ],
    },
    'team_seasons': {
        'entity': 'team',
        'schema': 'stats',
        'primary_key': ['league_code', 'team_id', 'season', 'season_type'],
        'foreign_keys': [
            {
                'column':     'team_id',
                'ref_schema': 'profiles',
                'ref_table':  'teams',
                'ref_column': 'sts_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'league_code',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'code',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': [
            {'name': 'season_type_season', 'columns': ['season_type', 'season']},
        ]
    },
    # ------------------------------------------------------------------
    # OPS SCHEMA
    # ------------------------------------------------------------------
    'runs': {
        'entity': 'run',
        'schema': 'ops',
        'primary_key': ['process_id'],
        'foreign_keys': None,
        'unique_constraints': None,
        'indexes': [
            {'name': 'pipeline_status', 'columns': ['pipeline', 'status']},
        ],
    },
    'tasks': {
        'entity': 'task',
        'schema': 'ops',
        'primary_key': ['process_id'],
        'foreign_keys': [
            {
                'column':     'run_id',
                'ref_schema': 'ops',
                'ref_table':  'runs',
                'ref_column': 'process_id',
                'strategy':   'direct',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'league_code',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'code',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': [['run_id', 'task_name']],
        'indexes': [
            {'name': 'run_id_status', 'columns': ['run_id', 'status']},
        ],
    },
    'coverages': {
        'entity': 'coverage',
        'schema': 'ops',
        'primary_key': ['league_code', 'entity', 'season', 'season_type', 'identity', 'source_params', 'col_name', 'dataset', 'dataset_params', 'field'],
        'foreign_keys': [
            {
                'column':     'league_code',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'code',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': None,
    }
}

# table_name -> entity (e.g. 'players' -> 'player')
TABLE_ENTITY: Dict[str, str] = {
    table_name: meta['entity'] for table_name, meta in TABLES.items()
}
