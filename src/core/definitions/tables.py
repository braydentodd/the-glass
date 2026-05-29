"""
The Glass - Database Table Registry

Unified registry of every table in the database: profiles, stats, rosters,
and operational (run-tracking) tables.

Operational tables use a ``pipeline`` discriminator column so both ETL and
publish runs share single ``runs`` and ``tasks`` tables rather than
maintaining four mirrored tables.

Column definitions live in ``src.core.definitions.columns.DB_COLUMNS``.
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
VALID_ENTITY_TYPES = frozenset({'league', 'player', 'team'})
VALID_SCOPES = frozenset({'profiles', 'stats', 'rosters', 'staging', 'ops'})
VALID_REFRESH_MODES = frozenset({'null_only', 'always'})
VALID_FK_ACTIONS = frozenset({'CASCADE', 'RESTRICT', 'SET NULL', 'NO ACTION'})
VALID_MANAGERS = frozenset({'db', 'execution_context', 'in_season_source', 'perennial_source'})
VALID_FK_STRATEGIES = frozenset({'direct', 'profile_lookup'})

THE_GLASS_ID = 'the_glass_id'


def _schemas() -> frozenset:
    """Derive the set of schema names from the TABLES registry."""
    return frozenset(meta['schema'] for meta in TABLES.values() if meta.get('schema'))


# ============================================================================
# DB_COLUMNS SCHEMA
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
    entity: Union[str, None]
    schema: Union[str, None]
    primary_key: Union[List[str], None]
    foreign_keys: Union[List[FKDef], None]
    scope: Union[str, None]
    source_scopes: Union[List[str], None]
    unique_constraints: Union[List[List[str]], None]
    indexes: Union[List[IndexDef], None]
    source_ids: Union[bool, None]


TABLES: Dict[str, TableDef] = {

    # ------------------------------------------------------------------
    # PROFILES SCHEMA
    # ------------------------------------------------------------------
    'profiles.leagues': {
        'entity': 'league',
        'schema': 'profiles',
        'primary_key': ['the_glass_id'],
        'foreign_keys': [],
        'unique_constraints': [['league_key'], ['abbr']],
        'indexes': [],
        'scope': 'profiles',
        'source_ids': True,
    },
    'profiles.teams': {
        'entity': 'team',
        'schema': 'profiles',
        'primary_key': ['the_glass_id'],
        'foreign_keys': [],
        'unique_constraints': None,
        'indexes': [],
        'scope': 'profiles',
        'source_ids': True,
    },
    'profiles.players': {
        'entity': 'player',
        'schema': 'profiles',
        'primary_key': ['the_glass_id'],
        'foreign_keys': [],
        'unique_constraints': None,
        'indexes': [],
        'scope': 'profiles',
        'source_ids': True,
    },
    # ------------------------------------------------------------------
    # STAGING TABLES (profiles schema, transient ingestion)
    # ------------------------------------------------------------------
    'profiles.teams_staging': {
        'entity': 'team',
        'schema': 'profiles',
        'primary_key': ['league_id', 'source_id'],
        'foreign_keys': [],
        'unique_constraints': None,
        'indexes': [
            {'name': 'league_source', 'columns': ['league_id', 'source_key']},
            {'name': 'matched_glass_id', 'columns': ['matched_glass_id']},
        ],
        'scope': 'staging',
        'source_scopes': ['profiles', 'staging'],
        'source_ids': False,
    },
    'profiles.players_staging': {
        'entity': 'player',
        'schema': 'profiles',
        'primary_key': ['league_id', 'source_id'],
        'foreign_keys': [],
        'unique_constraints': None,
        'indexes': [
            {'name': 'league_source', 'columns': ['league_id', 'source_key']},
            {'name': 'matched_glass_id', 'columns': ['matched_glass_id']},
            {'name': 'team_source_id', 'columns': ['team_source_id']},
        ],
        'scope': 'staging',
        'source_scopes': ['profiles', 'rosters', 'staging'],
        'source_ids': False,
    },
    # ------------------------------------------------------------------
    # ROSTERS SCHEMA
    # ------------------------------------------------------------------
    'rosters.leagues': {
        'entity': 'team',
        'schema': 'rosters',
        'primary_key': ['league_id', 'team_id'],
        'foreign_keys': [
            {
                'column':     'league_id',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'team_id',
                'ref_schema': 'profiles',
                'ref_table':  'teams',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': [],
        'scope': 'rosters',
        'source_ids': False,
    },
    'rosters.teams': {
        'entity': 'player',
        'schema': 'rosters',
        'primary_key': ['team_id', 'player_id'],
        'foreign_keys': [
            {
                'column':     'team_id',
                'ref_schema': 'profiles',
                'ref_table':  'teams',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'player_id',
                'ref_schema': 'profiles',
                'ref_table':  'players',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': [],
        'scope': 'rosters',
        'source_ids': False,
    },
    # ------------------------------------------------------------------
    # STATS SCHEMA
    # ------------------------------------------------------------------
    'stats.players': {
        'entity': 'player',
        'schema': 'stats',
        'primary_key': ['player_id', 'team_id', 'season', 'season_type'],
        'foreign_keys': [
            {
                'column':     'player_id',
                'ref_schema': 'profiles',
                'ref_table':  'players',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'team_id',
                'ref_schema': 'profiles',
                'ref_table':  'teams',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'league_id',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': [
            {'name': 'season_type_season', 'columns': ['season_type', 'season']},
        ],
        'scope': 'stats',
        'source_ids': False,
    },
    'stats.teams': {
        'entity': 'team',
        'schema': 'stats',
        'primary_key': ['team_id', 'season', 'season_type'],
        'foreign_keys': [
            {
                'column':     'team_id',
                'ref_schema': 'profiles',
                'ref_table':  'teams',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
            {
                'column':     'league_id',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': [
            {'name': 'season_type_season', 'columns': ['season_type', 'season']},
        ],
        'scope': 'stats',
        'source_ids': False,
    },
    # ------------------------------------------------------------------
    # OPS SCHEMA
    # ------------------------------------------------------------------
    'ops.runs': {
        'entity': None,
        'schema': 'ops',
        'primary_key': ['process_id'],
        'foreign_keys': [],
        'unique_constraints': None,
        'indexes': [
            {'name': 'pipeline_status', 'columns': ['pipeline', 'status']},
        ],
        'scope': 'ops',
        'source_ids': False,
    },
    'ops.tasks': {
        'entity': None,
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
                'column':     'league_id',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': [['run_id', 'item_key']],
        'indexes': [
            {'name': 'run_id_status', 'columns': ['run_id', 'status']},
        ],
        'scope': 'ops',
        'source_ids': False,
    },
    'ops.coverages': {
        'entity': None,
        'schema': 'ops',
        'primary_key': ['league_id', 'entity_type', 'season', 'season_type', 'source_key'],
        'foreign_keys': [
            {
                'column':     'league_id',
                'ref_schema': 'profiles',
                'ref_table':  'leagues',
                'ref_column': 'the_glass_id',
                'strategy':   'profile_lookup',
                'on_update':  'CASCADE',
                'on_delete':  'CASCADE',
            },
        ],
        'unique_constraints': None,
        'indexes': [],
        'scope': 'ops',
        'source_ids': False,
    },
}


PROFILE_TABLES = {name: meta for name, meta in TABLES.items() if meta.get('scope') == 'profiles'}
STATS_TABLES = {name: meta for name, meta in TABLES.items() if meta.get('scope') == 'stats'}
ROSTER_TABLES = {name: meta for name, meta in TABLES.items() if meta.get('scope') == 'rosters'}
STAGING_TABLES = {name: meta for name, meta in TABLES.items() if meta.get('scope') == 'staging'}
OPS_TABLES = {name: meta for name, meta in TABLES.items() if meta.get('scope') == 'ops'}