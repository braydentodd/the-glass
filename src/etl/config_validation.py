"""
The Glass - ETL Config Validation

ETL-specific validation: cross-reference checks, PostgreSQL type validation,
and source structure checks.  Uses the generic validation engine from
``src.core.config_validation``.

Schemas are co-located with their config files:

  - DB_COLUMNS_SCHEMA, LEAGUES_SCHEMA, SOURCES_SCHEMA, PROFILE_TABLES_SCHEMA,
    STATS_TABLES_SCHEMA, JUNCTION_TABLES_SCHEMA, ETL_CONFIG_SCHEMA,
    ETL_TABLES_SCHEMA  -> src/etl/definitions/config.py
  - ENDPOINTS_SCHEMA, SEASON_TYPES_SCHEMA  -> src/etl/sources/<source>/config.py

Add a new config?  Define a schema dict next to the data, then register it
in :func:`validate_config`.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.config_validation import validate_dict_config, validate_flat_config

logger = logging.getLogger(__name__)


VALID_TRANSFORMS = {
    'safe_int', 'safe_float', 'safe_str',
    'parse_height', 'parse_birthdate', 'format_season',
}


# ============================================================================
# CROSS-REFERENCE VALIDATORS
# ============================================================================

def _validate_pg_types(db_columns: Dict[str, Dict]) -> List[str]:
    """Validate that all DB_COLUMNS types are valid PostgreSQL types."""
    from src.etl.definitions import VALID_PG_TYPES

    errors = []
    for col_name, meta in db_columns.items():
        col_type = meta.get('type', '')
        base = col_type.split('(')[0].upper()
        if base not in VALID_PG_TYPES:
            errors.append(f"DB_COLUMNS['{col_name}']: unknown type '{col_type}'")
    return errors


def _validate_source_structure(
    db_columns: Dict[str, Dict],
    sources: Dict[str, Dict],
) -> List[str]:
    """Validate the nested sources structure in DB_COLUMNS.

    Each provider key must exist in SOURCES, and each entity key must be in
    VALID_ENTITY_TYPES and in the provider's ``applies_to`` list.
    """
    from src.etl.definitions import VALID_ENTITY_TYPES
    errors = []
    for col_name, meta in db_columns.items():
        col_sources = meta.get('sources')
        if col_sources is None:
            continue

        prefix = f"DB_COLUMNS['{col_name}']"
        if not isinstance(col_sources, dict):
            errors.append(f"{prefix}: 'sources' must be dict or None")
            continue

        for provider, entities in col_sources.items():
            if provider not in sources:
                errors.append(f"{prefix}: sources['{provider}'] not registered in SOURCES")
                continue
            applies_to = sources[provider].get('applies_to', [])
            if not isinstance(entities, dict):
                errors.append(f"{prefix}: sources['{provider}'] must be dict")
                continue
            for entity_name, source_def in entities.items():
                if entity_name not in VALID_ENTITY_TYPES:
                    errors.append(
                        f"{prefix}: sources.{provider} has "
                        f"invalid entity '{entity_name}'"
                    )
                elif entity_name not in applies_to and entity_name != 'opponent':
                    errors.append(
                        f"{prefix}: sources.{provider}.{entity_name} - "
                        f"source '{provider}' does not declare applies_to {entity_name!r}"
                    )
                if not isinstance(source_def, dict):
                    errors.append(
                        f"{prefix}: sources.{provider}.{entity_name} must be dict"
                    )
    return errors


def _validate_endpoint_refs(
    db_columns: Dict[str, Dict],
    endpoints: Dict[str, Dict],
) -> List[str]:
    """Validate that source endpoint references exist in ENDPOINTS."""
    errors = []
    for col_name, meta in db_columns.items():
        sources = meta.get('sources')
        if not sources or not isinstance(sources, dict):
            continue

        prefix = f"DB_COLUMNS['{col_name}']"
        for provider, entities in sources.items():
            if not isinstance(entities, dict):
                continue
            for entity_name, source_def in entities.items():
                if not isinstance(source_def, dict):
                    continue
                ep = (
                    source_def.get('endpoint')
                    or source_def.get('pipeline', {}).get('endpoint')
                )
                if ep and ep not in endpoints:
                    errors.append(
                        f"{prefix}: references unknown endpoint '{ep}'"
                    )
    return errors


def _validate_stats_primary_keys(
    stats_tables: Dict[str, Dict],
    db_columns: Dict[str, Dict],
) -> List[str]:
    """Validate that every PK column on a stats table is either the synthetic
    ``the_glass_id`` or a column declared in DB_COLUMNS."""
    from src.etl.definitions import THE_GLASS_ID_COLUMN
    errors = []
    for table_name, meta in stats_tables.items():
        for col in meta.get('primary_key', []):
            if col == THE_GLASS_ID_COLUMN:
                continue
            if col not in db_columns:
                errors.append(
                    f"STATS_TABLES['{table_name}']: primary_key references "
                    f"unknown column '{col}'"
                )
    return errors


def _validate_league_reader_sources(
    leagues: Dict[str, Dict],
    sources: Dict[str, Dict],
) -> List[str]:
    """Each league.reader_source must exist in SOURCES with role=reader and
    list the league in its leagues array."""
    errors = []
    for league_key, meta in leagues.items():
        rs = meta.get('reader_source')
        if not rs:
            errors.append(f"LEAGUES['{league_key}']: missing reader_source")
            continue
        if rs not in sources:
            errors.append(
                f"LEAGUES['{league_key}']: reader_source '{rs}' not in SOURCES"
            )
            continue
        src = sources[rs]
        if src.get('role') != 'reader':
            errors.append(
                f"LEAGUES['{league_key}']: reader_source '{rs}' has role "
                f"{src.get('role')!r}, expected 'reader'"
            )
        if league_key not in src.get('leagues', []):
            errors.append(
                f"LEAGUES['{league_key}']: reader_source '{rs}' does not list "
                f"'{league_key}' in its leagues"
            )
    return errors


# ============================================================================
# PUBLIC API
# ============================================================================

def validate_config(
    endpoints: Optional[Dict[str, Any]] = None,
    endpoints_schema: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Validate every ETL configuration dict at startup.

    Args:
        endpoints:        Optional ENDPOINTS dict from the active source config.
                          If supplied, source endpoint references are cross-checked.
        endpoints_schema: Optional schema dict for validating ``endpoints``.

    Raises:
        RuntimeError: if any validation errors are found.
    """
    from src.etl.definitions import (
        DB_COLUMNS,
        DB_COLUMNS_SCHEMA,
        ETL_CONFIG,
        ETL_CONFIG_SCHEMA,
        ETL_TABLES,
        ETL_TABLES_SCHEMA,
        JUNCTION_TABLES,
        JUNCTION_TABLES_SCHEMA,
        LEAGUES,
        LEAGUES_SCHEMA,
        PROFILE_TABLES,
        PROFILE_TABLES_SCHEMA,
        SOURCES,
        SOURCES_SCHEMA,
        STATS_TABLES,
        STATS_TABLES_SCHEMA,
    )
    from src.core.config_validation import validate_core_constants

    errors: List[str] = []

    errors.extend(validate_core_constants())

    # Schema-level validations
    errors.extend(validate_dict_config(DB_COLUMNS, DB_COLUMNS_SCHEMA, 'DB_COLUMNS'))
    errors.extend(validate_dict_config(LEAGUES, LEAGUES_SCHEMA, 'LEAGUES'))
    errors.extend(validate_dict_config(SOURCES, SOURCES_SCHEMA, 'SOURCES'))
    errors.extend(validate_dict_config(PROFILE_TABLES, PROFILE_TABLES_SCHEMA, 'PROFILE_TABLES'))
    errors.extend(validate_dict_config(STATS_TABLES, STATS_TABLES_SCHEMA, 'STATS_TABLES'))
    errors.extend(validate_dict_config(JUNCTION_TABLES, JUNCTION_TABLES_SCHEMA, 'JUNCTION_TABLES'))
    errors.extend(validate_dict_config(ETL_TABLES, ETL_TABLES_SCHEMA, 'ETL_TABLES'))
    errors.extend(validate_flat_config(ETL_CONFIG, ETL_CONFIG_SCHEMA, 'ETL_CONFIG'))

    if endpoints and endpoints_schema:
        errors.extend(validate_dict_config(endpoints, endpoints_schema, 'ENDPOINTS'))

    # Type and structural validations
    errors.extend(_validate_pg_types(DB_COLUMNS))
    errors.extend(_validate_source_structure(DB_COLUMNS, SOURCES))
    errors.extend(_validate_stats_primary_keys(STATS_TABLES, DB_COLUMNS))
    errors.extend(_validate_league_reader_sources(LEAGUES, SOURCES))

    if endpoints:
        errors.extend(_validate_endpoint_refs(DB_COLUMNS, endpoints))

    if errors:
        for err in errors:
            logger.error('Config validation: %s', err)
        raise RuntimeError(
            f"Config validation failed with {len(errors)} error(s)"
        )

    logger.info(
        'Config validation passed (%d columns, %d profiles, %d stats, %d junctions)',
        len(DB_COLUMNS), len(PROFILE_TABLES), len(STATS_TABLES), len(JUNCTION_TABLES),
    )
    return errors
