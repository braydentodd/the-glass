"""
The Glass - Call Group Builder

Transforms column configuration into executable API call groups for any
data source.  A "call group" is a batch of columns that can be satisfied
by a single API call.

Functions accept source-specific config (``source_key``, ``datasets``)
as parameters rather than importing from a specific source, keeping this
module source-agnostic.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.definitions.columns import DB_COLUMNS

logger = logging.getLogger(__name__)


# Default transform per PostgreSQL base type, applied when a source does not
# declare its own ``transform`` and is not a pipeline / multi-call shape.
# Inlined here because :func:`_enrich_source` is the only consumer.
_TYPE_TRANSFORMS: Dict[str, str] = {
    'SMALLINT': 'safe_int',
    'INTEGER':  'safe_int',
    'BIGINT':   'safe_int',
    'VARCHAR':  'safe_str',
    'TEXT':     'safe_str',
    'CHAR':     'safe_str',
}


# ============================================================================
# INTERNAL HELPERS
# ============================================================================

def _enrich_source(source: Dict[str, Any], col_meta: Dict[str, Any]) -> Dict[str, Any]:
    """Add a default transform to a source based on column type if not already set."""
    enriched = {**source}
    if 'transform' not in enriched and 'pipeline' not in enriched and 'multi_call' not in enriched:
        base_type = col_meta.get('type', '').split('(')[0]
        enriched['transform'] = _TYPE_TRANSFORMS.get(base_type, 'safe_int')
    if 'removed_refresh_mode' not in enriched:
        enriched['removed_refresh_mode'] = col_meta.get('removed_refresh_mode', 'null_only')
    return enriched


def _get_source_definition(
    col_meta: Dict[str, Any],
    entity: str,
    source_key: str,
) -> Optional[Dict[str, Any]]:
    """Return the per-entity source definition under ``col_meta['sources'][source_key][entity]``,
    or ``None`` if absent."""
    source_entries = (col_meta.get('sources') or {}).get(source_key)
    if not source_entries:
        return None
    return source_entries.get(entity)


# ============================================================================
# DATASET AVAILABILITY
# ============================================================================

def is_dataset_available(
    dataset_name: str,
    season: str,
    datasets: Dict[str, Dict[str, Any]],
) -> bool:
    """Check whether a dataset has data for the given season."""
    ds = datasets.get(dataset_name)
    if not ds:
        return False
    min_season = ds.get('min_season')
    if min_season is None:
        return True
    return season >= min_season


# ============================================================================
# COLUMN QUERIES
# ============================================================================

def get_columns_for_dataset(
    dataset_name: str,
    entity: str,
    source_key: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Find all columns whose source definition maps to the given dataset.

    Returns ``{col_name: enriched_source_dict}`` with default transforms injected.
    """
    matched: Dict[str, Dict[str, Any]] = {}

    for col_name, col_meta in DB_COLUMNS.items():
        source = _get_source_definition(col_meta, entity, source_key)
        if not source:
            continue

        ds = source.get('dataset')
        if not ds:
            ds = source.get('pipeline', {}).get('dataset')
        if ds != dataset_name:
            continue

        if params:
            source_params = source.get('params', {})
            if not all(source_params.get(k) == v for k, v in params.items()):
                continue

        matched[col_name] = _enrich_source(source, col_meta)

    return matched


def get_all_sources_for_entity(
    entity: str,
    source_key: str,
    datasets: Dict[str, Dict[str, Any]],
    season: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return every column with a source definition for the given entity.

    If ``season`` is provided, excludes datasets not available for that season.
    """
    matched: Dict[str, Dict[str, Any]] = {}

    for col_name, col_meta in DB_COLUMNS.items():
        source = _get_source_definition(col_meta, entity, source_key)
        if not source:
            continue

        if season:
            ds = source.get('dataset') or source.get('pipeline', {}).get('dataset', '')
            if not is_dataset_available(ds, season, datasets):
                continue

        matched[col_name] = _enrich_source(source, col_meta)

    return matched


# ============================================================================
# EXECUTION TIER RESOLUTION
# ============================================================================

def tier_for_dataset(
    dataset: str,
    datasets: Dict[str, Dict[str, Any]],
) -> str:
    """Get the default execution tier for a dataset."""
    return datasets.get(dataset, {}).get('execution_tier', 'league')


def tier_for_source(
    source: Dict[str, Any],
    dataset: str,
    datasets: Dict[str, Dict[str, Any]],
) -> str:
    """Resolve execution tier from a source config or the dataset default."""
    tier = source.get('tier')
    if tier:
        return tier
    pipeline = source.get('pipeline', {})
    if pipeline.get('tier'):
        return pipeline['tier']
    return tier_for_dataset(dataset, datasets)


# ============================================================================
# CALL GROUP BUILDING
# ============================================================================

def build_call_groups(
    entity: str,
    season: str,
    source_key: str,
    datasets: Dict[str, Dict[str, Any]],
    scope: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Group all columns for ``entity`` into API call batches.

    Walks DB_COLUMNS, groups simple/derived columns that share the same
    (dataset, params) so each batch requires exactly one API call.
    Multi-call, pipeline, and team_call columns get their own entries.

    Args:
        scope: If set, only include columns whose ``scope`` matches this
               value or is ``'both'``.

    Returns a list of dicts, each with:
        dataset, params, tier, columns ({col_name: enriched_source})
    """
    simple_groups: Dict[tuple, Dict[str, Dict[str, Any]]] = {}
    special: List[Dict[str, Any]] = []

    for col_name, col_meta in DB_COLUMNS.items():
        if scope:
            col_scope = col_meta.get('scope')
            if col_scope != scope and col_scope != 'both':
                continue

        source = _get_source_definition(col_meta, entity, source_key)
        if not source:
            continue

        enriched = _enrich_source(source, col_meta)

        ds = enriched.get('dataset')
        if not ds:
            ds = enriched.get('pipeline', {}).get('dataset')
        if not ds:
            continue
        if not is_dataset_available(ds, season, datasets):
            continue

        if 'multi_call' in enriched or 'pipeline' in enriched:
            special.append({
                'dataset': ds,
                'params': enriched.get('params', {}),
                'tier': tier_for_source(enriched, ds, datasets),
                'columns': {col_name: enriched},
            })
        elif enriched.get('tier') == 'team_call':
            special.append({
                'dataset': ds,
                'params': {},
                'tier': 'team_call',
                'columns': {col_name: enriched},
            })
        else:
            params = enriched.get('params', {})
            key = (ds, frozenset(sorted(params.items())))
            simple_groups.setdefault(key, {})[col_name] = enriched

    groups: List[Dict[str, Any]] = []

    for (ds, frozen_params), cols in simple_groups.items():
        removed_refresh_mode = 'always' if any(
            src.get('removed_refresh_mode') == 'always' for src in cols.values()
        ) else 'null_only'
        groups.append({
            'dataset': ds,
            'params': dict(frozen_params),
            'tier': tier_for_dataset(ds, datasets),
            'columns': cols,
            'removed_refresh_mode': removed_refresh_mode,
        })

    # Merge team_call columns that share the same dataset into one group
    team_call_merged: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for item in special:
        if item['tier'] == 'team_call':
            team_call_merged.setdefault(item['dataset'], {}).update(item['columns'])
        else:
            groups.append(item)
    for ds, cols in team_call_merged.items():
        groups.append({
            'dataset': ds,
            'params': {},
            'tier': 'team_call',
            'columns': cols,
            'removed_refresh_mode': 'null_only',
        })

    return groups
