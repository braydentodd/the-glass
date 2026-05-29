"""
The Glass - Schema Helpers

Pure resolvers over the table registries in
:mod:`src.core.definitions.tables`.  Builds qualified table names.
"""

from src.core.definitions.tables import TABLES


def _normalize_scope(s: str) -> str:
    if not s:
        return s
    return s if s.endswith('s') else f"{s}s"


def get_table_name(entity: str, scope: str, _league_key: str = None) -> str:
    """Resolve the schema-qualified table name for an entity / scope.

    ``scope == 'profiles'`` -> ``profiles.{entity}``
    ``scope == 'stats'``    -> ``stats.{entity}``
    ``scope == 'rosters'``  -> ``rosters.{entity}``
    ``scope == 'staging'``  -> ``profiles.{entity}_staging``
    """
    norm_scope = _normalize_scope(scope)

    candidates = []
    for name, meta in TABLES.items():
        meta_scope = _normalize_scope(meta.get('scope') or '')
        if meta.get('entity') != entity:
            continue
        if meta_scope != norm_scope:
            continue
        candidates.append((name, meta))

    if not candidates:
        raise ValueError(f"No table for entity {entity!r} scope {scope!r}")
    if len(candidates) > 1:
        raise ValueError(f"Ambiguous table resolution for entity {entity!r} scope {scope!r}")

    _name, meta = candidates[0]
    schema = meta.get('schema')
    if not schema:
        raise ValueError(f"No schema defined for entity {entity!r} scope {scope!r}")
    bare_name = _name.split('.', 1)[1] if '.' in _name else _name
    return f"{schema}.{bare_name}"
