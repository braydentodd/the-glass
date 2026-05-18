"""
The Glass - Schema Helpers

Pure resolvers over the table registries in
:mod:`src.core.definitions.tables`.  Builds qualified table names.
"""

from src.core.definitions.tables import (
    CORE_SCHEMA,
    PROFILE_TABLES,
    STATS_TABLES,
)


_PROFILE_BY_ENTITY = {meta['entity']: name for name, meta in PROFILE_TABLES.items()}
_STATS_BY_ENTITY = {meta['entity']: name for name, meta in STATS_TABLES.items()}


def get_table_name(entity: str, scope: str, league_key: str = None) -> str:
    """Resolve the schema-qualified table name for an entity / scope.

    ``scope == 'profiles'`` -> ``core.{entity}_profiles`` (league_key ignored).
    ``scope == 'stats'``  -> ``{league_key}.{entity}_season_stats`` (league_key required).
    """
    if scope == 'profiles':
        if entity not in _PROFILE_BY_ENTITY:
            raise ValueError(f"No profile table for entity {entity!r}")
        return f'{CORE_SCHEMA}.{_PROFILE_BY_ENTITY[entity]}'

    if scope == 'stats':
        if entity not in _STATS_BY_ENTITY:
            raise ValueError(f"No stats table for entity {entity!r}")
        if not league_key:
            raise ValueError(f"league_key required for stats scope (entity {entity!r})")
        return f'{league_key}.{_STATS_BY_ENTITY[entity]}'

    raise ValueError(f"Unsupported scope: {scope!r}")
