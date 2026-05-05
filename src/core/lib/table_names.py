"""
The Glass - Schema Helpers

Pure resolvers over the table registries in
:mod:`src.core.definitions.db_tables`.  Builds qualified table names and
iterates over per-schema table sets.
"""

from typing import Iterable, Tuple

from src.core.definitions.db_tables import (
    CORE_SCHEMA,
    JUNCTION_TABLES,
    PROFILE_TABLES,
    STATS_TABLES,
)


_PROFILE_BY_ENTITY = {meta['entity']: name for name, meta in PROFILE_TABLES.items()}
_STATS_BY_ENTITY = {meta['entity']: name for name, meta in STATS_TABLES.items()}


def get_table_name(entity: str, scope: str, league_key: str = None) -> str:
    """Resolve the schema-qualified table name for an entity / scope.

    ``scope == 'entity'`` -> ``core.{entity}_profiles`` (league_key ignored).
    ``scope == 'stats'``  -> ``{league_key}.{entity}_season_stats`` (league_key required).
    """
    if scope == 'entity':
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


def iter_core_tables() -> Iterable[Tuple[str, str, dict]]:
    """Yield ``(table_name, kind, meta)`` for every core-schema table.

    ``kind`` is ``'profile'`` or ``'junction'``.
    """
    for name, meta in PROFILE_TABLES.items():
        yield name, 'profile', meta
    for name, meta in JUNCTION_TABLES.items():
        yield name, 'junction', meta


def iter_league_tables() -> Iterable[Tuple[str, str, dict]]:
    """Yield ``(table_name, kind, meta)`` for every per-league table."""
    for name, meta in STATS_TABLES.items():
        yield name, 'stats', meta
