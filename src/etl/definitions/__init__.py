"""
The Glass - ETL Definitions Package

Re-exports all definition symbols and provides high-level resolvers used
across the ETL and publish layers.  Resolvers translate between leagues,
sources, entities, and physical schema artifacts.

Naming conventions:
    {source_key}_id            -> per-source identity column on profile tables
    core.{entity}_profiles     -> entity tables (shared across leagues)
    {league_key}.{entity}_season_stats -> stats tables (per-league schema)
    core.league_rosters        -> league/team membership junction
    core.team_rosters          -> team/player membership junction
"""

from datetime import datetime
from typing import Iterable, List, Tuple

from src.core.config import format_season_label
from src.etl.definitions.config import (                           # noqa: F401
    CORE_SCHEMA,
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
    THE_GLASS_ID_COLUMN,
    THE_GLASS_ID_SEQUENCE,
    THE_GLASS_ID_TYPE,
    TYPE_TRANSFORMS,
    VALID_ENTITY_TYPES,
    VALID_FK_ACTIONS,
    VALID_PG_TYPES,
    VALID_REFRESH_MODES,
    VALID_SCHEMA_KINDS,
    VALID_SCOPES,
    VALID_SOURCE_ROLES,
    VALID_UPDATE_FREQUENCIES,
)
from src.etl.definitions.columns import DB_COLUMNS                  # noqa: F401


# ---------------------------------------------------------------------------
# Source / league lookups
# ---------------------------------------------------------------------------

def get_reader_source(league_key: str) -> str:
    """Return the source registry key configured to read data for a league.

    Raises:
        ValueError: if the league has no reader_source configured or the key
                    is not registered in SOURCES.
    """
    if league_key not in LEAGUES:
        raise ValueError(f"Unknown league: {league_key!r}")

    source_key = LEAGUES[league_key].get('reader_source')
    if not source_key:
        raise ValueError(f"League {league_key!r} has no reader_source configured")

    if source_key not in SOURCES:
        raise ValueError(
            f"League {league_key!r} reader_source {source_key!r} is not in SOURCES"
        )
    if SOURCES[source_key]['role'] != 'reader':
        raise ValueError(
            f"Source {source_key!r} configured as reader for {league_key!r} "
            f"has role={SOURCES[source_key]['role']!r}"
        )
    return source_key


def get_writer_sources(league_key: str) -> List[str]:
    """Return all writer sources that operate on the given league."""
    return [
        key for key, meta in SOURCES.items()
        if meta['role'] == 'writer' and league_key in meta['leagues']
    ]


def get_source_id_column(source_key: str) -> str:
    """Return the per-source identity column name for profile tables.

    Convention: ``{source_key}_id`` (e.g. ``nba_api_id``).
    """
    if source_key not in SOURCES:
        raise ValueError(f"Unknown source: {source_key!r}")
    return f'{source_key}_id'


def get_source_id_columns_for_entity(entity: str) -> List[Tuple[str, str]]:
    """Return ``[(column_name, pg_type), ...]`` of source-id columns to add
    to the profile table for ``entity``, in deterministic order.

    A source contributes a column when it has a non-null ``entity_id_type``
    and includes ``entity`` in its ``applies_to`` list.
    """
    columns: List[Tuple[str, str]] = []
    for source_key in sorted(SOURCES):
        meta = SOURCES[source_key]
        if meta.get('entity_id_type') is None:
            continue
        if entity not in meta.get('applies_to', []):
            continue
        columns.append((get_source_id_column(source_key), meta['entity_id_type']))
    return columns


# ---------------------------------------------------------------------------
# Table-name resolution
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Season helpers (calendar-flip aware)
# ---------------------------------------------------------------------------

def _parse_flip_md(value: str) -> Tuple[int, int]:
    """Parse ``'MM-DD'`` into ``(month, day)``."""
    month_str, day_str = value.split('-')
    return int(month_str), int(day_str)


def get_current_season_year_for_league(league_key: str, now: datetime = None) -> int:
    """End-year of the current season for a league, respecting calendar_flip_md."""
    if league_key not in LEAGUES:
        raise ValueError(f"Unknown league: {league_key!r}")
    flip_month, flip_day = _parse_flip_md(LEAGUES[league_key]['calendar_flip_md'])
    now = now or datetime.now()
    if (now.month, now.day) >= (flip_month, flip_day):
        return now.year + 1
    return now.year


def get_current_season_for_league(league_key: str, now: datetime = None) -> str:
    """Current season label for a league (e.g. ``'2025-26'``)."""
    return format_season_label(get_current_season_year_for_league(league_key, now))


def get_retained_seasons(league_key: str, current_season: str) -> List[str]:
    """Return retained seasons (oldest to newest) under ``LEAGUES[..].retention_seasons``."""
    if league_key not in LEAGUES:
        raise ValueError(f"Unknown league: {league_key!r}")
    count = LEAGUES[league_key]['retention_seasons']
    end_year = int(current_season.split('-')[0]) + 1
    return [format_season_label(end_year - count + i + 1) for i in range(count)]


def get_oldest_retained_season(league_key: str, current_season: str) -> str:
    """Return the oldest season still inside the retention window."""
    return get_retained_seasons(league_key, current_season)[0]

