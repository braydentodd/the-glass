"""
The Glass - Source Resolvers

Pure resolvers over :data:`src.core.definitions.sources.SOURCES` and the
per-league primary-source assignment in
:data:`src.core.definitions.leagues.LEAGUES`.

Source season-label rendering / parsing is delegated to
:mod:`src.core.lib.seasons`, which drives both league-canonical and
source-wire formats off the same shape engine.
"""

from typing import List, Tuple

from src.core.definitions.leagues import LEAGUES
from src.core.definitions.sources import SOURCES
from src.core.lib.seasons import parse_season_in_shape, render_season_in_shape


def get_primary_source(league_key: str) -> str:
    """Return the registry key of the primary source assigned to ``league_key``.

    The primary source is the first source run for a league.  It owns the
    external IDs (e.g. NBA Stats PERSON_ID) and is responsible for defining
    the current team rosters.

    Raises:
        ValueError: if the league has no primary_source configured, the
                    key is not registered, or the registered source is not an
                    authoritative role.
    """
    if league_key not in LEAGUES:
        raise ValueError(f"Unknown league: {league_key!r}")

    source_key = LEAGUES[league_key].get('primary_source')
    if not source_key:
        raise ValueError(f"League {league_key!r} has no primary_source configured")

    if source_key not in SOURCES:
        raise ValueError(
            f"League {league_key!r} primary_source {source_key!r} is not in SOURCES"
        )
    if SOURCES[source_key]['role'] != 'authoritative':
        raise ValueError(
            f"Source {source_key!r} configured as primary_source for "
            f"{league_key!r} has role={SOURCES[source_key]['role']!r}"
        )
    return source_key


def get_editable_sources(league_key: str) -> List[str]:
    """Return all editable sources that operate on ``league_key``."""
    return [
        key for key, meta in SOURCES.items()
        if meta['role'] == 'editable' and league_key in meta['leagues']
    ]


def get_source_id_column(source_key: str) -> str:
    """Return the per-source identity column name for profile tables.

    Convention: ``{source_key}_id`` (e.g. ``nba_api_id``).
    """
    if source_key not in SOURCES:
        raise ValueError(f"Unknown source: {source_key!r}")
    return f'{source_key}_id'


def get_source_id_columns_for_entity(entity: str) -> List[Tuple[str, str]]:
    """Source-id columns to add to ``entity``'s profile table, in stable order.

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


def render_season_for_source(end_year: int, source_key: str) -> str:
    """Render an end_year integer in the wire format expected by ``source_key``."""
    fmt = _require_source_season_format(source_key)
    return render_season_in_shape(end_year, fmt['shape'], fmt.get('anchor'))


def parse_source_season(label: str, source_key: str) -> int:
    """Inverse of :func:`render_season_for_source`; recovers the end_year."""
    fmt = _require_source_season_format(source_key)
    return parse_season_in_shape(label, fmt['shape'], fmt.get('anchor'))


def _require_source_season_format(source_key: str) -> dict:
    if source_key not in SOURCES:
        raise ValueError(f"Unknown source: {source_key!r}")
    fmt = SOURCES[source_key].get('season_format')
    if not fmt:
        raise ValueError(
            f"Source {source_key!r} has no season_format; cannot render or "
            f"parse season labels for it"
        )
    return fmt
