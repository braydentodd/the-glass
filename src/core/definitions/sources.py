"""
The Glass - Source Registry

Declarative registry of every external data source.  Roles describe each
source's relationship to the data model, *not* the I/O direction:

  - ``authoritative``: owns its own external IDs (e.g. NBA Stats PERSON_ID)
                       and is the canonical origin of stats data.
  - ``editable``:      no external IDs of its own; surfaces user-edited
                       overlays on canonical profiles via ``the_glass_id``.

``season_format`` describes the source's *wire* format -- how it expects /
emits season labels in API requests and responses.  ``shape`` is one of
:data:`src.core.lib.seasons.VALID_SHAPES`; ``anchor`` is required for
single-segment shapes (``YYYY`` / ``YY``) and ignored for two-segment shapes
(set to ``None``).

Helpers that resolve source assignments per league/entity live in
:mod:`src.core.lib.sources`.
"""

from typing import Any, Dict

from src.core.lib.seasons import VALID_ANCHORS, VALID_SHAPES


VALID_SOURCE_ROLES = frozenset({'authoritative', 'editable'})


SOURCE_SEASON_FORMAT_SCHEMA: Dict[str, Dict[str, Any]] = {
    'shape':  {'required': True, 'types': (str, type(None)), 'allowed_values': VALID_SHAPES | {None}},
    'anchor': {'required': True, 'types': (str, type(None)), 'allowed_values': VALID_ANCHORS},
}

SOURCES_SCHEMA: Dict[str, Dict[str, Any]] = {
    'leagues':        {'required': True, 'types': (list,)},
    'role':           {'required': True, 'types': (str,), 'allowed_values': VALID_SOURCE_ROLES},
    'entity_id_type': {'required': True, 'types': (str, type(None))},
    'applies_to':     {'required': True, 'types': (list,)},
    'season_format':  {'required': True, 'types': (dict, type(None))},
}

SOURCES: Dict[str, Dict[str, Any]] = {
    'nba_api': {
        'leagues':        ['nba'],
        'role':           'authoritative',
        'entity_id_type': 'BIGINT',
        'applies_to':     ['team', 'player'],
        'season_format':  {'shape': 'YYYY-YY', 'anchor': None},
    },
    'the_glass_sheets': {
        'leagues':        ['nba'],
        'role':           'editable',
        'entity_id_type': None,
        'applies_to':     ['team', 'player'],
        'season_format':  None,
    },
}
