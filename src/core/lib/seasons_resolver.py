"""
The Glass - Season Label Format Engine

Shape-driven rendering and parsing for season labels.  Supports both
league-canonical formats (a coarse ``same_year``/``split_year`` enum) and
arbitrary source wire formats (e.g. ``YYYY-YYYY`` for NCAA stats, ``YY/YY``
for an FIBA feed).

Two-digit year parsing uses a 1980 pivot: ``00..79`` resolves to ``2000..2079``,
``80..99`` to ``1980..1999``.  This window is plenty for sports data; revisit
if a source emits two-digit labels for seasons before 1980.
"""

from typing import Optional, Tuple


# ---------------------------------------------------------------------------
# Shape vocabulary
# ---------------------------------------------------------------------------

VALID_SHAPES = frozenset({
    'YYYY', 'YY',
    'YYYY-YY', 'YY-YY', 'YYYY-YYYY',
    'YYYY/YY', 'YY/YY', 'YYYY/YYYY',
})

VALID_ANCHORS = frozenset({'start', 'end', None})

VALID_LEAGUE_SEASON_FORMATS = frozenset({'same_year', 'split_year'})

_LEAGUE_FORMAT_TO_SHAPE: dict = {
    'same_year':  ('YYYY',    'end'),
    'split_year': ('YYYY-YY', None),
}

_TWO_DIGIT_PIVOT = 80


# ---------------------------------------------------------------------------
# Shape parsing
# ---------------------------------------------------------------------------

def _separator(shape: str) -> Optional[str]:
    if '-' in shape:
        return '-'
    if '/' in shape:
        return '/'
    return None


def _segments(shape: str) -> Tuple[str, ...]:
    sep = _separator(shape)
    return tuple(shape.split(sep)) if sep else (shape,)


def _validate_shape(shape: str) -> None:
    if shape not in VALID_SHAPES:
        raise ValueError(
            f"Invalid season shape {shape!r}; expected one of {sorted(VALID_SHAPES)}"
        )


def _resolve_two_digit_year(two_digit: int) -> int:
    """Map a 0..99 short year to a four-digit year via the 1980 pivot."""
    return 2000 + two_digit if two_digit < _TWO_DIGIT_PIVOT else 1900 + two_digit


def _format_year(year: int, digits: int) -> str:
    if digits == 4:
        return str(year)
    if digits == 2:
        return f'{year % 100:02d}'
    raise ValueError(f"Year segment must be 2 or 4 digits, got {digits}")


# ---------------------------------------------------------------------------
# Shape-level renderer / parser
# ---------------------------------------------------------------------------

def render_season_in_shape(
    end_year: int,
    shape: str,
    anchor: Optional[str] = None,
) -> str:
    """Render an integer ``end_year`` (e.g. ``2026``) into a season label.

    For single-segment shapes (``YYYY`` / ``YY``), ``anchor`` selects which
    side of the season the digits represent: ``'start'`` -> start year,
    ``'end'`` -> end year.

    For two-segment shapes, segments are always chronological (start -> end)
    and ``anchor`` is ignored.
    """
    _validate_shape(shape)
    if anchor not in VALID_ANCHORS:
        raise ValueError(f"Invalid anchor {anchor!r}; expected one of {sorted(VALID_ANCHORS)}")

    sep = _separator(shape)
    if sep is None:
        if anchor not in ('start', 'end'):
            raise ValueError(
                f"Single-year shape {shape!r} requires anchor='start' or 'end'"
            )
        year = end_year if anchor == 'end' else end_year - 1
        return _format_year(year, len(shape))

    left_seg, right_seg = _segments(shape)
    return (
        f'{_format_year(end_year - 1, len(left_seg))}'
        f'{sep}'
        f'{_format_year(end_year, len(right_seg))}'
    )


def parse_season_in_shape(
    label: str,
    shape: str,
    anchor: Optional[str] = None,
) -> int:
    """Inverse of :func:`render_season_in_shape`; returns the end_year integer."""
    _validate_shape(shape)
    if anchor not in VALID_ANCHORS:
        raise ValueError(f"Invalid anchor {anchor!r}; expected one of {sorted(VALID_ANCHORS)}")

    sep = _separator(shape)
    if sep is None:
        if anchor not in ('start', 'end'):
            raise ValueError(
                f"Single-year shape {shape!r} requires anchor='start' or 'end'"
            )
        digits = len(shape)
        if digits == 4:
            year = int(label)
        elif digits == 2:
            year = _resolve_two_digit_year(int(label))
        else:
            raise ValueError(f"Single-year shape {shape!r} has unsupported digit count")
        return year if anchor == 'end' else year + 1

    left_seg, right_seg = _segments(shape)
    left_label, right_label = label.split(sep)

    # End year is preferred when the right segment is unambiguous (4-digit).
    if len(right_seg) == 4:
        return int(right_label)

    # Two-digit right segment: use left segment to disambiguate the century.
    end_two = int(right_label)
    if len(left_seg) == 4:
        start = int(left_label)
        return start + 1
    return _resolve_two_digit_year(end_two)


# ---------------------------------------------------------------------------
# League-level convenience (same_year / split_year enum)
# ---------------------------------------------------------------------------

def format_season_label(end_year: int, league_season_format: str) -> str:
    """Render a season label using a league's coarse format enum.

    ``'split_year'`` -> e.g. ``2026 -> '2025-26'`` (NBA, NCAA basketball).
    ``'same_year'``  -> e.g. ``2026 -> '2026'`` (single-calendar-year leagues).
    """
    if league_season_format not in _LEAGUE_FORMAT_TO_SHAPE:
        raise ValueError(
            f"Unsupported league season_format {league_season_format!r}; "
            f"expected one of {sorted(VALID_LEAGUE_SEASON_FORMATS)}"
        )
    shape, anchor = _LEAGUE_FORMAT_TO_SHAPE[league_season_format]
    return render_season_in_shape(end_year, shape, anchor)


def parse_season_end_year(label: str, league_season_format: str) -> int:
    """Inverse of :func:`format_season_label`."""
    if league_season_format not in _LEAGUE_FORMAT_TO_SHAPE:
        raise ValueError(
            f"Unsupported league season_format {league_season_format!r}; "
            f"expected one of {sorted(VALID_LEAGUE_SEASON_FORMATS)}"
        )
    shape, anchor = _LEAGUE_FORMAT_TO_SHAPE[league_season_format]
    return parse_season_in_shape(label, shape, anchor)
