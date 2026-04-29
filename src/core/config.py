"""
The Glass - Core Configuration & Constants

Shared values, standards, and conventions used across both ETL and Publish.

Lives in ``src.core`` because both layers import from it; nothing here may
import from ``src.etl`` or ``src.publish`` (would create a cycle).
"""

from datetime import datetime
from typing import Any, Dict


# ============================================================================
# SEASON FORMATTING
# ============================================================================

def format_season_label(season_year: int) -> str:
    """Convert end-year integer to a descriptive season string.

    Example: ``2026 -> '2025-26'``.
    """
    return f"{season_year - 1}-{str(season_year)[2:]}"


# Static-cutoff season helpers.  Per-league variants that respect each
# league's calendar_flip_md live in ``src.etl.definitions``; these are kept
# here for callers that have no league context (e.g. the publish layer's
# date-stamp formatting and validation reports).
_DEFAULT_FLIP_MONTH = 7  # July 1st -- aligns with NBA/NCAA carry-over


def get_current_season_year() -> int:
    """End-year of the current season using a static July-1 cutoff."""
    now = datetime.now()
    return now.year + 1 if now.month >= _DEFAULT_FLIP_MONTH else now.year


def get_current_season() -> str:
    """Current season label (``'YYYY-YY'``) using the static July-1 cutoff."""
    return format_season_label(get_current_season_year())


# ============================================================================
# STAT DOMAINS
# ============================================================================
# Domains group counter columns that share a common minutes/games denominator
# and a common 0-vs-NULL coherency rule.  Cleanup uses ``minutes_col`` to
# decide whether to NULL out a domain's stats; publish uses ``games_col`` for
# per-game scaling and ``minutes_col`` for per-minute / per-possession rates.
#
# A domain is "primary" when its denominator covers the entire stats row
# (i.e. base minutes / games).  Non-primary domains only apply to the subset
# of stats they cover, so cleanup and publish must use the domain's own
# denominator instead of the row-level one.

STAT_DOMAINS: Dict[str, Dict[str, Any]] = {
    'base': {
        'minutes_col': 'minutes_x10',
        'games_col':   'games',
        'primary':     True,
    },
    'tracking': {
        'minutes_col': 'tracking_minutes_x10',
        'games_col':   'tracking_games',
        'primary':     False,
    },
    'hustle': {
        'minutes_col': 'hustle_minutes_x10',
        'games_col':   'hustle_games',
        'primary':     False,
    },
    'onoff': {
        'minutes_col': 'off_minutes_x10',
        'games_col':   'off_games',
        'primary':     False,
    },
}


# ============================================================================
# SEASON TYPE CLASSIFICATION
# Codes are grouped into "regular_season" vs "postseason"; queries use the
# groups to decide which season_type values to aggregate.
# ============================================================================

SEASON_TYPE_GROUPS: Dict[str, tuple] = {
    'regular_season': ('rs',),
    'postseason':     ('po', 'pi', 'ct'),
}

SEASON_TYPE_LABELS: Dict[str, str] = {
    'rs': 'Regular Season',
    'po': 'Playoffs',
    'pi': 'Play-In',
    'ct': 'Conference Tournament',
}


# ============================================================================
# CONFIG VALIDATION SCHEMAS  (consumed by src.core.config_validation)
# ============================================================================

CORE_CONFIG_SCHEMAS: Dict[str, Dict[str, Dict[str, Any]]] = {
    'SEASON_TYPE_GROUPS': {
        'regular_season': {'required': True, 'types': (tuple, list)},
        'postseason':     {'required': True, 'types': (tuple, list)},
    },
    'STAT_DOMAINS_ENTRY': {
        'minutes_col': {'required': True, 'types': (str,)},
        'games_col':   {'required': True, 'types': (str,)},
        'primary':     {'required': True, 'types': (bool,)},
    },
}

# Backwards-compat alias (older importers may still reference the old name).
CORE_CONFIG_SCHEMA = CORE_CONFIG_SCHEMAS
