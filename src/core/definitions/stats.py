"""
Shoot the Sheet - Stat Domain & Season-Type Definitions

Cross-cutting stats constants used by both ETL cleanup and Publish scaling.

A *domain* groups counter columns that share a common minutes/games denominator
and a common 0-vs-NULL coherency rule.  Cleanup uses ``minutes_col`` to decide
whether to NULL out a domain's stats; publish uses ``minutes_col`` for
per-minute / per-possession rates.

A domain is "primary" when its denominator covers the entire stats row
(base minutes).  Non-primary domains apply only to the subset of stats
they cover, so cleanup and publish must use the domain's own denominator
instead of the row-level one.
"""

from typing import Dict

STAT_RATES: Dict[str, dict] = {
    "per_poss": {"short_label": "Poss", "rate": 100},
    "per_min": {"short_label": "Min", "rate": 40},
}


# ----------------------------------------------------------------------------
# Stat Domains
# ----------------------------------------------------------------------------
# Each domain groups columns that share a minutes/games denominator.
# ``primary`` domains use the row-level base minutes (mins_x10).
# Non-primary domains (e.g. tracking stats) have their own denominator column.
#
# The ``minutes_col`` is the column (stored in DB as *_x10 or raw) that
# serves as the denominator for the domain's columns.  Cleanup uses it for
# NULL/0 coherency; publish uses it for per-minute/per-possession scaling.
# ----------------------------------------------------------------------------

STAT_DOMAINS: Dict[str, dict] = {
    "base": {
        "primary": True,
        "minutes_col": "mins_x10",
    },
}


# ----------------------------------------------------------------------------
# Season Type Groups
# ----------------------------------------------------------------------------
# Maps consolidated groups to the canonical season type keys that belong
# to each group.  Used by publish queries to aggregate across all types
# within a group (e.g. regular_season + showcase_cup -> regular_season group).
#
# The canonical keys here match those declared in each source's SEASON_TYPES
# config (e.g. nba_api/config.py, pbp_stats/config.py).
# ----------------------------------------------------------------------------

SEASON_TYPE_GROUPS: Dict[str, list[str]] = {
    "regular_season": ["regular_season", "showcase_cup"],
    "postseason": ["playoffs", "play_in"],
}
