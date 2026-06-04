"""
The Glass - Stat Domain & Season-Type Definitions

Cross-cutting stats constants used by both ETL cleanup and Publish scaling.

A *domain* groups counter columns that share a common minutes/games denominator
and a common 0-vs-NULL coherency rule.  Cleanup uses ``minutes_col`` to decide
whether to NULL out a domain's stats; publish uses ``minutes_col`` for per-minute / per-possession rates.

A domain is "primary" when its denominator covers the entire stats row (base minutes).  Non-primary domains apply only to the subset of stats
they cover, so cleanup and publish must use the domain's own denominator
instead of the row-level one.
"""

from typing import Dict

STAT_RATES = {
    'per_poss': {
        'short_label': 'Poss',
        'rate':        100
    },
    'per_min': {
        'short_label': 'Min',
        'rate':        40
    },
}


# Codes are grouped into "regular_season" vs "postseason"; queries use the
# groups to decide which season_type values to aggregate.
SEASON_TYPE_GROUPS: Dict[str, list[str]] = {
    'regular_season': ['rs'],
    'postseason':     ['po', 'pi', 'ct']
}