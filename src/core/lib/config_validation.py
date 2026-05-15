"""
Config validation runtime shapes (now deprecated, statically typed with TypedDict)
"""

def validate_core_constants():
    from src.core.definitions.stats import STAT_DOMAINS
    errors = []
    primaries = [k for k, v in STAT_DOMAINS.items() if v.get("primary")]
    if len(primaries) != 1:
        errors.append(f"STAT_DOMAINS: expected exactly one entry with primary=True, got {primaries!r}")
    return errors
