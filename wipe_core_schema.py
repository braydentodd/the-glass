import re

with open('src/core/lib/config_validation.py', 'w') as f:
    f.write('"""\nConfig validation runtime shapes (now deprecated, statically typed with TypedDict)\n"""\n\n')
    f.write('def validate_core_constants():\n')
    f.write('    from src.core.definitions.stats import STAT_DOMAINS\n')
    f.write('    errors = []\n')
    f.write('    primaries = [k for k, v in STAT_DOMAINS.items() if v.get("primary")]\n')
    f.write('    if len(primaries) != 1:\n')
    f.write('        errors.append(f"STAT_DOMAINS: expected exactly one entry with primary=True, got {primaries!r}")\n')
    f.write('    return errors\n')
