import re

with open('src/etl/lib/config_validation.py', 'r') as f:
    text = f.read()

replacement = """def validate_config() -> List[str]:
    from src.core.definitions.columns import DB_COLUMNS
    from src.core.definitions.leagues import LEAGUES
    from src.core.definitions.tables import STATS_TABLES, ROSTER_TABLES, PROFILE_TABLES
    from src.etl.definitions.sources import SOURCES

    errors: List[str] = []
    
    errors.extend(_validate_pg_types(DB_COLUMNS))
    errors.extend(_validate_source_structure(DB_COLUMNS, SOURCES))
    errors.extend(_validate_stats_primary_keys(STATS_TABLES, DB_COLUMNS))
    errors.extend(_validate_league_source_roles(LEAGUES, SOURCES))
    errors.extend(_validate_legacy_source_fields(LEAGUES))
    errors.extend(_validate_legacy_pipeline_fields(LEAGUES))
    errors.extend(_validate_domain_coverage(DB_COLUMNS))
    errors.extend(_validate_fk_targets(STATS_TABLES, ROSTER_TABLES, PROFILE_TABLES))
    
    errors.extend(_validate_league_stage_definitions())
    errors.extend(_validate_entity_matcher_definitions())
    errors.extend(_validate_pipeline_structure())
    
    return errors
"""

text = re.sub(r'def validate_config\(\) -> List\[str\]:\n\s*return \[\]\n', replacement, text)

with open('src/etl/lib/config_validation.py', 'w') as f:
    f.write(text)
