import re

with open('src/etl/lib/config_validation.py', 'r') as f:
    text = f.read()

# Remove validate_entry usage in _validate_league_stage_definitions
text = re.sub(r'from src\.core\.lib\.config_validation import validate_entry\n', '', text)
text = re.sub(r'errors\.extend\(validate_entry\(\s*step,\s*PIPELINE_STEP_SCHEMA,\s*f"PIPELINE_STEPS\[\{\s*step_name\s*}\]"\s*\)\)', '', text, flags=re.DOTALL)

# Remove the whole _validate_nba_api function since it just validates shapes
text = re.sub(r'def _validate_nba_api\(cfg_mod\) -> List\[str\]:.*?(?=\n\n|\Z)', '', text, flags=re.DOTALL)
text = re.sub(r'aggregated\.extend\(_validate_nba_api\(cfg_mod\)\)', '', text)

with open('src/etl/lib/config_validation.py', 'w') as f:
    f.write(text)
