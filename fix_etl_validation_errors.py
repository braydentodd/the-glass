import re

with open('src/etl/lib/config_validation.py', 'r') as f:
    text = f.read()

# Remove the empty if block:
text = re.sub(r'\n\s*if source_key == \'nba_api\':\n\s*\n', '\n', text)

# Remove the leftover validate_entry line:
text = re.sub(r'\s*errors\.extend\(validate_entry\(step,\s*prefix\)\)', '', text)

with open('src/etl/lib/config_validation.py', 'w') as f:
    f.write(text)
