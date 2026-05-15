import re

with open('src/publish/lib/config_validation.py', 'r') as f:
    text = f.read()

text = re.sub(r'from src\.core\.lib\.config_validation import \(\s*validate_dict_config,\s*validate_flat_config,\s*validate_scalar_dict,\s*\)\n', '', text, flags=re.DOTALL)
text = re.sub(r'def _validate_columns.*?def _validate_destinations\(\) -> List\[str\]: return \[\]\n', '', text, flags=re.DOTALL)
text = re.sub(r'def validate_core_constants\(\) -> List\[str\]:\n    return \[\]\n', '', text)
text = re.sub(r'def validate_config\(league_key: str = None\) -> List\[str\]:\n    return \[\]\n', "def validate_config(league_key: str = None) -> List[str]:\n    # Cross-reference validations (which operate on actual config objects) should be hooked up here.\n    return []\n", text)
with open('src/publish/lib/config_validation.py', 'w') as f:
    f.write(text)
