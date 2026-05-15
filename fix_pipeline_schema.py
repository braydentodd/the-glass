import re

with open('src/etl/definitions/pipeline.py', 'r') as f:
    text = f.read()

# Replace PIPELINE_STEP_SCHEMA carefully
start_str = "PIPELINE_STEP_SCHEMA: Dict[str, Dict[str, Any]] = {"
idx = text.find(start_str)
if idx != -1:
    end_idx = text.find("}", idx)
    # the schema contains nested dicts so the first } is not the end
    end_idx = text.find("}\n", idx)
    end_idx = text.find("}\n", end_idx + 1)
    # The block consists of 3 entries, so it closes with } after the last entry's }
    
    text = re.sub(r'PIPELINE_STEP_SCHEMA: Dict\[str, Dict\[str, Any\]\] = \{\n.*?\n\}\n', '', text, flags=re.DOTALL)

with open('src/etl/definitions/pipeline.py', 'w') as f:
    f.write(text)
