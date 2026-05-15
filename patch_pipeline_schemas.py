import re

with open('src/etl/definitions/pipeline.py', 'r') as f:
    text = f.read()

text = re.sub(r'PIPELINE_STEP_SCHEMA:\s*Dict\[str, Dict\[str, Any\]\] = \{.*?\}\n*', '', text, flags=re.DOTALL)

with open('src/etl/definitions/pipeline.py', 'w') as f:
    f.write(text)
