import re

with open('src/etl/orchestrator.py', 'r') as f:
    text = f.read()

text = re.sub(r'raw_stage_frequencies =\.get\(handler\)\n?', '', text)

with open('src/etl/orchestrator.py', 'w') as f:
    f.write(text)
