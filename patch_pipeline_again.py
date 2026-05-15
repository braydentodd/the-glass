import re

with open('src/etl/definitions/pipeline.py', 'r') as f:
    text = f.read()

text = re.sub(r',\n\s*\'season_window\':.*?\}', '', text, flags=re.DOTALL)

with open('src/etl/definitions/pipeline.py', 'w') as f:
    f.write(text)
