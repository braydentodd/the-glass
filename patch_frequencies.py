import re

def clean_file(path):
    with open(path, 'r') as f:
        text = f.read()

    # Remove the import line containing HANDLER_UPDATE_FREQUENCY_FILTERS
    text = re.sub(r'\s*HANDLER_UPDATE_FREQUENCY_FILTERS,?', '', text)

    # Remove the kwarg declaration everywhere it occurs
    # e.g.: update_frequencies: Union[Set[Optional[str]], None] = None,
    text = re.sub(r'\s*update_frequencies:\s*Union\[Set\[Optional\[str\]\],\s*None\]\s*=\s*None,?', '', text)

    # Remove the kwarg passed in function calls
    # e.g.: update_frequencies=update_frequencies,
    text = re.sub(r'\s*update_frequencies=update_frequencies,?', '', text)
    
    # e.g.: update_frequencies=stage_update_frequencies,
    text = re.sub(r'\s*update_frequencies=stage_update_frequencies,?', '', text)

    # Remove the logic calculating stage_update_frequencies and raw_stage_frequencies
    text = re.sub(r'\s*raw_stage_frequencies\s*=\s*HANDLER_UPDATE_FREQUENCY_FILTERS\.get\(handler\)', '', text)
    text = re.sub(r'\s*stage_update_frequencies:\s*Union\[Set\[Optional\[str\]\],\s*None\]\s*=\s*\(\s*set\(raw_stage_frequencies\)\s*if\s*raw_stage_frequencies\s*is\s*not\s*None\s*else\s*None\s*\)', '', text)

    with open(path, 'w') as f:
        f.write(text)

clean_file('src/etl/orchestrator.py')
clean_file('src/etl/lib/plan.py')
