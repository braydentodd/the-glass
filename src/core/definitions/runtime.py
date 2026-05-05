"""
The Glass - Unified Runtime Settings

Operational knobs shared by ETL and Publish.  Each entry carries a value
per pipeline domain so cross-domain divergence is visible at a glance:
each row of the table reads "this knob is X for ETL and Y for Publish."

If a future knob applies to only one domain, set the inapplicable side to
``None`` rather than splitting the table across files.
"""

from typing import Any, Dict


RUNTIME_CONFIG_SCHEMA: Dict[str, Dict[str, Any]] = {
    'auto_resume':         {'required': True, 'types': (dict,)},
    'max_retry_attempts':  {'required': True, 'types': (dict,)},
    'retry_delay_seconds': {'required': True, 'types': (dict,)},
}

RUNTIME_DOMAIN_SCHEMA: Dict[str, Dict[str, Any]] = {
    'etl':     {'required': True, 'types': (bool, int, float, type(None))},
    'publish': {'required': True, 'types': (bool, int, float, type(None))},
}

RUNTIME_CONFIG: Dict[str, Dict[str, Any]] = {
    'auto_resume':         {'etl': True, 'publish': True},
    'max_retry_attempts':  {'etl': 3,    'publish': 3},
    'retry_delay_seconds': {'etl': 60,   'publish': 30},
}
