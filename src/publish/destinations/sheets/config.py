"""
The Glass - Google Sheets Destination Credentials

Sheets-API-only configuration: per-league spreadsheet identifiers, OAuth
scopes, and credentials file path.  Cross-destination presentation knobs
(`SHEET_FORMATTING`, `HEADER_ROWS`) live in :mod:`src.publish.definitions.sheets`
because they're tabular-renderer abstractions, not Sheets-specific.
"""

import os
from typing import Any, Dict


GOOGLE_SHEETS_CONFIG_SCHEMA: Dict[str, Dict[str, Any]] = {
    'credentials_file': {'required': True, 'types': (str, type(None))},
    'spreadsheet_id':   {'required': True, 'types': (str, type(None))},
    'scopes':           {'required': True, 'types': (list,)},
}

GOOGLE_SHEETS_CONFIG: Dict[str, Dict[str, Any]] = {
    'nba': {
        'credentials_file': 'google-credentials.json',
        'spreadsheet_id':   os.getenv('NBA_SPREADSHEET_ID'),
        'scopes': [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive',
        ],
    },
    'ncaa': {
        'credentials_file': 'google-credentials.json',
        'spreadsheet_id':   os.getenv('NCAA_SPREADSHEET_ID'),
        'scopes': [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive',
        ],
    },
}
