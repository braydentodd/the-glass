"""
The Glass - Tabular Renderer Defaults

Cross-destination presentation knobs: row-class formatting matrix and
sheet-wide defaults that any tabular renderer (Google Sheets, Excel,
HTML, PDF) can consume.  Sheets-API-specific config (spreadsheet IDs,
OAuth scopes) lives in :mod:`src.publish.destinations.sheets.config`.
"""

from typing import Any, Dict


# ============================================================================
# SHEET-WIDE FORMATTING DEFAULTS
# ============================================================================

SHEET_FORMATTING: Dict[str, Any] = {
    'header_font':                    'Staatliches',
    'data_font':                      'Sofia Sans',
    'header_bg':                      'black',
    'header_fg':                      'white',
    'data_row_even_bg':               'white',
    'data_row_odd_bg':                'light_gray',
    'data_fg':                        'black',
    'horizontal_align':               'CENTER',
    'vertical_align':                 'MIDDLE',
    'wrap_strategy':                  'CLIP',
    'hide_advanced_columns':          True,
    'frozen_columns':                 1,
    'frozen_rows':                    6,
    'header_rows':                    6,
    'percentile_companion_width':     18,
    'percentile_companion_font_size': 5,
    'sync_delay_seconds':             0,
}


# ============================================================================
# HEADER ROW CLASS FORMATTING
# ============================================================================

HEADER_ROWS: Dict[str, Dict[str, Any]] = {
    'sections': {
        'row_height':                     25,
        'font_size':                      12,
        'description_spacer_count':       None,
        'divider_row_weight':             4,
        'divider_row_direction':          'below',
        'divider_column_weight':          4,
        'divider_column_direction':       'right',
        'column_a_font_size':             15,
        'column_a_divider_column_weight': None,
    },
    'subsections': {
        'row_height':                     21,
        'font_size':                      11,
        'description_spacer_count':       None,
        'divider_row_weight':             2,
        'divider_row_direction':          'below',
        'divider_column_weight':          2,
        'divider_column_direction':       'right',
        'column_a_font_size':             11,
        'column_a_divider_column_weight': None,
    },
    'columns': {
        'row_height':                     21,
        'font_size':                      10,
        'description_spacer_count':       1500,
        'divider_row_weight':             None,
        'divider_row_direction':          None,
        'divider_column_weight':          1,
        'divider_column_direction':       'right',
        'column_a_font_size':             10,
        'column_a_divider_column_weight': None,
    },
    'filters': {
        'row_height':                     12,
        'font_size':                      10,
        'description_spacer_count':       None,
        'divider_row_weight':             None,
        'divider_row_direction':          None,
        'divider_column_weight':          1,
        'divider_column_direction':       'right',
        'column_a_font_size':             10,
        'column_a_divider_column_weight': None,
    },
}


# ============================================================================
# VALIDATION SCHEMAS
# ============================================================================

SHEET_FORMATTING_SCHEMA: Dict[str, Dict[str, Any]] = {
    'header_font':                    {'required': True, 'types': (str,)},
    'data_font':                      {'required': True, 'types': (str,)},
    'header_bg':                      {'required': True, 'types': (str,)},
    'header_fg':                      {'required': True, 'types': (str,)},
    'wrap_strategy':                  {'required': True, 'types': (str,)},
    'hide_advanced_columns':          {'required': True, 'types': (bool,)},
    'percentile_companion_width':     {'required': True, 'types': (int,)},
    'percentile_companion_font_size': {'required': True, 'types': (int,)},
    'frozen_rows':                    {'required': True, 'types': (int,)},
    'frozen_columns':                 {'required': True, 'types': (int,)},
    'sync_delay_seconds':             {'required': True, 'types': (int,)},
}
