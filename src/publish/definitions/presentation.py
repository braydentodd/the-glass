"""
The Glass - Presentation Configuration

Cross-destination presentation configuration: fonts, colors, alignment,
wrap strategy, visual assets, and column width classes used by the publish
layout and formatting engines. Destination-specific config (spreadsheet IDs,
OAuth scopes, frozen panes) lives in :mod:`src.publish.destinations.sheets.config`.
"""

from typing import Any, Dict, Optional


# ============================================================================
# PRESENTATION DEFAULTS
# ============================================================================

PRESENTATION_DEFAULTS: Dict[str, Any] = {
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
    'percentile_companion_width':     18,
    'percentile_companion_font_size': 5,
}


# ============================================================================
# COLOR PALETTE
# ============================================================================

COLORS: Dict[str, Dict[str, float]] = {
    'red':        {'red': 0.933, 'green': 0.294, 'blue': 0.169},
    'yellow':     {'red': 0.988, 'green': 0.961, 'blue': 0.373},
    'green':      {'red': 0.298, 'green': 0.733, 'blue': 0.090},
    'black':      {'red': 0,     'green': 0,     'blue': 0},
    'white':      {'red': 1,     'green': 1,     'blue': 1},
    'light_gray': {'red': 0.94,  'green': 0.94,  'blue': 0.94},
}


# ============================================================================
# PERCENTILE -> COLOR THRESHOLDS
# ============================================================================

COLOR_THRESHOLDS: Dict[str, int] = {
    'low':    0,    # 0% = pure red
    'mid':    50,   # 50% = pure yellow
    'high':   100,  # 100% = pure green
}


# ============================================================================
# COLUMN WIDTH CLASSES
# ============================================================================

WIDTH_CLASSES: Dict[str, Optional[int]] = {
    'auto':           None,
    'measurement':    34,
    'four_char':      31,
    'four_char_dec':  31,
    'three_char_dec': 24,
    'two_char_dec':   19,
    'two_char':       19,
}


# ============================================================================
# VALIDATION SCHEMAS
# ============================================================================

PRESENTATION_DEFAULTS_SCHEMA: Dict[str, Dict[str, Any]] = {
    'header_font':                    {'required': True, 'types': (str,)},
    'data_font':                      {'required': True, 'types': (str,)},
    'header_bg':                      {'required': True, 'types': (str,)},
    'header_fg':                      {'required': True, 'types': (str,)},
    'wrap_strategy':                  {'required': True, 'types': (str,)},
    'hide_advanced_columns':          {'required': True, 'types': (bool,)},
    'percentile_companion_width':     {'required': True, 'types': (int,)},
    'percentile_companion_font_size': {'required': True, 'types': (int,)},
}

COLORS_SCHEMA: Dict[str, Dict[str, Any]] = {
    'red':   {'required': True, 'types': (int, float)},
    'green': {'required': True, 'types': (int, float)},
    'blue':  {'required': True, 'types': (int, float)},
}

COLOR_THRESHOLDS_SCHEMA: Dict[str, Dict[str, Any]] = {
    'low':  {'required': True, 'types': (int, float)},
    'mid':  {'required': True, 'types': (int, float)},
    'high': {'required': True, 'types': (int, float)},
}
