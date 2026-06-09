"""
Shoot the Sheet - Google Sheets Destination Publisher

Takes pre-calculated, structured Intermediate Representation (IR) dictionaries
from the core Executor and translates them into Google Sheets batch API requests.
"""

import logging

from src.publish.destinations.google_sheets.request_builders import build_formatting_requests
from src.publish.destinations.google_sheets.client import get_or_create_worksheet, write_and_format

logger = logging.getLogger(__name__)


def publish_sheet(
    client,
    spreadsheet,
    sheet_name: str,
    ir_payload: dict,
    data_only: bool = False,
    show_advanced: bool = False,
) -> None:
    """
    Takes an agnostic IR payload and writes it to Google Sheets.
    
    IR format expected:
    {
        "columns_list": [...],
        "headers": {...},
        "data_rows": [...],
        "percentile_cells": [...],
        "n_player_rows": int,
        "sheet_type": str,
        "display_name": str
    }
    """
    logger.info('Publishing IR payload to sheet: %s', sheet_name)
    
    worksheet = get_or_create_worksheet(spreadsheet, sheet_name, clear=not data_only)
    
    write_and_format(
        worksheet=worksheet,
        columns_list=ir_payload['columns_list'],
        headers=ir_payload['headers'],
        data_rows=ir_payload['data_rows'],
        percentile_cells=ir_payload['percentile_cells'],
        n_player_rows=ir_payload['n_player_rows'],
        team_name=ir_payload['display_name'],
        sheet_type=ir_payload['sheet_type'],
        show_advanced=show_advanced,
        data_only=data_only,
        build_fn=build_formatting_requests,
    )
    
    logger.info('Successfully published %d main rows', ir_payload['n_player_rows'])
