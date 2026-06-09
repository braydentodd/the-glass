"""
Shoot the Sheet - Sheets Source Configuration

Pure data definitions for the ``shoot_the_sheet`` source: metadata for
syncing user-edited values from Google Sheets back to profile tables.

Unlike API sources (nba_api, pbp_stats), this is a ``writer`` source that
doesn't hold source-id columns -- it edits canonical profile data via
sts_id anchor column.
"""

from typing import TypedDict


class SourceConfigDef(TypedDict):
    source_key: str
    sts_id_column_key: str


SOURCE_CONFIG: SourceConfigDef = {
    'source_key': 'shoot_the_sheet',
    'sts_id_column_key': 'sts_id',
}
