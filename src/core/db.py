"""
The Glass - Shared Database Module

Lightweight, dependency-free database primitives used everywhere in the
codebase:

    get_db_connection / db_connection   - connection acquisition
    quote_col                           - identifier quoting
    get_current_season_year             - calendar-agnostic end-year helper
    get_current_season                  - same, formatted as 'YYYY-YY'

Schema-aware table-name resolution and per-league season helpers live in
``src.etl.definitions`` to keep this module free of higher-level concerns.

All schemas (``core``, per-league) share a single PostgreSQL instance.
Schema bootstrap is handled by :mod:`src.etl.core.ddl`.
"""
import logging
import os
from contextlib import contextmanager
from datetime import datetime

import psycopg2

from src.core.config import format_season_label

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------

def quote_col(col: str) -> str:
    """Quote a column name for PostgreSQL (handles digit-starting names like ``2fgm``)."""
    return f'"{col}"'


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def get_db_connection():
    """Create a new psycopg2 connection.

    Caller is responsible for closing.  Prefer :func:`db_connection` for
    short-lived operations.
    """
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is missing.")
    return psycopg2.connect(db_url)


@contextmanager
def db_connection():
    """Context-managed DB connection that commits on success, rolls back on
    exception, and always closes."""
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Static-cutoff season helpers (legacy - prefer the per-league variants in
# src.etl.definitions for new code).
# ---------------------------------------------------------------------------

_DEFAULT_FLIP_MONTH = 7  # July 1st - aligns with NBA/NCAA carry-over


def get_current_season_year() -> int:
    """End-year of the current season using a static July-1 cutoff.

    Retained for callers that have no league context (e.g. the publish
    layer's date-stamp formatting).  League-specific code should use
    :func:`src.etl.definitions.get_current_season_year_for_league` instead.
    """
    now = datetime.now()
    return now.year + 1 if now.month >= _DEFAULT_FLIP_MONTH else now.year


def get_current_season() -> str:
    """Current season label (``'YYYY-YY'``) using the static July-1 cutoff."""
    return format_season_label(get_current_season_year())
