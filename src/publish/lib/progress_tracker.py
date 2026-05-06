"""
The Glass - Publish Progress Tracking

Database operations for tracking publish run state and per-tab progress.
Uses the unified ``runs`` and ``tasks`` tables (pipeline = 'publish').

Each task row uses an ``item_key`` equal to the tab name.

Supports auto-resume: if a run was interrupted mid-flight, the runner
can detect the orphaned 'running' record and resume from the last
pending tab.
"""

import logging
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

_PIPELINE = 'publish'


# ============================================================================
# RUN LIFECYCLE
# ============================================================================


def create_run(
    conn: Any,
    db_schema: str,
    league: str,
    total_tabs: int,
) -> int:
    """Insert a new runs record for the publish pipeline and return the run id."""
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {db_schema}.runs "
            f"(pipeline, entity_type, total_items) "
            f"VALUES (%s, %s, %s) RETURNING id",
            (_PIPELINE, league, total_tabs),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    logger.info('Created publish run %d for %s', run_id, league)
    return run_id


def complete_run(conn: Any, db_schema: str, run_id: int) -> None:
    """Mark a run as completed."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.runs "
            f"SET status = 'completed', completed_at = NOW() "
            f"WHERE id = %s AND pipeline = %s",
            (run_id, _PIPELINE),
        )
    conn.commit()


def fail_run(conn: Any, db_schema: str, run_id: int, error_message: str) -> None:
    """Mark a run as failed."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.runs "
            f"SET status = 'failed', completed_at = NOW(), error_message = %s "
            f"WHERE id = %s AND pipeline = %s",
            (error_message, run_id, _PIPELINE),
        )
    conn.commit()


# ============================================================================
# TAB TASKS
# ============================================================================


def register_tabs(
    conn: Any,
    db_schema: str,
    run_id: int,
    tabs: List[str],
) -> List[int]:
    """Insert task rows for each tab. Returns task ids."""
    task_ids: List[int] = []
    with conn.cursor() as cur:
        for tab_name in tabs:
            cur.execute(
                f"INSERT INTO {db_schema}.tasks "
                f"(run_id, pipeline, item_key) "
                f"VALUES (%s, %s, %s) RETURNING id",
                (run_id, _PIPELINE, tab_name),
            )
            task_ids.append(cur.fetchone()[0])
    conn.commit()
    return task_ids


def mark_tab_started(conn: Any, db_schema: str, task_id: int) -> None:
    """Mark a task entry as running."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.tasks "
            f"SET status = 'running', started_at = NOW() "
            f"WHERE id = %s",
            (task_id,),
        )
    conn.commit()


def mark_tab_completed(conn: Any, db_schema: str, task_id: int) -> None:
    """Mark a task entry as completed."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.tasks "
            f"SET status = 'completed', completed_at = NOW() "
            f"WHERE id = %s",
            (task_id,),
        )
    conn.commit()


def mark_tab_failed(conn: Any, db_schema: str, task_id: int, error_message: str) -> None:
    """Mark a task entry as failed and increment retry count."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.tasks "
            f"SET status = 'failed', completed_at = NOW(), "
            f"error_message = %s, retry_count = retry_count + 1 "
            f"WHERE id = %s",
            (error_message, task_id),
        )
    conn.commit()


# ============================================================================
# AUTO-RESUME
# ============================================================================


def find_resumable_run(
    conn: Any,
    db_schema: str,
    league: str,
) -> Optional[int]:
    """Find an interrupted publish run for the given league.

    Returns the run_id if a 'running' record exists, else None.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id FROM {db_schema}.runs "
            f"WHERE pipeline = %s AND status = 'running' AND entity_type = %s "
            f"ORDER BY started_at DESC LIMIT 1",
            (_PIPELINE, league),
        )
        row = cur.fetchone()
    return row[0] if row else None


def get_pending_task_ids(
    conn: Any,
    db_schema: str,
    run_id: int,
) -> List[Tuple[int, str]]:
    """Return (task_id, tab_name) for incomplete tabs.

    Returns tabs with status 'pending' or 'running' (interrupted).
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id, item_key FROM {db_schema}.tasks "
            f"WHERE run_id = %s AND pipeline = %s AND status IN ('pending', 'running') "
            f"ORDER BY id",
            (run_id, _PIPELINE),
        )
        return cur.fetchall()


def update_run_completed_tabs(conn: Any, db_schema: str, run_id: int) -> None:
    """Sync the completed_items counter on the run record."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.runs SET completed_items = ("
            f"  SELECT COUNT(*) FROM {db_schema}.tasks "
            f"  WHERE run_id = %s AND pipeline = %s AND status = 'completed'"
            f") WHERE id = %s AND pipeline = %s",
            (run_id, _PIPELINE, run_id, _PIPELINE),
        )
    conn.commit()


# ============================================================================
# WORK RESOLUTION
# ============================================================================


def resolve_work(
    conn: Any,
    db_schema: str,
    league: str,
    tabs: List[str],
    auto_resume: bool,
) -> Tuple[int, List[Tuple[str, int]]]:
    """Determine the run_id and pending work items for a publish run.

    If *auto_resume* is enabled and an interrupted run exists for the league,
    resumes from the last pending tab. Otherwise creates a fresh run.

    Returns (run_id, [(tab_name, task_id), ...]).
    """
    if auto_resume:
        run_id = find_resumable_run(conn, db_schema, league)
        if run_id:
            logger.info('Resuming interrupted publish run %d for %s', run_id, league)
            pending = get_pending_task_ids(conn, db_schema, run_id)
            work_items = [(tab_name, tid) for tid, tab_name in pending]
            logger.info('Resuming with %d pending tabs', len(work_items))
            return run_id, work_items

    run_id = create_run(conn, db_schema, league, len(tabs))
    task_ids = register_tabs(conn, db_schema, run_id, tabs)
    return run_id, list(zip(tabs, task_ids))
