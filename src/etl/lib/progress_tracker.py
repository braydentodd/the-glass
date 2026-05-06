"""
The Glass - ETL Progress Tracking

Database operations for tracking ETL run state and per-group progress.
Uses the unified ``runs`` and ``tasks`` tables (pipeline = 'etl').

Each task row uses an ``item_key`` that encodes the work unit as:
  ``{entity_type}:{dataset}:{tier}:{comma_sorted_columns}``

Supports auto-resume: if a run was interrupted mid-flight, the runner
can detect the orphaned 'running' record and resume from the last
pending group.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_PIPELINE = 'etl'


def _make_item_key(entity_type: str, dataset: str, tier: str, columns: Dict) -> str:
    """Encode a call-group work unit as a stable, opaque string key."""
    col_part = ','.join(sorted(columns.keys())) if columns else ''
    return f'{entity_type}:{dataset}:{tier}:{col_part}'


# ============================================================================
# RUN LIFECYCLE
# ============================================================================

def create_run(
    conn: Any,
    db_schema: str,
    season: str,
    season_type: str,
    entity_type: str,
    total_groups: int,
) -> int:
    """Insert a new runs record for the ETL pipeline and return the run id."""
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {db_schema}.runs "
            f"(pipeline, season, season_type, entity_type, total_items) "
            f"VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (_PIPELINE, season, season_type, entity_type, total_groups),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    logger.info('Created ETL run %d', run_id)
    return run_id


def complete_run(conn: Any, db_schema: str, run_id: int, total_rows: int) -> None:
    """Mark a run as completed."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.runs "
            f"SET status = 'completed', completed_at = NOW(), total_rows = %s "
            f"WHERE id = %s AND pipeline = %s",
            (total_rows, run_id, _PIPELINE),
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
# GROUP TASKS
# ============================================================================

def register_groups(
    conn: Any,
    db_schema: str,
    run_id: int,
    groups: List[Dict[str, Any]],
    entity_type: str,
) -> List[int]:
    """Insert task rows for each call group. Returns task ids."""
    task_ids: List[int] = []
    with conn.cursor() as cur:
        for group in groups:
            item_key = _make_item_key(
                entity_type, group['dataset'], group['tier'],
                group.get('columns', {}),
            )
            cur.execute(
                f"INSERT INTO {db_schema}.tasks "
                f"(run_id, pipeline, item_key, entity_type) "
                f"VALUES (%s, %s, %s, %s) RETURNING id",
                (run_id, _PIPELINE, item_key, entity_type),
            )
            task_ids.append(cur.fetchone()[0])
    conn.commit()
    return task_ids


def mark_group_started(conn: Any, db_schema: str, task_id: int) -> None:
    """Mark a task entry as running."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.tasks "
            f"SET status = 'running', started_at = NOW() "
            f"WHERE id = %s",
            (task_id,),
        )
    conn.commit()


def mark_group_completed(
    conn: Any, db_schema: str, task_id: int, rows_written: int,
) -> None:
    """Mark a task entry as completed."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {db_schema}.tasks "
            f"SET status = 'completed', completed_at = NOW(), rows_written = %s "
            f"WHERE id = %s",
            (rows_written, task_id),
        )
    conn.commit()


def mark_group_failed(
    conn: Any, db_schema: str, task_id: int, error_message: str,
) -> None:
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
    season: str,
    season_type: str,
    entity_type: str,
) -> Optional[int]:
    """Find an interrupted ETL run matching the given parameters.

    Returns the run_id if a 'running' record exists, else None.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id FROM {db_schema}.runs "
            f"WHERE pipeline = %s AND status = 'running' "
            f"AND season = %s AND season_type = %s AND entity_type = %s "
            f"ORDER BY started_at DESC LIMIT 1",
            (_PIPELINE, season, season_type, entity_type),
        )
        row = cur.fetchone()
    return row[0] if row else None


def get_pending_task_ids(
    conn: Any,
    db_schema: str,
    run_id: int,
) -> List[Tuple[int, str]]:
    """Return (task_id, item_key) for incomplete groups.

    Returns groups with status 'pending' or 'running' (interrupted).
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id, item_key FROM {db_schema}.tasks "
            f"WHERE run_id = %s AND pipeline = %s AND status IN ('pending', 'running') "
            f"ORDER BY id",
            (run_id, _PIPELINE),
        )
        return cur.fetchall()


def update_run_completed_groups(
    conn: Any, db_schema: str, run_id: int,
) -> None:
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
    entity: str,
    season: str,
    season_type: str,
    groups: List[Dict[str, Any]],
    auto_resume: bool,
) -> Tuple[int, List[Tuple[Dict[str, Any], int]]]:
    """Determine the run_id and pending work items for an entity/season.

    If *auto_resume* is enabled and an interrupted run exists for the same
    (season, season_type, entity), resumes from the last pending group.
    Otherwise creates a fresh run and registers all groups.

    Returns (run_id, [(group_dict, task_id), ...]).
    """
    if auto_resume:
        run_id = find_resumable_run(conn, db_schema, season, season_type, entity)
        if run_id:
            logger.info('Resuming interrupted ETL run %d for %s', run_id, entity)
            pending = get_pending_task_ids(conn, db_schema, run_id)
            pending_by_key = {item_key: tid for tid, item_key in pending}
            work_items: List[Tuple[Dict[str, Any], int]] = []
            for group in groups:
                key = _make_item_key(entity, group['dataset'], group['tier'],
                                     group.get('columns', {}))
                if key in pending_by_key:
                    work_items.append((group, pending_by_key[key]))
            logger.info('Resuming with %d pending groups', len(work_items))
            return run_id, work_items

    run_id = create_run(
        conn, db_schema, season, season_type, entity, len(groups),
    )
    task_ids = register_groups(conn, db_schema, run_id, groups, entity)
    return run_id, list(zip(groups, task_ids))
