"""
Shoot the Sheet - Global ETL Pipeline Policy

Phases are ordered lists of handler names.  The orchestrator dispatches each
handler directly; season/season-type scoping lives in the orchestrator, not
in declarative step metadata.
"""

from typing import Dict, List

VALID_ETL_PHASES = frozenset({'full'})

PIPELINE_PHASES: Dict[str, List[str]] = {
    'full': [
        'build_schema',
        'update_internal',
        'backfill_external',
        'maintain_external',
        'match_entities',
        'upsert_entities',
        'prune_stats_retention',
        'prune_entities',
        'prune_coverages',
    ],
}

VALID_ETL_STEP_HANDLERS = frozenset(
    handler for handlers in PIPELINE_PHASES.values() for handler in handlers
)

