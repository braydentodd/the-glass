# ruff: noqa: E402  -- load_dotenv() must run before src.* imports that read os.getenv at module load.
"""
The Glass - ETL CLI

Single command-line entry point for the ETL pipeline.  Responsibilities,
in order:

    1. Parse arguments.
    2. Load .env, configure logging.
    3. Run config validation (fatal on error).
    4. Hand off to :func:`src.etl.orchestrator.run_etl`.

Anything that touches HTTP, the database, or per-phase logic lives in
:mod:`src.etl.orchestrator` (orchestration) or :mod:`src.etl.lib` (workers).

Usage:
    python -m src.etl.cli --league nba                         # full run
    python -m src.etl.cli --league nba --season 2023-24        # specific season
    python -m src.etl.cli --league nba --season-type po        # Playoffs
    python -m src.etl.cli --league nba --entity team
    python -m src.etl.cli --league nba --phase rosters
    python -m src.etl.cli --phase orphan                       # cross-league

Phases (per-league unless noted):
    full        discover -> rosters -> backfill -> update -> cleanup -> prune
    discover    populate core profiles for the current season
    rosters     resync core.league_rosters and core.team_rosters
    backfill    fetch stats for every season in the retention window
    update      refresh stats for the current season only
    prune       per-league retention (delete stats older than window)
    orphan      cross-league orphan profile sweep (no league_key needed)
"""

from dotenv import load_dotenv
load_dotenv()

import logging
import sys

from src.core.cli import (
    HelpFormatter,
    make_base_parser,
    print_banner,
    print_summary,
    style,
)
from src.core.logging import setup_logging
from src.etl.config_validation import validate_all
from src.etl.definitions import LEAGUES
from src.etl.orchestrator import VALID_PHASES, run_etl

logger = logging.getLogger(__name__)


def _build_parser():
    parser = make_base_parser(
        prog='python -m src.etl.cli',
        description='The Glass -- ETL pipeline (extract -> transform -> load).',
        epilog=(
            "Phases:\n"
            "  full        full league run\n"
            "  discover    profile rows for the current season\n"
            "  rosters     league/team and team/player junctions\n"
            "  backfill    stats for every retained season\n"
            "  update      stats for the current season only\n"
            "  prune       per-league retention pruning\n"
            "  orphan      cross-league orphan profile sweep"
        ),
    )
    parser.formatter_class = HelpFormatter

    parser.add_argument(
        '--league', type=str, default=None, choices=sorted(LEAGUES),
        help='League key. Required for every phase except "orphan".',
    )
    parser.add_argument(
        '--phase', type=str, default='full', choices=sorted(VALID_PHASES),
        help='ETL phase to run.',
    )
    parser.add_argument(
        '--season', type=str, default=None,
        help='Season label (e.g. 2024-25). Defaults to the league\'s current season.',
    )
    parser.add_argument(
        '--season-type', type=str, default='rs', choices=['rs', 'po', 'pi'],
        help='Season type code.',
    )
    parser.add_argument(
        '--entity', type=str, default='all', choices=['player', 'team', 'all'],
        help='Limit to one entity type.',
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.no_color:
        style.disable()

    setup_logging(verbose=args.verbose, quiet=args.quiet)

    print_banner('The Glass -- ETL', f'phase={args.phase}  league={args.league or "<n/a>"}')
    print_summary(
        {
            'phase':       args.phase,
            'league':      args.league or '(orphan / cross-league)',
            'season':      args.season or '(current)',
            'season_type': args.season_type,
            'entity':      args.entity,
        },
        title='Run parameters',
    )

    try:
        validate_all()
    except RuntimeError as exc:
        logger.error('Config validation failed: %s', exc)
        return 2

    try:
        run_etl(
            league_key=args.league,
            phase=args.phase,
            entity=args.entity,
            season=args.season,
            season_type=args.season_type,
        )
    except KeyboardInterrupt:
        logger.warning('Interrupted by user.')
        return 130
    except Exception:
        logger.exception('ETL run failed.')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
