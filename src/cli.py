# ruff: noqa: E402  -- load_dotenv() must run before src.* imports that read os.getenv at module load.
"""
The Glass - Unified CLI

Single entry point for all pipeline commands.  Dispatches to ETL or
publish via subcommands so that both share the same base flags
(--verbose, --quiet, --no-color) while retaining their own distinct
arguments.

Usage:
    python -m src.cli etl --league nba
    python -m src.cli etl --league nba --phase backfill --season 2023-24
    python -m src.cli etl --phase orphan
    python -m src.cli publish --league nba
    python -m src.cli publish --league nba --tab BOS --stat-rate per_minute
    python -m src.cli publish --league nba --export-config

Subcommand phases (etl):
    full        discover -> rosters -> backfill -> update -> cleanup -> prune
    discover    populate core profiles for the current season
    rosters     league/team and team/player junctions
    backfill    stats for every retained season
    update      stats for the current season only
    prune       per-league retention pruning
    orphan      cross-league orphan profile sweep (no --league required)
"""

from dotenv import load_dotenv
load_dotenv()

import logging
import sys

from src.core.lib.cli import (
    HelpFormatter,
    make_base_parser,
    print_banner,
    print_summary,
    style,
)
from src.core.lib.logging import setup_logging
from src.core.definitions.leagues import LEAGUES
from src.etl.orchestrator import VALID_PHASES, run_etl
from src.publish.definitions.stats import (
    DEFAULT_STAT_RATE,
    HISTORICAL_TIMEFRAMES,
    STAT_RATES,
)
from src.publish.lib.export_config import export_config
from src.publish.orchestrator import run_publish

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Subparser builders
# ---------------------------------------------------------------------------

def _add_etl_subparser(subparsers) -> None:
    p = subparsers.add_parser(
        'etl',
        help='ETL pipeline (extract -> transform -> load).',
        formatter_class=HelpFormatter,
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
    p.add_argument(
        '--league', type=str, default=None, choices=sorted(LEAGUES),
        help='League key. Required for every phase except "orphan".',
    )
    p.add_argument(
        '--phase', type=str, default='full', choices=sorted(VALID_PHASES),
        help='ETL phase to run.',
    )
    p.add_argument(
        '--season', type=str, default=None,
        help="Season label (e.g. 2024-25). Defaults to the league's current season.",
    )
    p.add_argument(
        '--season-type', type=str, default='rs', choices=['rs', 'po', 'pi'],
        help='Season type code.',
    )
    p.add_argument(
        '--entity', type=str, default='all', choices=['player', 'team', 'all'],
        help='Limit to one entity type.',
    )


def _add_publish_subparser(subparsers) -> None:
    p = subparsers.add_parser(
        'publish',
        help='Publish league data to Google Sheets.',
        formatter_class=HelpFormatter,
    )
    p.add_argument(
        '--league', choices=sorted({'nba', 'ncaa'}), required=True,
        help='League to sync.',
    )
    p.add_argument(
        '--tab', metavar='NAME', default=None,
        help='Sync this tab first (team abbr like "BOS", or "all_players" / "all_teams").',
    )
    p.add_argument(
        '--stat-rate', choices=sorted(STAT_RATES), default=DEFAULT_STAT_RATE,
        help='Default visible stat rate.',
    )
    p.add_argument(
        '--historical-timeframe', type=int,
        choices=sorted(HISTORICAL_TIMEFRAMES),
        default=min(HISTORICAL_TIMEFRAMES),
        help='Number of previous seasons to include in historical sections.',
    )
    p.add_argument(
        '--show-advanced', action='store_true',
        help='Render advanced stat columns visible by default.',
    )
    p.add_argument(
        '--data-only', action='store_true',
        help='Fast sync: skip structural formatting, only update data + colors.',
    )
    p.add_argument(
        '--export-config', action='store_true',
        help='Export the Apps Script config JS file and exit (no sheet sync).',
    )
    p.add_argument(
        '--skip-config-export', action='store_true',
        help='Skip config build/export and clasp push entirely.',
    )


# ---------------------------------------------------------------------------
# Top-level parser
# ---------------------------------------------------------------------------

def _build_parser():
    root = make_base_parser(
        prog='python -m src.cli',
        description='The Glass -- data pipeline CLI.',
    )
    root.formatter_class = HelpFormatter
    subparsers = root.add_subparsers(dest='pipeline', metavar='PIPELINE')
    subparsers.required = True
    _add_etl_subparser(subparsers)
    _add_publish_subparser(subparsers)
    return root


# ---------------------------------------------------------------------------
# Dispatch handlers
# ---------------------------------------------------------------------------

def _run_etl(args) -> int:
    from src.etl.config_validation import validate_all

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


def _run_publish(args) -> int:
    from src.publish.config_validation import validate_all

    league = args.league.lower()
    historical_config = {'mode': 'seasons', 'value': args.historical_timeframe}

    print_banner(
        'The Glass -- Publish',
        f'league={league}  stat_rate={args.stat_rate}  data_only={args.data_only}',
    )
    print_summary(
        {
            'league':             league,
            'stat_rate':          args.stat_rate,
            'priority_tab':       args.tab or '(none)',
            'historical_seasons': args.historical_timeframe,
            'data_only':          args.data_only,
            'show_advanced':      args.show_advanced,
            'export_config_only': args.export_config,
        },
        title='Run parameters',
    )

    try:
        validate_all()
    except RuntimeError as exc:
        logger.error('Config validation failed: %s', exc)
        return 2

    if args.export_config and not args.skip_config_export:
        path = export_config(league)
        logger.info('Config exported to %s', path)
        return 0

    try:
        run_publish(
            league=league,
            stat_rate=args.stat_rate,
            show_advanced=args.show_advanced,
            historical_config=historical_config,
            data_only=args.data_only,
            priority_tab=args.tab,
            config_export=not args.skip_config_export,
        )
    except KeyboardInterrupt:
        logger.warning('Interrupted by user.')
        return 130
    except Exception:
        logger.exception('Publish run failed.')
        return 1
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.no_color:
        style.disable()

    setup_logging(verbose=args.verbose, quiet=args.quiet)

    if args.pipeline == 'etl':
        return _run_etl(args)
    return _run_publish(args)


if __name__ == '__main__':
    sys.exit(main())
