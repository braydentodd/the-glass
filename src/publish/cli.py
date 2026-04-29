# ruff: noqa: E402  -- load_dotenv() must run before src.* imports that read os.getenv at module load.
"""
The Glass - Publish CLI

Single command-line entry point for the publish pipeline.  Responsibilities,
in order:

    1. Parse arguments.
    2. Load .env, configure logging.
    3. Run config validation (fatal on error).
    4. Hand off to :func:`src.publish.orchestrator.run_publish` (or, with
       ``--export-config``, to the apps-script config exporter directly).

Anything that touches HTTP, the database, or post-sync hooks (e.g. clasp
push) lives in :mod:`src.publish.orchestrator`.

Usage:
    python -m src.publish.cli --league nba
    python -m src.publish.cli --league nba --tab BOS
    python -m src.publish.cli --league nba --stat-rate per_minute
    python -m src.publish.cli --league nba --export-config
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
from src.publish.config_validation import validate_all
from src.publish.definitions.config import (
    DEFAULT_STAT_RATE,
    HISTORICAL_TIMEFRAMES,
    STAT_RATES,
)
from src.publish.lib.export_config import export_config
from src.publish.orchestrator import run_publish

logger = logging.getLogger(__name__)


def _build_parser():
    parser = make_base_parser(
        prog='python -m src.publish.cli',
        description='The Glass -- publish league data to Google Sheets.',
    )
    parser.formatter_class = HelpFormatter

    parser.add_argument(
        '--league', choices=sorted({'nba', 'ncaa'}), required=True,
        help='League to sync.',
    )
    parser.add_argument(
        '--tab', metavar='NAME', default=None,
        help='Sync this tab first (team abbr like "BOS", or "all_players" / "all_teams").',
    )
    parser.add_argument(
        '--stat-rate', choices=sorted(STAT_RATES), default=DEFAULT_STAT_RATE,
        help='Default visible stat rate.',
    )
    parser.add_argument(
        '--historical-timeframe', type=int,
        choices=sorted(HISTORICAL_TIMEFRAMES),
        default=min(HISTORICAL_TIMEFRAMES),
        help='Number of previous seasons to include in historical sections.',
    )
    parser.add_argument(
        '--show-advanced', action='store_true',
        help='Render advanced stat columns visible by default.',
    )
    parser.add_argument(
        '--data-only', action='store_true',
        help='Fast sync: skip structural formatting, only update data + colors.',
    )
    parser.add_argument(
        '--export-config', action='store_true',
        help='Export the Apps Script config JS file and exit (no sheet sync).',
    )
    parser.add_argument(
        '--skip-config-export', action='store_true',
        help='Skip config build/export and clasp push entirely.',
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.no_color:
        style.disable()

    setup_logging(verbose=args.verbose, quiet=args.quiet)

    league = args.league.lower()
    historical_config = {'mode': 'seasons', 'value': args.historical_timeframe}

    print_banner(
        'The Glass -- Publish',
        f'league={league}  stat_rate={args.stat_rate}  data_only={args.data_only}',
    )
    print_summary(
        {
            'league':              league,
            'stat_rate':           args.stat_rate,
            'priority_tab':        args.tab or '(none)',
            'historical_seasons':  args.historical_timeframe,
            'data_only':           args.data_only,
            'show_advanced':       args.show_advanced,
            'export_config_only':  args.export_config,
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


if __name__ == '__main__':
    sys.exit(main())
