"""
Shoot the Sheet - Publish Orchestrator

Top-level orchestration for the publish layer.  Mirrors :mod:`src.etl.orchestrator`:

    src.publish.cli           (argparse + dispatch)
        |
        v
    src.publish.orchestrator  (this module: build SyncContext, drive sheets)
        |
        +--> src.publish.lib.executor    (one sheet at a time)
        +--> src.publish.lib.calculations (percentile populations)
        +--> src.publish.lib.queries      (reads from the warehouse)

Knows nothing about HTTP transports or argparse -- just builds the run
context, precomputes shared percentile populations once, and dispatches
to the per-sheet workers in :mod:`src.publish.lib.executor`.
"""

from __future__ import annotations

import logging
import subprocess
import time
from collections import defaultdict
from functools import partial
from pathlib import Path
from typing import Union

from src.core.definitions.leagues import LEAGUES
from src.core.lib.leagues_resolver import (
    format_season_label,
    get_current_season,
    get_current_season_year,
    get_regular_season_types,
)
from src.core.lib.logging import phase_marker
from src.core.lib.postgres import get_db_connection
from src.core.lib.terminal import progress
from src.publish.definitions.destinations import DESTINATIONS
from src.publish.definitions.layout import AGGREGATE_SHEETS, SECTIONS_CONFIG

HISTORICAL_TIMEFRAMES = [1, 3, 5]
from src.publish.destinations.google_sheets.client import get_sheets_client
from src.publish.destinations.google_sheets.config import (
    GOOGLE_SHEETS_CONFIG,
    SHEETS_FORMATTING,
)
from src.publish.destinations.google_sheets.config_exporter import export_config
from src.publish.lib.calculations import compute_pct_by_rate, derive_db_fields
from src.publish.lib.executor import (
    SyncContext,
    sync_players_sheet,
    sync_team_sheet,
    sync_teams_sheet,
)
from src.publish.lib.progress_tracker import (
    complete_run,
    fail_run,
    mark_sheet_completed,
    mark_sheet_failed,
    mark_sheet_started,
    resolve_work,
    update_run_completed_sheets,
)
from src.publish.lib.queries import (
    fetch_all_players,
    fetch_all_teams,
    get_teams_from_db,
)

logger = logging.getLogger(__name__)

_AUTO_RESUME = True


def _push_apps_script_config(league: str, destination: str) -> None:
    """Regenerate the Apps Script config file and push it via clasp.

    Reads the apps_script directory from the destination config.  Failures
    are logged but never raised: the sheet sync has already succeeded by the
    time this runs, and a missing clasp install or a transient push failure
    should not invalidate the upstream work.
    """
    dest_cfg = DESTINATIONS.get(destination, {})
    apps_script_cfg = dest_cfg.get("apps_script", {})
    if not apps_script_cfg.get("enabled", False):
        return

    apps_script_dir = Path(__file__).resolve().parents[2] / apps_script_cfg["directory"]

    try:
        path = export_config(league)
        logger.info("Apps Script config exported to %s", path)
    except Exception:
        logger.exception("Apps Script config export failed.")
        return

    logger.info("Running clasp push from %s", apps_script_dir)
    try:
        subprocess.run(["clasp", "push", "-f"], cwd=apps_script_dir, check=True)
        logger.info("clasp push succeeded")
    except subprocess.CalledProcessError as exc:
        logger.error("clasp push failed: %s", exc)
    except FileNotFoundError:
        logger.error("The clasp CLI is not installed or not in PATH.")


# ---------------------------------------------------------------------------
# Percentile pre-computation  (one-shot, used by every sheet below)
# ---------------------------------------------------------------------------


def _precompute_percentiles(
    ctx: SyncContext,
    historical_config: dict,
) -> dict:
    """Pre-compute league-wide percentile populations for every stat rate.

    Called once per orchestrator run so every team and aggregate sheet share
    the same population baselines (consistency + speed).
    """
    current_season = ctx.league_config[ctx.season_key]
    current_season_year = ctx.league_config["current_season_year"]
    season_type_val = ctx.league_config.get("season_type", "rs")

    query_kw = dict(
        historical_config=historical_config,
        ctx=ctx,
        current_season=current_season,
        current_season_year=current_season_year,
        season_type_val=season_type_val,
    )

    needs_current = True
    needs_historical = True
    needs_postseason = True

    supported_years = list(HISTORICAL_TIMEFRAMES.keys())
    empty_teams = {"teams": [], "opponents": []}

    conn = get_db_connection()
    try:
        all_players_curr = (
            fetch_all_players(conn, "current_stats", **query_kw)
            if needs_current
            else []
        )
        all_teams_curr = (
            fetch_all_teams(conn, "current_stats", **query_kw)
            if needs_current
            else empty_teams
        )

        all_players_hist: dict = {}
        all_players_post: dict = {}
        all_teams_hist: dict = {}
        all_teams_post: dict = {}

        for y in supported_years:
            hist_kw = query_kw.copy()
            hist_kw["historical_config"] = {"mode": "seasons", "value": y}
            all_players_hist[y] = (
                fetch_all_players(conn, "historical_stats", **hist_kw)
                if needs_historical
                else []
            )
            all_players_post[y] = (
                fetch_all_players(conn, "postseason_stats", **hist_kw)
                if needs_postseason
                else []
            )
            all_teams_hist[y] = (
                fetch_all_teams(conn, "historical_stats", **hist_kw)
                if needs_historical
                else empty_teams
            )
            all_teams_post[y] = (
                fetch_all_teams(conn, "postseason_stats", **hist_kw)
                if needs_postseason
                else empty_teams
            )

        # Build player groups for team_average context in team percentiles.
        player_groups = defaultdict(list)
        for p in all_players_curr:
            ta = p.get(ctx.team_abbr_field)
            if ta:
                player_groups[ta].append(p)

        def _team_context_fn(entity):
            abbr = entity.get(ctx.team_abbr_field)
            return {"team_players": player_groups.get(abbr, [])}

        player_dict = {"current_stats": all_players_curr}
        team_dict = {"current_stats": all_teams_curr["teams"]}
        opp_dict = {"current_stats": all_teams_curr["opponents"]}

        for y in supported_years:
            player_dict[f"historical_stats_{y}yr"] = all_players_hist[y]
            player_dict[f"postseason_stats_{y}yr"] = all_players_post[y]
            team_dict[f"historical_stats_{y}yr"] = all_teams_hist[y]["teams"]
            team_dict[f"postseason_stats_{y}yr"] = all_teams_post[y]["teams"]
            opp_dict[f"historical_stats_{y}yr"] = all_teams_hist[y]["opponents"]
            opp_dict[f"postseason_stats_{y}yr"] = all_teams_post[y]["opponents"]

        precomputed = {
            "player": compute_pct_by_rate(player_dict, "player"),
            "team": compute_pct_by_rate(
                team_dict,
                "team",
                context_fn=_team_context_fn,
            ),
            "opponents": compute_pct_by_rate(opp_dict, "opponents"),
            "data": {
                "player": player_dict,
                "team": team_dict,
                "opponents": opp_dict,
            },
        }
        logger.info("Percentile populations ready")
        return precomputed
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# sheet-loop helper
# ---------------------------------------------------------------------------


def _sync_all_sheets(
    ctx,
    client,
    spreadsheet,
    conn,
    run_id: int,
    *,
    team_sheets,
    aggregate_sheets,
    pending_lookup: dict,
    team_names: dict,
    precomputed: dict,
    delay: float,
    sync_kwargs: dict,
) -> list:
    """Iterate team sheets then aggregate sheets, tracking progress per sheet.

    Returns a list of sheet names that failed.
    """
    db_schema = ctx.db_schema
    total_pending = sum(
        1 for t in list(team_sheets) + list(aggregate_sheets) if t in pending_lookup
    )
    failed_sheets = []

    with progress(
        total=total_pending, desc="publish", unit="sheet", leave=False
    ) as bar:
        for sheet in team_sheets:
            if sheet not in pending_lookup:
                continue
            pid = pending_lookup[sheet]
            bar.set_postfix_str(sheet, refresh=False)
            mark_sheet_started(conn, db_schema, pid)
            try:
                sync_team_sheet(
                    ctx,
                    client,
                    spreadsheet,
                    sheet,
                    team_name=team_names.get(sheet, sheet),
                    precomputed=precomputed,
                    **sync_kwargs,
                )
                mark_sheet_completed(conn, db_schema, pid)
                update_run_completed_sheets(conn, db_schema, run_id)
            except Exception as exc:
                logger.error("%s sheet failed: %s", sheet, exc, exc_info=True)
                failed_sheets.append(sheet)
                mark_sheet_failed(conn, db_schema, pid, str(exc))
            bar.update(1)
            time.sleep(delay)

        for sheet in aggregate_sheets:
            if sheet not in pending_lookup:
                continue
            pid = pending_lookup[sheet]
            bar.set_postfix_str(sheet, refresh=False)
            mark_sheet_started(conn, db_schema, pid)
            try:
                if sheet == "all_players":
                    sync_players_sheet(
                        ctx,
                        client,
                        spreadsheet,
                        precomputed=precomputed,
                        **sync_kwargs,
                    )
                else:
                    sync_teams_sheet(
                        ctx,
                        client,
                        spreadsheet,
                        precomputed=precomputed,
                        **sync_kwargs,
                    )
                mark_sheet_completed(conn, db_schema, pid)
                update_run_completed_sheets(conn, db_schema, run_id)
            except Exception as exc:
                logger.error("%s sheet failed: %s", sheet, exc, exc_info=True)
                failed_sheets.append(sheet)
                mark_sheet_failed(conn, db_schema, pid, str(exc))
            bar.update(1)
            time.sleep(delay)

    return failed_sheets


# ---------------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------------


def run_publish(
    league: str,
    stat_rate: str,
    show_advanced: bool,
    historical_config: dict,
    data_only: bool,
    priority_sheet: Union[str, None],
    config_export: bool = True,
    destination: str = "google_sheets",
) -> None:
    """Run a full sync for a league to the specified destination.

    Caller (the CLI) is expected to have configured logging and validated
    config before calling this function; nothing here touches stdout.
    """
    if league not in LEAGUES:
        raise ValueError(f"Unknown league: {league!r}")

    db_schema = league

    league_config = {
        "current_season": get_current_season(league),
        "current_season_year": get_current_season_year(league),
        "season_type": get_regular_season_types(league)[0],
    }

    # Bind format_season_label to the league's season_format so downstream
    # consumers (queries, header formatters) can call it with just an end-year.
    season_format_fn = partial(
        format_season_label,
        league_season_format=LEAGUES[league]["season_format"],
    )

    stats_sections = frozenset(
        name for name, cfg in SECTIONS_CONFIG.items() if cfg.get("stats_timeframe")
    )
    db_fields = derive_db_fields(league, stats_sections, set())

    ctx = SyncContext(
        league=league,
        google_sheets_config=GOOGLE_SHEETS_CONFIG[league],
        sheet_formatting=SHEETS_FORMATTING,
        league_config=league_config,
        db_schema=db_schema,
        player_entity_table="profiles.players",
        team_entity_table="profiles.teams",
        player_stats_table="stats.player_seasons",
        team_stats_table="stats.team_seasons",
        player_entity_fields=db_fields["player_entity_fields"],
        team_entity_fields=db_fields["team_entity_fields"],
        stat_fields=db_fields["stat_fields"],
        team_stat_fields=db_fields["team_stat_fields"],
        primary_minutes_col=(
            "mins_x10" if "mins_x10" in db_fields["stat_fields"] else "minutes"
        ),
        season_format_fn=season_format_fn,
    )

    logger.info(phase_marker("publish", f"league={league} rate={stat_rate}"))
    logger.info("Starting %s sync", "partial update" if data_only else "full")
    delay = (
        ctx.sheet_formatting.get("data_only_sync_delay_seconds", 0)
        if data_only
        else ctx.sheet_formatting.get("sync_delay_seconds", 0)
    )

    client = get_sheets_client(ctx.google_sheets_config)
    spreadsheet = client.open_by_key(ctx.google_sheets_config["spreadsheet_id"])

    sync_kwargs = dict(
        mode=stat_rate,
        show_advanced=show_advanced,
        historical_config=historical_config,
        data_only=data_only,
    )

    logger.info(phase_marker("precompute_percentiles"))
    precomputed = _precompute_percentiles(ctx, historical_config)

    teams_db = get_teams_from_db(ctx.db_schema)
    team_names = {abbr: name for _, (abbr, name) in teams_db.items()}
    abbrs = sorted(team_names.keys())

    if priority_sheet:
        pt = priority_sheet.upper()
        if pt in abbrs:
            abbrs = [pt] + [a for a in abbrs if a != pt]

    aggregate_order = list(AGGREGATE_SHEETS)
    if priority_sheet and priority_sheet.lower() in aggregate_order:
        first = priority_sheet.lower()
        aggregate_order = [first] + [s for s in aggregate_order if s != first]

    all_sheets = abbrs + aggregate_order

    # Auto-resume: resolve pending work (may be subset if resuming)
    conn = get_db_connection()
    try:
        run_process_id, pending_items = resolve_work(
            conn,
            db_schema,
            league,
            all_sheets,
            auto_resume=_AUTO_RESUME,
        )
    except Exception:
        conn.close()
        raise

    pending_lookup = {sheet: pid for sheet, pid in pending_items}
    total_pending = len(pending_items)
    logger.info(
        phase_marker(
            "publish_sheets", f"{total_pending} pending of {len(all_sheets)} total"
        ),
    )

    # Build team_gids for aggregate sheets (needed for hyperlink resolution)
    team_gids = {ws.title: ws.id for ws in spreadsheet.worksheets()}
    sync_kwargs["team_gids"] = team_gids

    failed_sheets = _sync_all_sheets(
        ctx,
        client,
        spreadsheet,
        conn,
        run_process_id,
        team_sheets=abbrs,
        aggregate_sheets=aggregate_order,
        pending_lookup=pending_lookup,
        team_names=team_names,
        precomputed=precomputed,
        delay=delay,
        sync_kwargs=sync_kwargs,
    )

    if failed_sheets:
        failed_list = ", ".join(failed_sheets)
        logger.error("Sync finished with failures: %s", failed_list)
        fail_run(
            conn, db_schema, run_process_id, f"Sync failed for sheet(s): {failed_list}"
        )
        conn.close()
        raise RuntimeError(f"Sync failed for sheet(s): {failed_list}")

    logger.info("Sync complete")
    complete_run(conn, db_schema, run_process_id)
    conn.close()

    # Apps Script config export is driven by destination config
    if config_export and DESTINATIONS.get(destination, {}).get("apps_script", {}).get(
        "enabled", False
    ):
        logger.info(phase_marker("apps_script_push"))
        _push_apps_script_config(league, destination)
