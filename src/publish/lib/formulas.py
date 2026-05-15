"""Pure helpers for TAB_COLUMNS value lambdas.

These replace the tuple-tree DSL formerly evaluated by
`calculations.evaluate_expression`. Column lambdas call these directly
so logic lives in real code, not interpreter rules.

Null propagation contract:
    - The evaluator (`evaluate_formula`) wraps each lambda call in a
      boundary that catches TypeError / ZeroDivisionError / KeyError
      and surfaces them as None. This means lambdas can use natural
      Python arithmetic (`r['x'] * 40 / r['y']`) without per-operation
      null guards. A missing field, a None operand, or a zero divisor
      collapses cleanly to None.
    - Helpers below mirror that contract internally for the rare cases
      where callers need an explicit None signal.
"""

from datetime import date, datetime
from typing import Any, Callable, Union


def calculate_age(birthdate: Any) -> Union[float, None]:
    """Age in years rounded to one decimal.

    Accepts datetime, date, or ISO date string. Returns None on null or
    unparseable input. Mirrors the prior interpreter's `calculate_age`
    operator semantics exactly.
    """
    if birthdate is None:
        return None
    if isinstance(birthdate, datetime):
        bdate = birthdate.date()
    elif isinstance(birthdate, date):
        bdate = birthdate
    else:
        try:
            bdate = datetime.fromisoformat(str(birthdate)).date()
        except ValueError:
            return None
    age_years = (date.today() - bdate).days / 365.25
    return round(age_years, 1)


def lookup(key_value: Any, table: str, field: str,
           ctx: Union[dict, None]) -> Any:
    """Resolve a foreign-key style lookup against `ctx['lookup_tables']`.

    Lookup tables are structured `{table_name: {key: {field: value}}}`.
    Returns None when the key is missing or the table/entry is not present.
    """
    if key_value is None:
        return None
    table_data = (ctx or {}).get('lookup_tables', {}).get(table, {})
    entry = table_data.get(key_value)
    return entry.get(field) if entry else None


def seasons_in_query(ctx: Union[dict, None]) -> int:
    """Return the season count from runtime context (default 1)."""
    return (ctx or {}).get('seasons_in_query', 1)


def team_average(value_fn: Callable[[dict, Union[dict, None]], Any],
                 ctx: Union[dict, None]) -> Union[float, None]:
    """Minute-weighted average of `value_fn(player, ctx)` across the team.

    `ctx['team_players']` provides the iterable of player rows. Each player
    contributes `value_fn(player, ctx) * minutes` to the weighted sum, with
    weight equal to the player's minutes. Returns None when no eligible
    player has positive minutes or when `team_players` is empty.

    Exceptions raised by `value_fn` for individual players are caught and
    that player is skipped, mirroring the lambda evaluator's boundary
    behavior so a single malformed row does not break a team aggregate.
    """
    team_players = (ctx or {}).get('team_players', [])
    if not team_players:
        return None
    total_weight = 0.0
    weighted_sum = 0.0
    for player in team_players:
        try:
            value = value_fn(player, ctx)
        except (TypeError, ZeroDivisionError, KeyError):
            continue
        minutes = (player.get('minutes_x10', 0) or 0) / 10.0
        if value is not None and minutes > 0:
            weighted_sum += float(value) * minutes
            total_weight += minutes
    if total_weight == 0:
        return None
    return weighted_sum / total_weight
