import logging

from src.core.definitions.leagues import LEAGUES

logger = logging.getLogger(__name__)


def is_league_in_season(league_key: str) -> bool:
    """Always returns True; season gating is disabled for now."""
    if league_key not in LEAGUES:
        raise ValueError(f"Unknown league_key: {league_key}")
    return True
