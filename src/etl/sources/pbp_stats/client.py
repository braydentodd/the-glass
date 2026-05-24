"""
The Glass - PBP Stats Client

Wraps pbpstats.com public API endpoints behind the same fetcher contract used
by the ETL execution engine.
"""

import logging
from typing import Any, Callable, Dict, List, Union

import requests

from src.core.lib.rate_limiter import get_rate_limiter
from src.etl.sources.pbp_stats.config import API_CONFIG, DATASETS

logger = logging.getLogger(__name__)


def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize known pbpstats payload shapes into a list of row dicts."""
    rows = payload.get('multi_row_table_data')
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]

    rows = payload.get('results')
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]

    single_row = payload.get('single_row_table_data')
    if isinstance(single_row, dict):
        return [single_row]

    return []


def _to_result_set(
    rows: List[Dict[str, Any]],
    result_set_name: str,
) -> Dict[str, Any]:
    """Convert row dicts into NBA-style ``resultSets`` payload."""
    if not rows:
        return {
            'resultSets': [
                {
                    'name': result_set_name,
                    'headers': [],
                    'rowSet': [],
                },
            ],
        }

    headers = list(rows[0].keys())
    row_set = [[row.get(header) for header in headers] for row in rows]
    return {
        'resultSets': [
            {
                'name': result_set_name,
                'headers': headers,
                'rowSet': row_set,
            },
        ],
    }


def _build_dataset_params(
    season: str,
    season_type_name: str,
    entity: str,
    extra_params: Union[Dict[str, Any], None] = None,
) -> Dict[str, Any]:
    """Build query params for pbpstats requests."""
    params: Dict[str, Any] = {
        'Season': season,
        'SeasonType': season_type_name,
    }

    if extra_params:
        for k, v in extra_params.items():
            if k == 'team_id':
                params['TeamId'] = v
            else:
                params[k] = v

    if 'Type' not in params:
        params['Type'] = 'Team' if entity == 'team' else 'Player'

    return params


def make_fetcher(season: str, season_type_name: str, entity: str) -> Callable:
    """Create an API fetcher closure for pbpstats datasets."""
    rate_limiter = get_rate_limiter('pbp_stats')

    def fetch(
        dataset: str,
        extra_params: Union[Dict[str, Any], None] = None,
    ) -> Union[Dict[str, Any], None]:
        ds_cfg = DATASETS.get(dataset)
        if ds_cfg is None:
            logger.warning('Unknown pbp_stats dataset: %s', dataset)
            return None

        endpoint = ds_cfg['endpoint']
        url = f"{API_CONFIG['base_url']}/{endpoint}/{API_CONFIG['league']}"
        params = _build_dataset_params(season, season_type_name, entity, extra_params)

        def _call() -> Dict[str, Any]:
            response = requests.get(
                url,
                params=params,
                timeout=rate_limiter.get_timeout(is_bulk=True),
            )
            response.raise_for_status()

            payload = response.json()
            if isinstance(payload, dict) and payload.get('detail'):
                raise RuntimeError(f"PBP API error for {dataset}: {payload['detail']}")

            rows = _extract_rows(payload)
            return _to_result_set(rows, ds_cfg['default_result_set'])

        return rate_limiter.with_retry(_call)

    return fetch
