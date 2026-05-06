"""
The Glass - Rate Limiting Configuration

Default rate limiting configuration for all sources and destinations.
Sources/destinations can override these defaults by providing their own
rate_limits configuration in their respective definitions files.

Minimal set of knobs to handle all rate limiting scenarios:
- requests_per_second: Throttling (lower = slower)
- max_retries: Retry attempts before giving up
- backoff_base: Exponential backoff base (seconds)
- timeout_default: Request timeout (seconds)
- max_consecutive_failures: When to trigger auto-restart
"""

from typing import Any, Dict

DEFAULT_RATE_LIMITS: Dict[str, Any] = {
    # Throttling: aggressive by default (2 requests per second)
    'requests_per_second': 2.0,
    
    # Retry behavior: 3 retries with exponential backoff
    'max_retries': 3,
    'backoff_base': 30,
    
    # Timeouts: 30s default, 120s for bulk operations
    'timeout_default': 30,
    'timeout_bulk': 120,
    
    # Auto-restart: trigger after 5 consecutive failures
    'max_consecutive_failures': 5,
    
    # Auto-restart toggle: whether to trigger auto-restart on consecutive failures
    'auto_restart': True,
}

RATE_LIMITS_SCHEMA: Dict[str, Dict[str, Any]] = {
    'requests_per_second': {'required': True, 'types': (int, float)},
    'max_retries': {'required': True, 'types': (int,)},
    'backoff_base': {'required': True, 'types': (int, float)},
    'timeout_default': {'required': True, 'types': (int, float)},
    'timeout_bulk': {'required': True, 'types': (int, float)},
    'max_consecutive_failures': {'required': True, 'types': (int,)},
    'auto_restart': {'required': True, 'types': (bool,)},
}
