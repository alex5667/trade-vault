"""
Common utilities package for scanner infrastructure.
"""

from .log import setup_logger, get_logger
from .robust_stats import RobustZscoreMADRolling, rolling_median, rolling_mad, robust_zscore
from .gpu_service import get_gpu_service, is_gpu_available, get_gpu_device_count
from .backoff import Backoff, retry_with_backoff, sleep_s, exponential_backoff_delay
from .redis_errors import (
    is_transient_error as is_transient_redis_error,
    is_redis_connection_error,
    is_redis_key_error,
    is_redis_stream_error,
    get_redis_error_category,
)
from .dlq_sanitize import sanitize_for_dlq, truncate_message, safe_json_dumps
from .time_norm import (
    normalize_epoch_ms,
    normalize_epoch_seconds,
    current_time_ms,
    current_time_seconds,
    format_timestamp_ms,
    format_timestamp_seconds,
    parse_duration,
    add_duration_ms,
    time_since_ms,
    is_recent_ms
)
from .time_utils import (
    get_current_timestamp_ms,
    get_current_timestamp_s,
    format_timestamp_for_redis,
    parse_timestamp_from_redis,
    extract_binance_close_time,
    format_duration_ms,
    normalize_timestamp,
    format_timestamp_iso
)

__all__ = [
    'setup_logger',
    'get_logger',
    'RobustZscoreMADRolling',
    'rolling_median',
    'rolling_mad',
    'robust_zscore',
    'get_gpu_service',
    'is_gpu_available',
    'get_gpu_device_count',
    'Backoff',
    'retry_with_backoff',
    'sleep_s',
    'exponential_backoff_delay',
    'is_transient_redis_error',
    'is_redis_connection_error',
    'is_redis_key_error',
    'is_redis_stream_error',
    'get_redis_error_category',
    'sanitize_for_dlq',
    'truncate_message',
    'safe_json_dumps',
    'normalize_epoch_ms',
    'normalize_epoch_seconds',
    'current_time_ms',
    'current_time_seconds',
    'format_timestamp_ms',
    'format_timestamp_seconds',
    'parse_duration',
    'add_duration_ms',
    'time_since_ms',
    'is_recent_ms',
    'get_current_timestamp_ms',
    'get_current_timestamp_s',
    'format_timestamp_for_redis',
    'parse_timestamp_from_redis',
    'extract_binance_close_time',
    'format_duration_ms',
    'normalize_timestamp',
    'format_timestamp_iso'
]
