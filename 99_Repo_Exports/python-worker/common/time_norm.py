from utils.time_utils import get_ny_time_millis
"""
Time normalization utilities for the scanner infrastructure.

Provides functions to normalize and convert timestamps.
"""

import math
import time
from typing import Union
from datetime import datetime, timezone


def normalize_epoch_ms(timestamp: Union[int, float, str, datetime]) -> int:
    """
    Normalize a timestamp to milliseconds since epoch.

    Args:
        timestamp: Timestamp in various formats

    Returns:
        Timestamp as integer milliseconds since epoch
    """
    def _normalize_number(value: Union[int, float]) -> int:
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"Non-finite timestamp: {value}")

        v = float(value)
        if v <= 0:
            return int(v)

        # Unit policy:
        # - epoch seconds:      1e9..1e10   (2001..2286)
        # - epoch milliseconds: 1e12..1e13  (2001..2286)
        # - epoch microseconds: 1e15..1e16
        # - epoch nanoseconds:  1e18..1e19
        # Sub-1e9 values are treated as seconds to preserve historical behavior.
        abs_v = abs(v)
        if abs_v < 10_000_000_000:
            return int(round(v * 1000))
        if abs_v < 10_000_000_000_000:
            return int(v)
        if abs_v < 10_000_000_000_000_000:
            return int(v / 1000)
        return int(v / 1_000_000)

    if isinstance(timestamp, int):
        return _normalize_number(timestamp)

    if isinstance(timestamp, float):
        return _normalize_number(timestamp)

    if isinstance(timestamp, str):
        # Try to parse as ISO format or as float
        try:
            # Try ISO format first
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            # Try as float string
            try:
                return normalize_epoch_ms(float(timestamp))
            except ValueError:
                raise ValueError(f"Cannot parse timestamp string: {timestamp}")

    if isinstance(timestamp, datetime):
        # Convert datetime to milliseconds
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp() * 1000)

    raise TypeError(f"Unsupported timestamp type: {type(timestamp)}")


def normalize_epoch_seconds(timestamp: Union[int, float, str, datetime]) -> int:
    """
    Normalize a timestamp to seconds since epoch.

    Args:
        timestamp: Timestamp in various formats

    Returns:
        Timestamp as integer seconds since epoch
    """
    ms = normalize_epoch_ms(timestamp)
    return ms // 1000


def current_time_ms() -> int:
    """
    Get current time in milliseconds since epoch.

    Returns:
        Current timestamp in milliseconds
    """
    return get_ny_time_millis()


def current_time_seconds() -> int:
    """
    Get current time in seconds since epoch.

    Returns:
        Current timestamp in seconds
    """
    return int(time.time())


def format_timestamp_ms(timestamp_ms: int, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Format a millisecond timestamp as a string.

    Args:
        timestamp_ms: Timestamp in milliseconds
        format_str: Format string for strftime

    Returns:
        Formatted timestamp string
    """
    seconds = timestamp_ms / 1000
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime(format_str)


def format_timestamp_seconds(timestamp_s: int, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Format a second timestamp as a string.

    Args:
        timestamp_s: Timestamp in seconds
        format_str: Format string for strftime

    Returns:
        Formatted timestamp string
    """
    dt = datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
    return dt.strftime(format_str)


def parse_duration(duration_str: str) -> int:
    """
    Parse a duration string like "1h", "30m", "45s", "500ms" into milliseconds.

    Args:
        duration_str: Duration string

    Returns:
        Duration in milliseconds
    """
    duration_str = duration_str.strip().lower()

    # Handle milliseconds
    if duration_str.endswith('ms'):
        return int(duration_str[:-2])

    # Handle seconds
    if duration_str.endswith('s'):
        return int(duration_str[:-1]) * 1000

    # Handle minutes
    if duration_str.endswith('m'):
        return int(duration_str[:-1]) * 60 * 1000

    # Handle hours
    if duration_str.endswith('h'):
        return int(duration_str[:-1]) * 60 * 60 * 1000

    # Handle days
    if duration_str.endswith('d'):
        return int(duration_str[:-1]) * 24 * 60 * 60 * 1000

    # Default to seconds if no unit
    try:
        return int(duration_str) * 1000
    except ValueError:
        raise ValueError(f"Invalid duration format: {duration_str}")


def add_duration_ms(timestamp_ms: int, duration_str: str) -> int:
    """
    Add a duration to a millisecond timestamp.

    Args:
        timestamp_ms: Base timestamp in milliseconds
        duration_str: Duration string (e.g., "1h", "30m")

    Returns:
        New timestamp in milliseconds
    """
    duration_ms = parse_duration(duration_str)
    return timestamp_ms + duration_ms


def time_since_ms(timestamp_ms: int) -> int:
    """
    Calculate milliseconds since the given timestamp.

    Args:
        timestamp_ms: Past timestamp in milliseconds

    Returns:
        Milliseconds elapsed since that timestamp
    """
    return current_time_ms() - timestamp_ms


def is_recent_ms(timestamp_ms: int, max_age_str: str = "1h") -> bool:
    """
    Check if a timestamp is recent (within max_age).

    Args:
        timestamp_ms: Timestamp to check
        max_age_str: Maximum age string (e.g., "1h", "30m")

    Returns:
        True if timestamp is recent, False otherwise
    """
    max_age_ms = parse_duration(max_age_str)
    return time_since_ms(timestamp_ms) <= max_age_ms
