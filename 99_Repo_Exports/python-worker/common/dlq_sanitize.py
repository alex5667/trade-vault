"""
DLQ (Dead Letter Queue) sanitization utilities.

Provides functions to sanitize data before sending to dead letter queues.
"""

import json
from datetime import datetime
from typing import Any


def sanitize_for_dlq(data: Any, max_depth: int = 3, max_length: int = 1000) -> Any:
    """
    Sanitize data for safe storage in dead letter queue.

    This function removes or truncates problematic data that could cause
    issues when storing in queues or logs.

    Args:
        data: Data to sanitize
        max_depth: Maximum nesting depth
        max_length: Maximum string length

    Returns:
        Sanitized data
    """
    return _sanitize_value(data, max_depth, max_length, 0)


def _sanitize_value(value: Any, max_depth: int, max_length: int, current_depth: int) -> Any:
    """Recursively sanitize a value."""

    # Prevent infinite recursion
    if current_depth > max_depth:
        return f"<truncated: max depth {max_depth} exceeded>"

    # Handle None
    if value is None:
        return None

    # Handle basic types
    if isinstance(value, (int, float, bool)):
        return value

    # Handle strings
    if isinstance(value, str):
        if len(value) > max_length:
            return value[:max_length] + f"...<truncated {len(value) - max_length} chars>"
        return value

    # Handle datetime objects
    if isinstance(value, datetime):
        return value.isoformat()

    # Handle dictionaries
    if isinstance(value, dict):
        if current_depth >= max_depth:
            return f"<dict with {len(value)} keys>"
        return {
            str(k): _sanitize_value(v, max_depth, max_length, current_depth + 1)
            for k, v in value.items()
        }

    # Handle lists/tuples
    if isinstance(value, (list, tuple)):
        if current_depth >= max_depth:
            return f"<{type(value).__name__} with {len(value)} items>"
        # Limit list size
        max_items = 10
        items = value[:max_items]
        result = [_sanitize_value(item, max_depth, max_length, current_depth + 1) for item in items]
        if len(value) > max_items:
            result.append(f"<... and {len(value) - max_items} more items>")
        return result

    # Handle sets
    if isinstance(value, set):
        return list(_sanitize_value(list(value), max_depth, max_length, current_depth))

    # Handle other objects - convert to string representation
    try:
        str_repr = str(value)
        if len(str_repr) > max_length:
            return f"<{type(value).__name__}: {str_repr[:max_length]}...>"
        return f"<{type(value).__name__}: {str_repr}>"
    except Exception:
        return f"<{type(value).__name__}: <unrepresentable>>"


def truncate_message(message: str, max_length: int = 500) -> str:
    """
    Truncate a message to a maximum length.

    Args:
        message: Message to truncate
        max_length: Maximum length

    Returns:
        Truncated message
    """
    if len(message) <= max_length:
        return message

    return message[:max_length - 3] + "..."


def safe_json_dumps(data: Any, **kwargs) -> str:
    """
    Safely serialize data to JSON, handling problematic values.

    Args:
        data: Data to serialize
        **kwargs: Additional arguments for json.dumps

    Returns:
        JSON string
    """
    try:
        sanitized = sanitize_for_dlq(data)
        return json.dumps(sanitized, **kwargs)
    except Exception as e:
        # If sanitization fails, create a minimal error representation
        return json.dumps({
            "error": "Failed to serialize data",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "data_type": type(data).__name__
        }, **kwargs)
