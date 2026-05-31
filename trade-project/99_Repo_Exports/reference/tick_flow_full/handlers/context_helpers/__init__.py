"""
Utility functions package for handlers.

Provides context manipulation, type conversion, and helper utilities.
"""

from .context_utils import (
    get_attr,
    set_attr,
    safe_float_pos,
    first_item,
    normalize_side_int,
    side_int_to_payload,
    ensure_levels,
    to_float_or_nan,
    to_opt_float,
)

__all__ = [
    "get_attr",
    "set_attr",
    "safe_float_pos",
    "first_item",
    "normalize_side_int",
    "side_int_to_payload",
    "ensure_levels",
    "to_float_or_nan",
    "to_opt_float",
]
