from __future__ import annotations

import math
from typing import Any


def safe_float(x: Any, default: float = float("nan")) -> float:
    """
    Convert to float without throwing.
    - None/bytes/str/NaN -> default
    - inf/-inf -> default
    """
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def safe_isfinite(x: Any) -> bool:
    """
    math.isfinite(...) that never throws on None/str/bytes/etc.
    """
    try:
        return bool(math.isfinite(float(x)))
    except Exception:
        return False
