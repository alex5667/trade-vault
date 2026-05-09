from __future__ import annotations

import time
from typing import Any

from utils.time_utils import get_ny_time_millis


def utc_epoch_ms(value: Any = None) -> int:
    if value is None:
        return get_ny_time_millis()
    try:
        return int(float(value))
    except Exception:
        return get_ny_time_millis()


def monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


def clamp_non_negative_ms(value: Any) -> int:
    try:
        v = int(float(value))
    except Exception:
        return 0
    return v if v >= 0 else 0
