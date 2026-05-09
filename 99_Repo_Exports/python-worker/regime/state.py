from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

Status = Literal["active", "degraded", "disabled"]


@dataclass
class RegimeState:
    family: str
    venue: str
    symbol: str
    timeframe: str

    status: Status = "active"
    wr_window: float = 0.0
    exp_r_window: float = 0.0
    dd_r_window: float = 0.0
    trades_window: int = 0

    disable_until: datetime | None = None
    threshold_mult: float = 1.0
    reason: str = ""
