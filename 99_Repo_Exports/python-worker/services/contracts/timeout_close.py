"""timeout_close.py — DTO for max-hold timeout close commands.

Producer: TradeMonitor._request_real_timeout_close()
Consumer: BinanceExecutor (action=timeout_close), MT5 bridge (action=CLOSE)
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal, Optional

Venue = Literal["binance_futures", "mt5", "paper"]
TimeoutReason = Literal[
    "TIMEOUT_MAX_HOLD",
    "TIMEOUT_PROFITABLE",
    "TIMEOUT_ADVERSE_MOVE",
    "TIMEOUT_SESSION_END",
    "TIMEOUT_STALE_SIGNAL",
]

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class TimeoutCloseCommand:
    """Immutable command published to orders:queue when max-hold fires on a real position."""

    v: int
    action: Literal["timeout_close"]
    sid: str
    symbol: str
    venue: Venue
    close_reason_raw: TimeoutReason
    request_ts_ms: int
    entry_ts_ms: int
    max_hold_ms: int
    age_ms: int
    idempotency_key: str
    source: str = "trade_monitor"
    position_id: Optional[str] = None
    ticket: Optional[str] = None
    expected_side: Optional[str] = None
    expected_qty: Optional[float] = None
    last_price: Optional[float] = None
    last_price_ts_ms: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)
