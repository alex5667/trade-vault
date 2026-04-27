"""
Unified SignalContext for the entire scanner_infra pipeline.

This context flows from:
- Signal detector → ExecutionPlanner → Redis → MT5/Nest → PerformanceTracker

Only contains fields actually needed by planner/performance-tracker.
All microstructure (deltaSpikeZ, OBI, weakProgress etc.) goes into features/tags.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Dict, List, Optional, Any

from .models import (
    Side,
    AccountState,
    SwingPoint,
    HTFLevel,
    OrderBookSnapshot,
)


@dataclass
class SignalContext:
    """
    Base signal context for entire pipeline:
    - detector → ExecutionPlanner → Redis → MT5/Nest → PerformanceTracker.

    IMPORTANT:
      Only fields actually needed by planner/performance-tracker.
      All microstructure (deltaSpikeZ, OBI, weakProgress etc.) goes into features/tags.
    """

    # identification
    signal_id: str
    symbol: str
    setup_type: str
    side: Side

    # time and price at detection
    ts_signal: datetime
    price_at_signal: float

    # volatility and instrument specification
    atr_1m: float
    tick_size: float
    contract_size: float

    # model scoring
    final_score: float

    # account risk profile
    account_state: AccountState

    # microstructural levels for stop/targets
    local_swings: List[SwingPoint] = field(default_factory=list)
    htf_levels: List[HTFLevel] = field(default_factory=list)

    # order book snapshot (if want to use later in execution logic)
    orderbook: Optional[OrderBookSnapshot] = None

    # custom model features: deltaSpikeZ, OBI, volumeSpikeZ ...
    features: Dict[str, float] = field(default_factory=dict)

    # any additional data
    extra: Dict[str, Any] = field(default_factory=dict)

    # override for signal lifetime (if want to pull from Timescale)
    ttd_expiry_bars: Optional[int] = None

    # --- serialization for Redis/Timescale ---

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert dataclass → dict → JSON-compatible structure.
        Nested dataclasses also converted to dict.
        """
        payload = asdict(self)

        # convert Enum → str
        payload["side"] = self.side.value

        # AccountState
        payload["account_state"] = {
            "equity_usd": self.account_state.equity_usd,
            "open_risk_usd": self.account_state.open_risk_usd,
            "max_risk_per_trade_pct": self.account_state.max_risk_per_trade_pct,
            "max_portfolio_risk_pct": self.account_state.max_portfolio_risk_pct,
        }

        # Swings
        payload["local_swings"] = [
            {
                "ts": sp.ts.isoformat(),
                "price": sp.price,
                "type": sp.type,
                "volume": sp.volume,
                "delta": sp.delta,
            }
            for sp in self.local_swings
        ]

        # HTF levels
        payload["htf_levels"] = [
            {
                "ts": lv.ts.isoformat(),
                "price": lv.price,
                "kind": lv.kind,
                "strength": lv.strength,
            }
            for lv in self.htf_levels
        ]

        # orderbook (if exists)
        if self.orderbook is not None:
            ob = self.orderbook
            payload["orderbook"] = {
                "ts": ob.ts.isoformat(),
                "best_bid": ob.best_bid,
                "best_ask": ob.best_ask,
                "bids": ob.bids,
                "asks": ob.asks,
            }

        # signal time
        payload["ts_signal"] = self.ts_signal.isoformat()

        return payload

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SignalContext":
        """
        Reverse operation (if want to restore context from Redis/Timescale).
        """
        from datetime import datetime

        side = Side(data["side"])
        ts_signal = datetime.fromisoformat(data["ts_signal"])

        acc = data["account_state"]
        account_state = AccountState(
            equity_usd=acc["equity_usd"],
            open_risk_usd=acc["open_risk_usd"],
            max_risk_per_trade_pct=acc["max_risk_per_trade_pct"],
            max_portfolio_risk_pct=acc["max_portfolio_risk_pct"],
        )

        swings = [
            SwingPoint(
                ts=datetime.fromisoformat(sp["ts"]),
                price=sp["price"],
                type=sp["type"],
                volume=sp.get("volume", 0.0),
                delta=sp.get("delta", 0.0),
            )
            for sp in data.get("local_swings", [])
        ]

        levels = [
            HTFLevel(
                ts=datetime.fromisoformat(lv["ts"]),
                price=lv["price"],
                kind=lv["kind"],
                strength=lv.get("strength", 1.0),
            )
            for lv in data.get("htf_levels", [])
        ]

        ob_data = data.get("orderbook")
        orderbook = None
        if ob_data:
            orderbook = OrderBookSnapshot(
                ts=datetime.fromisoformat(ob_data["ts"]),
                best_bid=ob_data["best_bid"],
                best_ask=ob_data["best_ask"],
                bids=ob_data.get("bids", []),
                asks=ob_data.get("asks", []),
            )

        return cls(
            signal_id=data["signal_id"],
            symbol=data["symbol"],
            setup_type=data["setup_type"],
            side=side,
            ts_signal=ts_signal,
            price_at_signal=data["price_at_signal"],
            atr_1m=data["atr_1m"],
            tick_size=data["tick_size"],
            contract_size=data["contract_size"],
            final_score=data["final_score"],
            account_state=account_state,
            local_swings=swings,
            htf_levels=levels,
            orderbook=orderbook,
            features=data.get("features", {}),
            extra=data.get("extra", {}),
            ttd_expiry_bars=data.get("ttd_expiry_bars"),
        )
