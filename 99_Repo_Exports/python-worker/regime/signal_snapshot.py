from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class SignalL3Snapshot:
    """L3-Lite метрики для сигнала"""
    spread_bps: float = 0.0
    microprice_shift_bps_20: float = 0.0

    obi_5: float = 0.0
    obi_20: float = 0.0
    obi_50: float = 0.0
    obi_persistence_score: float = 0.0

    cancel_to_trade_bid_5s: float = 0.0
    cancel_to_trade_ask_5s: float = 0.0
    cancel_to_trade_bid_20s: float = 0.0
    cancel_to_trade_ask_20s: float = 0.0

    microprice_velocity_bps: float = 0.0
    queue_pressure_bid: float = 0.0
    queue_pressure_ask: float = 0.0
    market_depth_imbalance: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SignalSnapshot:
    """Полный snapshot сигнала для логирования"""
    signal_id: str
    symbol: str
    ts_ms: int
    direction: int
    signal_family: str
    conf_score: float

    # L3-Lite snapshot
    l3: SignalL3Snapshot

    # Основные метрики (примеры - расширить по необходимости)
    atr_14: float = 0.0   # 1m ATR (signal_logger maps as atr_1m)
    atr_5m: float = 0.0   # 5m ATR
    delta_spike_z: float = 0.0
    obi_avg_20: float = 0.0
    weak_progress_ratio: float = 0.0

    # Extra field to preserve additional context (e.g. confidence metrics, debug flags)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for database insertion"""
        result = asdict(self)
        # Flatten L3 fields
        l3_dict = result.pop('l3')
        for key, value in l3_dict.items():
            result[f'l3_{key}'] = value
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SignalSnapshot:
        """Create SignalSnapshot from dictionary"""
        l3_data = {}
        # Try to find L3 keys (prefixed or not)
        l3_keys = [f.name for f in SignalL3Snapshot.__dataclass_fields__.values()]
        for k in l3_keys:
            if k in data:
                l3_data[k] = data[k]
            elif f"l3_{k}" in data:
                l3_data[k] = data[f"l3_{k}"]

        l3 = SignalL3Snapshot(**l3_data)

        # Populate extra logic:
        # 1. Start with existing 'extra' or empty dict
        extra = data.get("extra")
        if not isinstance(extra, dict):
            extra = {}
        else:
            extra = extra.copy()

        # 2. Preserve 'indicators' if present (contains confidence metrics)
        if "indicators" in data and isinstance(data["indicators"], dict):
            extra["indicators"] = data["indicators"]

        inds = data.get("indicators", {})
        if not isinstance(inds, dict):
            inds = {}

        def _get_field(key: str, fallback: str = "") -> Any:
            v = data.get(key)
            if v is not None: return v
            if fallback and data.get(fallback) is not None: return data.get(fallback)
            v = inds.get(key)
            if v is not None: return v
            if fallback and inds.get(fallback) is not None: return inds.get(fallback)
            return None

        # Map fields
        return cls(
            signal_id=data.get("signal_id", ""),
            symbol=data.get("symbol", ""),
            ts_ms=data.get("ts_ms") or data.get("ts") or 0,
            direction=data.get("direction", 0),
            signal_family=data.get("signal_family") or data.get("setup_type") or "unknown",
            conf_score=data.get("conf_score") or data.get("confidence") or data.get("final_score") or 0.0,
            atr_14=float(_get_field("atr_14", "atr") or 0.0),
            atr_5m=float(_get_field("atr_5m", "atr_5m") or 0.0),
            delta_spike_z=float(_get_field("delta_spike_z", "delta_z") or 0.0),
            obi_avg_20=float(_get_field("obi_avg_20", "obi") or 0.0),
            weak_progress_ratio=float(_get_field("weak_progress_ratio", "weak_progress") or 0.0),
            l3=l3,
            extra=extra
        )

def build_signal_snapshot(
    signal_id: str,
    symbol: str,
    ts_ms: int,
    family: str,
    conf_score: float,
    ctx: Any  # SignalContext with L3 fields
) -> SignalSnapshot:
    """Build SignalSnapshot from SignalContext"""

    l3 = SignalL3Snapshot(
        spread_bps=getattr(ctx, 'spread_bps', 0.0),
        microprice_shift_bps_20=getattr(ctx, 'microprice_shift_bps_20', 0.0),
        obi_5=getattr(ctx, 'obi_5', 0.0),
        obi_20=getattr(ctx, 'obi_20', 0.0),
        obi_50=getattr(ctx, 'obi_50', 0.0),
        obi_persistence_score=getattr(ctx, 'obi_persistence_score', 0.0),
        cancel_to_trade_bid_5s=getattr(ctx, 'cancel_to_trade_bid_5s', 0.0),
        cancel_to_trade_ask_5s=getattr(ctx, 'cancel_to_trade_ask_5s', 0.0),
        cancel_to_trade_bid_20s=getattr(ctx, 'cancel_to_trade_bid_20s', 0.0),
        cancel_to_trade_ask_20s=getattr(ctx, 'cancel_to_trade_ask_20s', 0.0),
        microprice_velocity_bps=getattr(ctx, 'microprice_velocity_bps', 0.0),
        queue_pressure_bid=getattr(ctx, 'queue_pressure_bid', 0.0),
        queue_pressure_ask=getattr(ctx, 'queue_pressure_ask', 0.0),
        market_depth_imbalance=getattr(ctx, 'market_depth_imbalance', 0.0),
    )

    # Extract extra from ctx if available
    extra = getattr(ctx, 'extra', {})
    if not isinstance(extra, dict):
        extra = {}

    return SignalSnapshot(
        signal_id=signal_id,
        symbol=symbol,
        ts_ms=ts_ms,
        direction=getattr(ctx, 'direction', 0),
        signal_family=family,
        conf_score=conf_score,
        # Основные поля (расширить по необходимости)
        atr_14=getattr(ctx, 'atr', 0.0),
        atr_5m=getattr(ctx, 'atr_5m', 0.0),
        delta_spike_z=getattr(ctx, 'z_delta', getattr(ctx, 'delta_spike_z', 0.0)),
        obi_avg_20=getattr(ctx, 'obi_avg_20', getattr(ctx, 'obi', 0.0)),
        weak_progress_ratio=getattr(ctx, 'weak_progress_ratio', 0.0),
        l3=l3,
        extra=extra
    )
