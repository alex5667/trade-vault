from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict


@dataclass
class _EMA:
    # Time-based EMA with continuous-time decay:
    # alpha(dt) = 1 - exp(-dt/tau)
    tau_ms: float
    v: float = 0.0
    initialized: bool = False
    last_ts_ms: int = 0

    def update(self, x: float, ts_ms: int) -> float:
        x = float(x)
        ts_ms = int(ts_ms)
        if not self.initialized:
            self.v = x
            self.initialized = True
            self.last_ts_ms = ts_ms
            return self.v

        dt = ts_ms - int(self.last_ts_ms)
        if dt <= 0:
            # same-time or backward timestamp: no update (deterministic)
            return self.v

        self.last_ts_ms = ts_ms
        tau = float(self.tau_ms) if float(self.tau_ms) > 1.0 else 1.0
        alpha = 1.0 - math.exp(-float(dt) / tau)
        self.v = self.v + alpha * (x - self.v)
        return self.v


class TickQualityEMA:
    # Per-symbol EMAs for tick-quality signals.
    #
    # Tracks:
    #   - unknown_side_ema: share of unknown-side ticks (0..1)
    #   - ts_now_ema: share of event_ts sourced from wall/now (0..1)
    #   - ts_stream_id_ema: share of event_ts sourced from stream_id (0..1)
    #   - skew_abs_ema_ms: EMA of abs(event_ts_ms - stream_ms)
    #   - age_abs_ema_ms: EMA of abs(now_ms - event_ts_ms)
    def __init__(self, tau_ms: int = 300_000) -> None:
        self.tau_ms = int(tau_ms)
        self._by_symbol: Dict[str, Dict[str, _EMA]] = {}

    def _s(self, symbol: str) -> Dict[str, _EMA]:
        s = self._by_symbol.get(symbol)
        if s is None:
            s = {
                "unknown": _EMA(tau_ms=self.tau_ms)
                "ts_now": _EMA(tau_ms=self.tau_ms)
                "ts_stream_id": _EMA(tau_ms=self.tau_ms)
                "skew_abs_ms": _EMA(tau_ms=self.tau_ms)
                "age_abs_ms": _EMA(tau_ms=self.tau_ms)
            }
            self._by_symbol[symbol] = s
        return s

    def update(
        self
        *
        symbol: str
        ts_ms: int
        unknown_side: float
        ts_source: str
        abs_skew_ms: float
        abs_age_ms: float
    ) -> Dict[str, float]:
        s = self._s(str(symbol))
        ts_ms = int(ts_ms)
        ts_source = str(ts_source or "")
        now_flag = 1.0 if ts_source in ("now", "wall") else 0.0
        stream_id_flag = 1.0 if ts_source == "stream_id" else 0.0

        out = {
            "unknown": s["unknown"].update(float(unknown_side), ts_ms)
            "ts_now": s["ts_now"].update(now_flag, ts_ms)
            "ts_stream_id": s["ts_stream_id"].update(stream_id_flag, ts_ms)
            "skew_abs_ms": s["skew_abs_ms"].update(float(abs_skew_ms), ts_ms)
            "age_abs_ms": s["age_abs_ms"].update(float(abs_age_ms), ts_ms)
        }
        return out
