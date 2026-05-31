from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Tuple
from collections import deque
import math


@dataclass
class EdgeAbsorbEvent:
    ts_ms: int
    bias: str           # "LONG" or "SHORT"
    p90: float
    value: float
    range_expansion: int


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


class FPEdgeAbsorbDetector:
    """
    Detect "p90 delta without range expansion" on microbars.

    Input:
      - last_bar.fp_peak_delta (cluster delta)
      - last_bar.high/low/open/close

    Rolling window:
      - store abs(fp_peak_delta) for last W bars (e.g., 10-30 minutes of 1s bars => 600..1800)
      - compute p90 by sorting (O(W log W)) at refresh interval (not every bar)
    """
    def __init__(self, window_bars: int = 1800, refresh_every: int = 5) -> None:
        self.window_bars = int(window_bars)
        self.refresh_every = int(refresh_every)
        if self.window_bars < 200:
            self.window_bars = 200
        if self.refresh_every < 1:
            self.refresh_every = 1
        self._buf: Deque[float] = deque(maxlen=self.window_bars)
        self._p90: float = 0.0
        self._n: int = 0
        self._prev_low: Optional[float] = None
        self._prev_high: Optional[float] = None

    def apply_config(self, cfg: Dict[str, Any]) -> None:
        try:
            self.window_bars = int(cfg.get("fp_edge_window_bars", self.window_bars))
            if self.window_bars < 200:
                self.window_bars = 200
        except Exception:
            pass
        try:
            self.refresh_every = int(cfg.get("fp_edge_refresh_every", self.refresh_every))
            if self.refresh_every < 1:
                self.refresh_every = 1
        except Exception:
            pass
        # resize deque if changed
        if getattr(self._buf, "maxlen", None) != self.window_bars:
            old = list(self._buf)
            self._buf = deque(old[-self.window_bars:], maxlen=self.window_bars)

    @staticmethod
    def _percentile90(xs: list[float]) -> float:
        if not xs:
            return 0.0
        xs2 = sorted(xs)
        # nearest-rank
        k = int(math.ceil(0.90 * len(xs2))) - 1
        k = max(0, min(len(xs2) - 1, k))
        return float(xs2[k])

    def update_bar(self, bar: Any, cfg: Dict[str, Any]) -> Optional[EdgeAbsorbEvent]:
        if bar is None:
            return None
        if not bool(getattr(bar, "fp_enabled", False)):
            # still update prev range for expansion checks
            self._prev_low = _f(getattr(bar, "low", self._prev_low), self._prev_low or 0.0)
            self._prev_high = _f(getattr(bar, "high", self._prev_high), self._prev_high or 0.0)
            return None

        v = abs(_f(getattr(bar, "fp_peak_delta", 0.0), 0.0))
        if v <= 0:
            self._prev_low = _f(getattr(bar, "low", self._prev_low), self._prev_low or 0.0)
            self._prev_high = _f(getattr(bar, "high", self._prev_high), self._prev_high or 0.0)
            return None

        self._buf.append(v)
        self._n += 1
        if self._n % self.refresh_every == 0:
            self._p90 = self._percentile90(list(self._buf))

        p90 = float(self._p90)
        if p90 <= 0:
            self._prev_low = _f(getattr(bar, "low", self._prev_low), self._prev_low or 0.0)
            self._prev_high = _f(getattr(bar, "high", self._prev_high), self._prev_high or 0.0)
            return None

        mult = _f(cfg.get("fp_edge_p90_mult", 1.0), 1.0)
        min_buckets = int(cfg.get("fp_edge_min_buckets", 10))
        max_prog = _f(cfg.get("fp_edge_max_progress", 0.35), 0.35)
        prog = _f(getattr(bar, "fp_progress", 1.0), 1.0)
        nb = int(getattr(bar, "fp_n_buckets", 0) or 0)
        if nb < min_buckets or prog > max_prog:
            self._prev_low = _f(getattr(bar, "low", self._prev_low), self._prev_low or 0.0)
            self._prev_high = _f(getattr(bar, "high", self._prev_high), self._prev_high or 0.0)
            return None

        # Range expansion check: do we extend previous extreme?
        eps_ticks = int(cfg.get("fp_edge_eps_ticks", 2))
        tick_px = _f(cfg.get("tick_size_px", 0.0), 0.0)
        if tick_px <= 0:
            tick_px = _f(getattr(bar, "fp_bucket_px", 0.0), 1e-9)
        eps_px = max(1e-9, float(eps_ticks) * float(tick_px))

        low = _f(getattr(bar, "low", 0.0), 0.0)
        high = _f(getattr(bar, "high", 0.0), 0.0)

        prev_low = self._prev_low
        prev_high = self._prev_high
        self._prev_low = low
        self._prev_high = high

        # if no prev, cannot decide
        if prev_low is None or prev_high is None:
            return None

        peak_delta = _f(getattr(bar, "fp_peak_delta", 0.0), 0.0)
        # Down-pressure absorption => peak_delta negative => expect NO new low
        # Up-pressure absorption   => peak_delta positive => expect NO new high
        if peak_delta < 0:
            range_exp = 1 if (low < (prev_low - eps_px)) else 0
            bias = "LONG"
        else:
            range_exp = 1 if (high > (prev_high + eps_px)) else 0
            bias = "SHORT"

        if range_exp == 0 and v >= (mult * p90):
            ts = int(getattr(bar, "end_ts_ms", 0) or 0)
            return EdgeAbsorbEvent(ts_ms=ts, bias=bias, p90=p90, value=v, range_expansion=0)

        return None
