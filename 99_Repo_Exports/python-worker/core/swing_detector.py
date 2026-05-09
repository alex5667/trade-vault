from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from core.microbar import MicroBar
import contextlib


@dataclass
class SwingPoint:
    """
    Swing point на микро-барах.
    """
    kind: str  # "high" | "low"
    ts_ms: int
    price: float
    cvd: float
    bar_start_ts_ms: int
    bar_end_ts_ms: int


class SwingDetector:
    """
    Fractal Swing Detector:
    Pivot High на баре i, если high[i] >= max(high[i-L..i+R])
    Pivot Low  на баре i, если low[i]  <= min(low[i-L..i+R])
    """

    def __init__(
        self,
        left: int = 3,
        right: int = 3,
        min_bp: float = 5.0,
        min_range_bp: float = 1.0,
    ) -> None:
        self.left = int(left)
        self.right = int(right)
        self.min_bp = float(min_bp)
        self.min_range_bp = float(min_range_bp)

        self._buf: deque[MicroBar] = deque(maxlen=max(20, self.left + self.right + 10))
        self._last_high: SwingPoint | None = None
        self._last_low: SwingPoint | None = None

    def apply_config(self, cfg: dict) -> None:
        try:
            self.left = int(cfg.get("swing_left", self.left))
            self.right = int(cfg.get("swing_right", self.right))
            if self.left < 1: self.left = 1
            if self.right < 1: self.right = 1
        except Exception:
            pass
        with contextlib.suppress(Exception):
            self.min_bp = float(cfg.get("swing_min_bp", self.min_bp))
        with contextlib.suppress(Exception):
            self.min_range_bp = float(cfg.get("swing_min_range_bp", self.min_range_bp))

    @staticmethod
    def _bp(a: float, b: float) -> float:
        mid = 0.5 * (abs(a) + abs(b))
        if mid <= 1e-12:
            return 0.0
        return 10000.0 * abs(a - b) / mid

    def _range_bp(self, bar: MicroBar) -> float:
        return self._bp(bar.high, bar.low)

    def update(self, bar: MicroBar) -> list[SwingPoint]:
        out: list[SwingPoint] = []
        self._buf.append(bar)

        L = self.left
        R = self.right
        n = len(self._buf)
        if n < (L + R + 1):
            return out

        center_idx = n - 1 - R
        window = list(self._buf)[center_idx - L : center_idx + R + 1]
        center = window[L]

        if self._range_bp(center) < self.min_range_bp:
            return out

        highs = [b.high for b in window]
        lows = [b.low for b in window]

        is_pivot_high = center.high >= max(highs)
        is_pivot_low = center.low <= min(lows)

        if is_pivot_high:
            sp = SwingPoint(
                kind="high",
                ts_ms=int(center.end_ts_ms),
                price=float(center.high),
                cvd=float(center.cvd_close),
                bar_start_ts_ms=int(center.start_ts_ms),
                bar_end_ts_ms=int(center.end_ts_ms),
            )
            if self._last_high is None or self._bp(sp.price, self._last_high.price) >= self.min_bp:
                self._last_high = sp
                out.append(sp)

        if is_pivot_low:
            sp = SwingPoint(
                kind="low",
                ts_ms=int(center.end_ts_ms),
                price=float(center.low),
                cvd=float(center.cvd_close),
                bar_start_ts_ms=int(center.start_ts_ms),
                bar_end_ts_ms=int(center.end_ts_ms),
            )
            if self._last_low is None or self._bp(sp.price, self._last_low.price) >= self.min_bp:
                self._last_low = sp
                out.append(sp)

        return out

    def last_swings(self) -> tuple[SwingPoint | None, SwingPoint | None]:
        return self._last_high, self._last_low
