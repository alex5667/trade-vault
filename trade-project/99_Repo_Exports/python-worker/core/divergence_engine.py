from __future__ import annotations

from dataclasses import dataclass

from core.swing_detector import SwingPoint
import contextlib


@dataclass
class DivergenceEvent:
    """
    Divergence between price swing and CVD swing.
    """
    kind: str  # bullish_regular | bearish_regular | bullish_hidden | bearish_hidden
    ts_ms: int
    prev_ts_ms: int
    price_prev: float
    price_curr: float
    cvd_prev: float
    cvd_curr: float
    strength: float


class DivergenceEngine:
    """
    Lightweight divergence engine.
    """

    def __init__(
        self,
        min_strength: float = 2.5,
        min_price_bp: float = 5.0,
        require_bias_for_hidden: bool = True,
    ) -> None:
        self.min_strength = float(min_strength)
        self.min_price_bp = float(min_price_bp)
        self.require_bias_for_hidden = bool(require_bias_for_hidden)

        self._prev_high: SwingPoint | None = None
        self._prev_low: SwingPoint | None = None
        self._last_event: DivergenceEvent | None = None

    def apply_config(self, cfg: dict) -> None:
        with contextlib.suppress(Exception):
            self.min_strength = float(cfg.get("div_strength_min", self.min_strength))
        with contextlib.suppress(Exception):
            self.min_price_bp = float(cfg.get("div_min_price_bp", self.min_price_bp))
        with contextlib.suppress(Exception):
            self.require_bias_for_hidden = bool(cfg.get("div_require_bias_hidden", self.require_bias_for_hidden))

    @staticmethod
    def _bp(a: float, b: float) -> float:
        mid = 0.5 * (abs(a) + abs(b))
        if mid <= 1e-12:
            return 0.0
        return 10000.0 * abs(a - b) / mid

    def _strength(self, price_prev: float, price_curr: float, cvd_prev: float, cvd_curr: float) -> float:
        price_sep_bp = self._bp(price_prev, price_curr)
        cvd_den = max(1.0, abs(cvd_prev))
        cvd_sep_rel = abs(cvd_curr - cvd_prev) / cvd_den
        return (price_sep_bp / max(1.0, self.min_price_bp)) * (1.0 + 5.0 * cvd_sep_rel)

    def update_swing(self, sp: SwingPoint, trend_bias: str = "none") -> list[DivergenceEvent]:
        out: list[DivergenceEvent] = []
        bias = (trend_bias or "none").lower()

        if sp.kind == "high":
            prev = self._prev_high
            self._prev_high = sp
            if prev is None:
                return out

            price_hh = sp.price > prev.price
            price_lh = sp.price < prev.price
            cvd_hh = sp.cvd > prev.cvd
            cvd_lh = sp.cvd < prev.cvd

            # bearish regular: price HH, CVD LH
            if price_hh and cvd_lh and self._bp(prev.price, sp.price) >= self.min_price_bp:
                s = self._strength(prev.price, sp.price, prev.cvd, sp.cvd)
                if s >= self.min_strength:
                    ev = DivergenceEvent(
                        kind="bearish_regular",
                        ts_ms=sp.ts_ms,
                        prev_ts_ms=prev.ts_ms,
                        price_prev=prev.price,
                        price_curr=sp.price,
                        cvd_prev=prev.cvd,
                        cvd_curr=sp.cvd,
                        strength=s,
                    )
                    self._last_event = ev
                    out.append(ev)

            # bearish hidden: price LH, CVD HH (downtrend)
            if price_lh and cvd_hh and self._bp(prev.price, sp.price) >= self.min_price_bp:
                if (not self.require_bias_for_hidden) or (bias == "down"):
                    s = self._strength(prev.price, sp.price, prev.cvd, sp.cvd)
                    if s >= self.min_strength:
                        ev = DivergenceEvent(
                            kind="bearish_hidden",
                            ts_ms=sp.ts_ms,
                            prev_ts_ms=prev.ts_ms,
                            price_prev=prev.price,
                            price_curr=sp.price,
                            cvd_prev=prev.cvd,
                            cvd_curr=sp.cvd,
                            strength=s,
                        )
                        self._last_event = ev
                        out.append(ev)

        elif sp.kind == "low":
            prev = self._prev_low
            self._prev_low = sp
            if prev is None:
                return out

            price_ll = sp.price < prev.price
            price_hl = sp.price > prev.price
            cvd_hl = sp.cvd > prev.cvd
            cvd_ll = sp.cvd < prev.cvd

            # bullish regular: price LL, CVD HL
            if price_ll and cvd_hl and self._bp(prev.price, sp.price) >= self.min_price_bp:
                s = self._strength(prev.price, sp.price, prev.cvd, sp.cvd)
                if s >= self.min_strength:
                    ev = DivergenceEvent(
                        kind="bullish_regular",
                        ts_ms=sp.ts_ms,
                        prev_ts_ms=prev.ts_ms,
                        price_prev=prev.price,
                        price_curr=sp.price,
                        cvd_prev=prev.cvd,
                        cvd_curr=sp.cvd,
                        strength=s,
                    )
                    self._last_event = ev
                    out.append(ev)

            # bullish hidden: price HL, CVD LL (uptrend)
            if price_hl and cvd_ll and self._bp(prev.price, sp.price) >= self.min_price_bp:
                if (not self.require_bias_for_hidden) or (bias == "up"):
                    s = self._strength(prev.price, sp.price, prev.cvd, sp.cvd)
                    if s >= self.min_strength:
                        ev = DivergenceEvent(
                            kind="bullish_hidden",
                            ts_ms=sp.ts_ms,
                            prev_ts_ms=prev.ts_ms,
                            price_prev=prev.price,
                            price_curr=sp.price,
                            cvd_prev=prev.cvd,
                            cvd_curr=sp.cvd,
                            strength=s,
                        )
                        self._last_event = ev
                        out.append(ev)

        return out

    def last_event(self) -> DivergenceEvent | None:
        return self._last_event
