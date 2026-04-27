# regime_engine.py
"""
Regime Engine: классификация режима рынка (trend/range/mixed) на основе ATR, VWAP, delta flow, crossings.

Основные компоненты:
- BarBuilder1m: агрегация тиков в 1-минутные бары
- RegimeEngine: расчет режима рынка с score [-1, +1]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Deque
from collections import deque
import math
import time


def clamp(x: float, lo: float, hi: float) -> float:
    """Clamp value to [lo, hi] range."""
    return lo if x < lo else hi if x > hi else x


@dataclass
class Bar:
    """1-minute bar for regime calculation."""
    ts_open: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    delta: float  # signed volume sum for the bar


class BarBuilder1m:
    """
    Builds 1-minute bars from ticks.

    Maintains current bar state and returns completed bars.
    """

    def __init__(self):
        self.cur: Optional[Bar] = None
        self.cur_minute: Optional[int] = None

    def update_tick(self, ts_ms: int, price: float, volume: float, delta: float) -> Optional[Bar]:
        """
        Update with new tick data. Returns completed bar if minute boundary crossed.

        Args:
            ts_ms: timestamp in milliseconds
            price: trade price
            volume: trade volume
            delta: signed delta (positive for buy, negative for sell)

        Returns:
            Completed Bar if minute changed, None otherwise
        """
        minute_id = ts_ms // 60000  # 60000 ms = 1 minute

        if self.cur is None:
            # Initialize first bar
            self.cur_minute = minute_id
            self.cur = Bar(
                ts_open=minute_id * 60000,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=volume,
                delta=delta
            )
            return None

        if minute_id != self.cur_minute:
            # Minute changed - return completed bar and start new one
            finished = self.cur
            self.cur_minute = minute_id
            self.cur = Bar(
                ts_open=minute_id * 60000,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=volume,
                delta=delta
            )
            return finished

        # Same minute - update current bar
        b = self.cur
        b.high = max(b.high, price)
        b.low = min(b.low, price)
        b.close = price
        b.volume += volume
        b.delta += delta
        return None


@dataclass
class RegimeState:
    """
    Current regime state returned by RegimeEngine.compute()
    """
    ts_ms: int = 0
    score: float = 0.0  # [-1, +1]: +1=trend, -1=range, 0=mixed
    label: str = "mixed"  # "trend", "range", "mixed"

    # Diagnostic features
    atr1m: float = 0.0
    atr5m: float = 0.0
    atr_q: float = 0.5  # quantile [0..1]

    vwap: float = 0.0
    open_day: float = 0.0

    delta_ema: float = 0.0
    vwap_cross_rate: float = 0.0  # [0..1] fraction of crossings
    hold_side_score: float = 0.0  # [-1..+1] persistence of one-sided price action


class RegimeEngine:
    """
    Stable intraday regime classifier.

    Features used:
    - ATR quantiles (volatility context)
    - VWAP deviation and crossing frequency (range vs trend behavior)
    - Delta flow directionality (momentum)
    - Hold-side persistence (trend persistence)

    Score in [-1..+1]:
    - +1: strong trend (high ATR, directional delta, persistent one-sided moves)
    - -1: strong range (low ATR, frequent VWAP crossings, oscillating delta)
    - 0: mixed/uncertain
    """

    def __init__(self, config):
        """
        Initialize with configuration.

        Args:
            config: configuration object with regime_* parameters
        """
        self.cfg = config

        # ATR(14) 1m calculation
        self.atr_n = int(getattr(config, "regime_atr_n", 14))
        self._prev_close_1m: Optional[float] = None
        self._atr1m: float = 0.0
        self._atr1m_init_count = 0

        # ATR(14) 5m via aggregation of 1m TR into 5m bars
        self._tr1m_for_5m: Deque[float] = deque(maxlen=5)
        self._atr5m: float = 0.0
        self._atr5m_init_count = 0

        # Rolling quantile window for atr1m
        self._atr_hist: Deque[float] = deque(maxlen=int(getattr(config, "regime_atr_hist", 240)))  # ~4h
        self._atr_q: float = 0.5

        # VWAP accumulators
        self._pv: float = 0.0  # price * volume sum
        self._vol: float = 0.0  # total volume
        self._vwap: float = 0.0
        self._open_day: float = 0.0
        self._day_id: Optional[int] = None

        # Delta flow EMA (directional momentum)
        self._delta_ema: float = 0.0
        self._delta_alpha = float(getattr(config, "regime_delta_ema_alpha", 0.05))

        # "Ping-pong" around VWAP: track crossings in last K minutes
        self._cross_hist: Deque[int] = deque(maxlen=int(getattr(config, "regime_cross_hist", 30)))  # 30m
        self._last_side_vs_vwap: int = 0  # -1, 0, +1

        # "Hold one side" persistence (EMA of side bias)
        self._hold_ema: float = 0.0
        self._hold_alpha = float(getattr(config, "regime_hold_ema_alpha", 0.10))

        self.state = RegimeState()

    # ---------- Helpers ----------

    def _update_day_open(self, ts_ms: int, price: float) -> None:
        """Reset daily accumulators at day boundary."""
        day_id = ts_ms // 86400000  # 86400000 ms = 1 day
        if self._day_id is None or day_id != self._day_id:
            self._day_id = day_id
            self._open_day = price
            self._pv = 0.0
            self._vol = 0.0
            self._vwap = price
            self._cross_hist.clear()
            self._last_side_vs_vwap = 0
            self._hold_ema = 0.0

    def on_tick(self, ts_ms: int, price: float, volume: float, delta: float) -> None:
        """
        Update regime features with tick data.

        Args:
            ts_ms: timestamp in milliseconds
            price: trade price
            volume: trade volume
            delta: signed delta (positive for buy, negative for sell)
        """
        # Day boundary check and reset
        self._update_day_open(ts_ms, price)

        # VWAP update (trade-based)
        if volume > 0.0:
            self._pv += price * volume
            self._vol += volume
            self._vwap = self._pv / self._vol if self._vol > 0 else price

        # Delta EMA (directional flow)
        self._delta_ema = (self._delta_alpha * delta) + ((1.0 - self._delta_alpha) * self._delta_ema)

        # VWAP crossings & hold-side (computed against VWAP)
        side = 0
        if self._vwap > 0:
            if price > self._vwap:
                side = 1
            elif price < self._vwap:
                side = -1

        crossed = 1 if (self._last_side_vs_vwap != 0 and side != 0 and side != self._last_side_vs_vwap) else 0
        self._cross_hist.append(crossed)
        self._last_side_vs_vwap = side if side != 0 else self._last_side_vs_vwap

        # Hold EMA: +1 if above vwap, -1 if below, 0 if near
        self._hold_ema = (self._hold_alpha * float(side)) + ((1.0 - self._hold_alpha) * self._hold_ema)

    def on_bar_1m(self, bar_ts: int, high: float, low: float, close: float) -> None:
        """
        Update ATR calculations with completed 1-minute bar.

        Args:
            bar_ts: bar open timestamp
            high: bar high
            low: bar low
            close: bar close
        """
        # True Range
        if self._prev_close_1m is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - self._prev_close_1m), abs(low - self._prev_close_1m))
        self._prev_close_1m = close

        # Wilder ATR update for 1m
        if self._atr1m_init_count < self.atr_n:
            self._atr1m = (self._atr1m * self._atr1m_init_count + tr) / (self._atr1m_init_count + 1)
            self._atr1m_init_count += 1
        else:
            self._atr1m = (self._atr1m * (self.atr_n - 1) + tr) / self.atr_n

        # 5m ATR proxy: aggregate 5 TRs
        self._tr1m_for_5m.append(tr)
        if len(self._tr1m_for_5m) == 5:
            tr5 = sum(self._tr1m_for_5m)
            if self._atr5m_init_count < self.atr_n:
                self._atr5m = (self._atr5m * self._atr5m_init_count + tr5) / (self._atr5m_init_count + 1)
                self._atr5m_init_count += 1
            else:
                self._atr5m = (self._atr5m * (self.atr_n - 1) + tr5) / self.atr_n

        # Update ATR quantile (rolling, cheap O(N) once per minute)
        self._atr_hist.append(self._atr1m)
        self._atr_q = self._approx_quantile(self._atr_hist, self._atr1m)

    @staticmethod
    def _approx_quantile(hist: Deque[float], x: float) -> float:
        """
        Approximate quantile by ranking against historical values.

        Args:
            hist: historical values
            x: current value

        Returns:
            quantile [0..1] of x within hist
        """
        if not hist:
            return 0.5
        arr = list(hist)
        # Count values <= x
        le = sum(1 for v in arr if v <= x)
        return le / max(len(arr), 1)

    def compute(self, ts_ms: int, price: float) -> RegimeState:
        """
        Compute current regime score and label.

        Args:
            ts_ms: current timestamp
            price: current price

        Returns:
            RegimeState with current classification
        """
        # Feature weights and thresholds
        atr_hi_q = float(getattr(self.cfg, "regime_atr_hi_q", 0.70))
        atr_lo_q = float(getattr(self.cfg, "regime_atr_lo_q", 0.35))

        # ATR component: high ATR => trend (+), low ATR => range (-)
        if self._atr_q >= atr_hi_q:
            s_atr = +1.0
        elif self._atr_q <= atr_lo_q:
            s_atr = -1.0
        else:
            # Linear interpolation between thresholds
            s_atr = (self._atr_q - 0.5) / 0.2  # ~[-1..+1] around middle
            s_atr = clamp(s_atr, -1.0, +1.0)

        # Delta flow component (directional momentum)
        delta_thr = float(getattr(self.cfg, "regime_delta_thr", 0.0))
        d = self._delta_ema
        if delta_thr > 0:
            s_delta = clamp(d / delta_thr, -1.0, +1.0)
        else:
            # Use tanh for natural scaling
            s_delta = math.tanh(d)

        # Hold-side component: persistence of one-sided moves
        s_hold = clamp(self._hold_ema, -1.0, +1.0)

        # Ping-pong component: frequent crossings => range (-)
        cross_rate = sum(self._cross_hist) / max(len(self._cross_hist), 1)
        # Map cross_rate ~ [0..0.5] to [ +0 .. -1 ]
        s_pingpong = -clamp(cross_rate / 0.20, 0.0, 1.0)

        # Weighted combination
        w_atr = float(getattr(self.cfg, "regime_w_atr", 0.35))
        w_delta = float(getattr(self.cfg, "regime_w_delta", 0.30))
        w_hold = float(getattr(self.cfg, "regime_w_hold", 0.25))
        w_ping = float(getattr(self.cfg, "regime_w_ping", 0.20))

        score = (
            w_atr * s_atr +
            w_delta * s_delta +
            w_hold * s_hold +
            w_ping * s_pingpong
        )
        score = clamp(score, -1.0, +1.0)

        # Label classification
        hi = float(getattr(self.cfg, "regime_label_hi", 0.35))
        lo = float(getattr(self.cfg, "regime_label_lo", -0.35))
        if score >= hi:
            label = "trend"
        elif score <= lo:
            label = "range"
        else:
            label = "mixed"

        # Update state
        self.state = RegimeState(
            ts_ms=ts_ms,
            score=score,
            label=label,
            atr1m=self._atr1m,
            atr5m=self._atr5m,
            atr_q=self._atr_q,
            vwap=self._vwap,
            open_day=self._open_day,
            delta_ema=self._delta_ema,
            vwap_cross_rate=cross_rate,
            hold_side_score=s_hold,
        )
        return self.state
