from __future__ import annotations

import math
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, Optional

from .types import RegimeFeatures, RegimeSample


def _is_finite(x: float) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


class RegimeDetector:
    """
    Handler-free regime feature engine.
      - owns history window logic (deque(maxlen=regime_window_size))
      - computes raw metrics + biases
    """

    def __init__(
        self,
        *,
        cfg: Any,
        history: Dict[str, Deque[RegimeSample]],
        get_htf_levels: Callable[[str], Any],
        compute_daily_open_cross_freq: Callable[[str], Optional[float]],
        now_s: Callable[[], float] = time.time,
    ) -> None:
        self.cfg = cfg
        self._history = history
        self._get_htf_levels = get_htf_levels
        self._cross_freq = compute_daily_open_cross_freq
        self._now_s = now_s

    def _maxlen(self) -> int:
        try:
            n = int(getattr(self.cfg, "regime_window_size", 240))
        except Exception:
            n = 240
        return max(10, n)  # guard tiny windows

    def _hist(self, symbol: str) -> Deque[RegimeSample]:
        h = self._history.get(symbol)
        want = self._maxlen()
        if h is None:
            h = deque(maxlen=want)
            self._history[symbol] = h
            return h
        # if existing deque has different maxlen (or None) -> rebuild to be stable
        if getattr(h, "maxlen", None) != want:
            nh: Deque[RegimeSample] = deque(list(h)[-want:], maxlen=want)
            self._history[symbol] = nh
            return nh
        return h

    def update_history(self, ctx: Any) -> None:
        """Mechanical перенесённый _update_regime_history."""
        symbol = getattr(ctx, "symbol", None)
        price = getattr(ctx, "last_price", None) or getattr(ctx, "price", None)
        vwap = getattr(ctx, "vwap", None)
        daily_open = getattr(ctx, "daily_open", None)

        if symbol is None or price is None:
            return
        try:
            price_f = float(price)
        except Exception:
            return
        if not _is_finite(price_f) or price_f <= 0.0:
            return

        now = getattr(ctx, "ts_utc", None) or self._now_s()
        try:
            now_f = float(now)
        except Exception:
            now_f = self._now_s()

        # сторона VWAP
        vwap_side = 0
        if vwap is not None:
            try:
                vwap_f = float(vwap)
            except Exception:
                vwap_f = float("nan")
            if _is_finite(vwap_f) and vwap_f > 0.0:
                diff_v = price_f - vwap_f
                if diff_v > 0.0:
                    vwap_side = 1
                elif diff_v < 0.0:
                    vwap_side = -1

        # сторона daily_open
        daily_open_side = 0
        if daily_open is not None:
            try:
                do_f = float(daily_open)
            except Exception:
                do_f = float("nan")
            if _is_finite(do_f) and do_f > 0.0:
                diff_o = price_f - do_f
                if diff_o > 0.0:
                    daily_open_side = 1
                elif diff_o < 0.0:
                    daily_open_side = -1

        h = self._hist(str(symbol))
        h.append(
            RegimeSample(
                ts=now_f,
                price=price_f,
                vwap_side=vwap_side,
                daily_open_side=daily_open_side,
                bar_index=None,
            )
        )

    def compute_features(self, ctx: Any) -> RegimeFeatures:
        """Mechanical перенесённый _compute_regime_features."""
        symbol = getattr(ctx, "symbol", None)
        price = getattr(ctx, "last_price", None) or getattr(ctx, "price", None)
        vwap = getattr(ctx, "vwap", None)
        daily_open = getattr(ctx, "daily_open", None)
        atr_14_bps = getattr(ctx, "atr_14_bps", None)
        weak_progress_raw = getattr(ctx, "weak_progress_raw", None)

        if symbol is None or price is None:
            return RegimeFeatures()
        try:
            price_f = float(price)
        except Exception:
            return RegimeFeatures()
        if not _is_finite(price_f) or price_f <= 0.0:
            return RegimeFeatures()

        # 1) VWAP dev (bps)
        vwap_dev_bps = None
        if vwap is not None:
            try:
                vwap_f = float(vwap)
            except Exception:
                vwap_f = float("nan")
            if _is_finite(vwap_f) and vwap_f > 0.0:
                vwap_dev_bps = abs(price_f - vwap_f) / price_f * 10_000.0

        # 2) daily_open dev (bps)
        daily_open_dev_bps = None
        if daily_open is not None:
            try:
                do_f = float(daily_open)
            except Exception:
                do_f = float("nan")
            if _is_finite(do_f) and do_f > 0.0:
                daily_open_dev_bps = abs(price_f - do_f) / do_f * 10_000.0

        # 3) cross freq
        daily_open_cross_freq = None
        try:
            daily_open_cross_freq = self._cross_freq(str(symbol))
        except Exception:
            daily_open_cross_freq = None
        if daily_open_cross_freq is not None:
            try:
                daily_open_cross_freq = float(daily_open_cross_freq)
            except Exception:
                daily_open_cross_freq = None
            if daily_open_cross_freq is not None and not _is_finite(daily_open_cross_freq):
                daily_open_cross_freq = None

        # 4) HTF nearest level dist (bps)
        htf_level_dist_bps = None
        try:
            htf_levels = self._get_htf_levels(str(symbol))
        except Exception:
            htf_levels = None
        if htf_levels is not None:
            levels = []
            for k in ("pdh", "pdl", "pdm"):
                if hasattr(htf_levels, k):
                    try:
                        v = float(getattr(htf_levels, k))
                    except Exception:
                        v = float("nan")
                    if _is_finite(v) and v > 0.0:
                        levels.append(v)
            if levels:
                htf_level_dist_bps = min(abs(price_f - lvl) / price_f * 10_000.0 for lvl in levels)

        # 5) biases
        atr_bias = None
        if atr_14_bps is not None:
            try:
                atr_f = float(atr_14_bps)
            except Exception:
                atr_f = float("nan")
            if _is_finite(atr_f):
                atr_bias = _clamp((atr_f - 50.0) / 50.0, -1.0, 1.0)

        delta_dir_bias = None
        hist = self._history.get(str(symbol))
        if hist and len(hist) >= 3:
            recent = list(hist)[-10:]
            sides = [s.vwap_side for s in recent if s.vwap_side != 0]
            if sides:
                pos = sum(1 for s in sides if s > 0)
                neg = sum(1 for s in sides if s < 0)
                tot = pos + neg
                if tot > 0:
                    delta_dir_bias = _clamp((pos - neg) / tot, -1.0, 1.0)

        vwap_dev_bias = None
        if vwap_dev_bps is not None and _is_finite(vwap_dev_bps):
            vwap_dev_bias = _clamp((float(vwap_dev_bps) - 25.0) / 75.0, -1.0, 1.0)

        daily_open_dev_bias = None
        if daily_open_dev_bps is not None and _is_finite(daily_open_dev_bps):
            daily_open_dev_bias = _clamp((float(daily_open_dev_bps) - 25.0) / 75.0, -1.0, 1.0)

        daily_open_cross_bias = None
        if daily_open_cross_freq is not None and _is_finite(daily_open_cross_freq):
            daily_open_cross_bias = _clamp(1.0 - 2.0 * float(daily_open_cross_freq), -1.0, 1.0)

        htf_prox_bias = None
        if htf_level_dist_bps is not None and _is_finite(htf_level_dist_bps):
            htf_prox_bias = _clamp(1.0 - (float(htf_level_dist_bps) / 50.0), -1.0, 1.0)

        weak_progress_bias = None
        if weak_progress_raw is not None:
            try:
                w = float(weak_progress_raw)
            except Exception:
                w = float("nan")
            if _is_finite(w):
                weak_progress_bias = _clamp((w - 0.5) * 2.0, -1.0, 1.0)

        return RegimeFeatures(
            vwap_dev_bps=vwap_dev_bps,
            daily_open_dev_bps=daily_open_dev_bps,
            daily_open_cross_freq=daily_open_cross_freq,
            htf_level_dist_bps=htf_level_dist_bps,
            atr_bias=atr_bias,
            delta_dir_bias=delta_dir_bias,
            vwap_dev_bias=vwap_dev_bias,
            daily_open_dev_bias=daily_open_dev_bias,
            daily_open_cross_bias=daily_open_cross_bias,
            htf_prox_bias=htf_prox_bias,
            weak_progress_bias=weak_progress_bias,
            session_bias=None,
        )
