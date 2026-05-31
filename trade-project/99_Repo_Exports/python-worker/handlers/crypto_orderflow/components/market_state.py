from __future__ import annotations

import math
from collections import deque
from typing import Any

from common.deque_utils import ensure_bounded_deque
from core.vol_z_thr_calibrator import VolZThrCalibrator
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import BarSample


class CryptoMarketState:
    """
    Manages market state history (bars, regime samples) and basic extreme detection.
    """
    def __init__(
        self,
        bar_history_maxlen: int = 512,
        regime_window_size: int = 240,
        vol_z_calibrator: VolZThrCalibrator | None = None,
    ):
        self._bar_history: dict[str, deque[BarSample]] = {}
        self._regime_history: dict[str, deque[Any]] = {}

        # Config params
        self._bar_history_maxlen = bar_history_maxlen
        self._regime_cfg_window_size = regime_window_size

        # Adaptive vol_z threshold calibrator (shadow-mode by default)
        self._vol_z_cal: VolZThrCalibrator = vol_z_calibrator or VolZThrCalibrator()

    def get_bar_hist(self, symbol: str) -> deque[BarSample]:
        """Get or initialize bounded deque for bar history."""
        # Protection against unlimited growth
        if symbol not in self._bar_history:
            self._bar_history[symbol] = deque(maxlen=self._bar_history_maxlen)

        d = self._bar_history[symbol]
        # Ensure correct maxlen if it changed (though usually static)
        d2 = ensure_bounded_deque(d, self._bar_history_maxlen)
        if d2 is not d:
            self._bar_history[symbol] = d2
        return d2

    def get_regime_hist(self, symbol: str) -> deque[Any]:
        """Get or initialize bounded deque for regime history."""
        if symbol not in self._regime_history:
            self._regime_history[symbol] = deque(maxlen=self._regime_cfg_window_size)

        d = self._regime_history[symbol]
        d2 = ensure_bounded_deque(d, self._regime_cfg_window_size)
        if d2 is not d:
            self._regime_history[symbol] = d2
        return d2

    def update_bar_history(self, symbol: str, bar: BarSample) -> None:
        """Append new bar to history."""
        hist = self.get_bar_hist(symbol)
        hist.append(bar)

    def is_new_local_extreme(
        self,
        symbol: str,
        bar: BarSample,
        atr_intraday: float,
        k_atr: float = 0.25,
        vol_z_thr: float | None = None,  # None → use calibrated threshold
        session: str = "na",             # e.g. "us" / "eu" / "asia" / "off"
    ) -> bool:
        """
        Detect if bar creates a new local extreme with volume spike.

        `vol_z_thr=None` delegates to the embedded VolZThrCalibrator.
        Pass an explicit float to override (e.g. in tests or legacy callers).
        """
        hist = self.get_bar_hist(symbol)
        if not hist or len(hist) < 20:
            return False

        highs = [b.high for b in hist]
        lows = [b.low for b in hist]
        vols = [b.volume for b in hist]  # type: ignore

        if len(highs) < 2:
            return False

        prev_high = max(highs[:-1])
        prev_low = min(lows[:-1])

        mu_vol = sum(vols[:-1]) / (len(vols) - 1)
        n_var = max(len(vols) - 2, 1)
        var_vol = sum((v - mu_vol) ** 2 for v in vols[:-1]) / n_var
        std_vol = math.sqrt(max(var_vol, 1e-9))

        vol_z = (bar.volume - mu_vol) / max(std_vol, 1e-6)  # type: ignore

        # Calibrated threshold (shadow-mode by default, fail-open)
        regime = f"{symbol.lower()}:{session}"
        self._vol_z_cal.observe(regime=regime, vol_z=vol_z)
        th = self._vol_z_cal.thresholds(regime=regime)
        effective_thr = vol_z_thr if vol_z_thr is not None else th.soft

        is_new_high = bar.high > prev_high + k_atr * atr_intraday and vol_z >= effective_thr
        is_new_low = bar.low < prev_low - k_atr * atr_intraday and vol_z >= effective_thr

        return is_new_high or is_new_low
