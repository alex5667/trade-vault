from __future__ import annotations

import math
from collections import deque
from typing import Any, Dict, Deque

from common.deque_utils import ensure_bounded_deque
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import BarSample

class CryptoMarketState:
    """
    Manages market state history (bars, regime samples) and basic extreme detection.
    """
    def __init__(self, bar_history_maxlen: int = 512, regime_window_size: int = 240):
        self._bar_history: Dict[str, Deque[BarSample]] = {}
        self._regime_history: Dict[str, Deque[Any]] = {}
        
        # Config params
        self._bar_history_maxlen = bar_history_maxlen
        self._regime_cfg_window_size = regime_window_size

    def get_bar_hist(self, symbol: str) -> Deque[BarSample]:
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

    def get_regime_hist(self, symbol: str) -> Deque[Any]:
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
        k_atr: float = 0.25,     # 0.25 ATR top/bottom
        vol_z_thr: float = 1.5,  # z-score volume
    ) -> bool:
        """
        Detect if bar creates a new local extreme with volume spike.
        """
        hist = self.get_bar_hist(symbol)
        if not hist or len(hist) < 20:
            return False

        highs = [b.high for b in hist]
        lows = [b.low for b in hist]
        vols = [b.volume for b in hist]

        # Previous extremes (excluding current bar if not yet appended, 
        # BUT usually this checks 'bar' against 'hist' where 'bar' might be the NEW one.
        # Original code: prev_high = max(highs[:-1]) suggests hist contains current bar?
        # Let's check original usage: _update_bar_history is called distinct from _is_new_local_extreme.
        # Usually checking happens *before* or *after* update.
        # Original code used highs[:-1]. If hist HAS the new bar, this makes sense.
        # If hist DOES NOT have new bar, highs[:-1] skips the last OLD bar.
        # Safe approach: pass explicit history or assume policy.
        # We will assume hist DOES NOT contain 'bar' yet, or if it does, 
        # we strictly follow original logic which took highs[:-1].
        
        # NOTE: Original code (Step 16/106)
        # highs = [b.high for b in hist]
        # prev_high = max(highs[:-1])
        # This implies 'hist' DOES contain some recent bars.
        # If 'bar' is passed as argument, and 'hist' is self._bar_history...
        # In original code, update_bar_history is separate.
        # If update called BEFORE this check, then hist[-1] is 'bar'.
        # If called AFTER, then hist hasn't 'bar'.
        # max(highs[:-1]) implies comparing to EVERYTHING except the very last one in history.
        # So it implies hist contains the candidate?
        # Let's assume standard usage: update -> check.
        pass

        # Re-implementing logic blindly matching original:
        if len(highs) < 2:
             return False

        prev_high = max(highs[:-1])
        prev_low = min(lows[:-1])

        mu_vol = sum(vols[:-1]) / (len(vols) - 1)
        # Avoid division by zero
        n_var = max(len(vols) - 2, 1)
        var_vol = sum((v - mu_vol) ** 2 for v in vols[:-1]) / n_var
        std_vol = math.sqrt(max(var_vol, 1e-9))
        
        # bar.volume is from the new bar being checked
        vol_z = (bar.volume - mu_vol) / max(std_vol, 1e-6)

        is_new_high = (
            bar.high > prev_high + k_atr * atr_intraday and vol_z >= vol_z_thr
        )
        is_new_low = (
            bar.low < prev_low - k_atr * atr_intraday and vol_z >= vol_z_thr
        )

        return is_new_high or is_new_low
