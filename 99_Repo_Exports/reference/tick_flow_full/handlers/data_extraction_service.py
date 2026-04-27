# data_extraction_service.py
"""
Data extraction functionality extracted from base_orderflow_handler.py
"""

from __future__ import annotations

from typing import Optional, Dict, Any, Tuple, List, Deque
from collections import deque
import time

from contexts import Tick, SimpleL2Snapshot, L2Level
# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)


class DataExtractionService:
    """
    Service for extracting and processing raw market data.
    """

    def __init__(self, symbol: str, *, enable_legacy_obi: bool = False):
        self.symbol = symbol
        self.logger = setup_logger(f"DataExtractionService:{symbol}")
        self.enable_legacy_obi = bool(enable_legacy_obi)

        # --- bucket tracking (explicit fields; no hasattr-mutation) ---
        self._current_bucket_id: Optional[int] = None
        self._bucket_sum: float = 0.0
        self._last_bucket_value: float = 0.0

        # --- OBI tracking ---
        # keep separate buffers for 5-window and 20-window (do not conflate)
        self._obi5_samples: Deque[float] = deque(maxlen=200)
        self._obi20_samples: Deque[float] = deque(maxlen=200)

        # OBI threshold (can be overridden by env/config if you want later)
        self._obi_thr: float = 0.10

        # How many last samples must exceed threshold to be "sustained"
        self._obi_sustain_k5: int = 5
        self._obi_sustain_k20: int = 10  # stricter for longer window

    def _extract_top1(self, x: Any) -> Tuple[float, float]:
        """Extract top level (price, size) from book data."""
        if not x:
            return 0.0, 0.0

        # Handle different data formats
        if isinstance(x, (list, tuple)) and len(x) >= 2:
            try:
                return float(x[0]), float(x[1])
            except (ValueError, TypeError):
                return 0.0, 0.0
        elif isinstance(x, dict):
            try:
                price = float(x.get("p") or x.get("price") or 0.0)
                size = float(x.get("q") or x.get("qty") or x.get("size") or 0.0)
                return price, size
            except (ValueError, TypeError):
                return 0.0, 0.0

        return 0.0, 0.0

    def _extract_top_levels(self, book_data: Dict[str, Any], side: str, n: int = 3) -> List[Tuple[float, float]]:
        """Extract top N levels from book data for specified side."""
        # Try different key variations
        keys = [side]
        if side == "bids":
            keys += ["b", "bid", "BIDS"]
        else:
            keys += ["a", "ask", "ASKS"]

        arr = None
        for k in keys:
            if k in book_data:
                arr = book_data.get(k)
                break

        if not arr or not isinstance(arr, list):
            return []

        out = []
        for i in range(min(n, len(arr))):
            lvl = arr[i]
            price, size = self._extract_top1(lvl)
            if price > 0 and size >= 0:
                out.append((price, size))

        return out

    def _classify_delta(self, tick: Tick) -> float:
        """Classify tick delta based on trade direction."""
        # Binance semantics:
        #   is_buyer_maker == True  -> buyer is maker -> taker is SELL -> delta negative
        #   is_buyer_maker == False -> taker BUY -> delta positive
        bm = getattr(tick, "is_buyer_maker", None)
        if bm is None:
            return 0.0
        sign = -1.0 if bool(bm) else 1.0
        vol = float(getattr(tick, "volume", 0.0) or 0.0)
        if vol <= 0.0:
            # do not inject phantom delta
            return 0.0
        return sign * vol

    def _taker_side(self, tick: Tick) -> int:
        """Determine taker side from tick."""
        bm = getattr(tick, "is_buyer_maker", None)
        if bm is None:
            return 0
        # buyer is maker -> taker sell -> -1
        return -1 if bool(bm) else 1

    def _feed_delta_bucket(self, delta: float, ts: int, bucket_ms: int, max_zero_buckets: int) -> Optional[int]:
        """
        Bucketization by timestamp.
        Returns bucket ID if closed, None if still open.
        """
        bucket_id = ts // bucket_ms

        if self._current_bucket_id is None:
            self._current_bucket_id = int(bucket_id)
            self._bucket_sum = float(delta)
            self._last_bucket_value = 0.0
            return None

        if bucket_id != self._current_bucket_id:
            # Bucket complete
            closed_id = int(self._current_bucket_id)

            # Save previous bucket value
            self._last_bucket_value = self._bucket_sum

            # Check for gaps and fill with zeros
            gap = bucket_id - self._current_bucket_id - 1
            if gap > 0:
                # Fill gaps with zero buckets (limited)
                zero_fill_count = min(gap, max_zero_buckets)
                for _ in range(zero_fill_count):
                    # Could add zero buckets to window here if needed
                    pass

            # Start new bucket
            self._current_bucket_id = int(bucket_id)
            self._bucket_sum = float(delta)
            return closed_id

        # Continue accumulating in current bucket
        self._bucket_sum += delta
        return None

    def _obi_sustained_eval(self, samples: List[float], thr: float) -> Tuple[float, bool]:
        """Evaluate OBI sustainability."""
        if not samples:
            return 0.0, False

        current_obi = samples[-1] if samples else 0.0
        sustained = all(abs(s) >= thr for s in samples)

        return current_obi, sustained

    def _track_obi(self, ts: int, obi5: float, obi20: float) -> None:
        """Track OBI for sustainability evaluation."""
        if not self.enable_legacy_obi:
            return
        # Keep both, do not conflate
        if obi5 is not None:
            self._obi5_samples.append(float(obi5))
        if obi20 is not None:
            self._obi20_samples.append(float(obi20))

    def _get_obi(self, ts: int) -> Tuple[float, float, bool, float, float, bool]:
        """Get current OBI state."""
        if not self.enable_legacy_obi:
            # disabled path: force "invalid"
            return 0.0, 0.0, False, 0.0, 0.0, True
        s5 = list(self._obi5_samples)
        s20 = list(self._obi20_samples)

        obi5_avg = (sum(s5[-5:]) / max(len(s5[-5:]), 1)) if s5 else 0.0
        obi20_avg = (sum(s20[-20:]) / max(len(s20[-20:]), 1)) if s20 else 0.0

        # Evaluate sustainability
        thr = float(self._obi_thr)
        k5 = int(self._obi_sustain_k5)
        k20 = int(self._obi_sustain_k20)
        _, obi5_sustained = self._obi_sustained_eval(s5[-k5:], thr)
        _, obi20_sustained = self._obi_sustained_eval(s20[-k20:], thr)

        current_obi = s5[-1] if s5 else 0.0

        return current_obi, obi5_avg, obi5_sustained, obi20_avg, obi20_sustained, False

    def _calc_obi_surrogate(self) -> float:
        """Calculate OBI surrogate when direct calculation is not available."""
        # Simple surrogate based on recent deltas
        # This would need access to delta window from data processor
        return 0.0

    def _is_trade_tick(self, tick: Tick) -> bool:
        """Check if tick represents a trade."""
        flags = int(getattr(tick, "flags", 0) or 0)
        vol = float(getattr(tick, "volume", 0.0) or 0.0)
        return ((flags & 1) == 1) or (vol > 0.0)

    def _l3_on_tick_trade(self, tick: Tick, signed_delta: float) -> None:
        """
        Hook: update L3LiteTracker on trade ticks.
        Default no-op, crypto handler overrides.
        """
        return

    def get_obi_state(self) -> Dict[str, Any]:
        """Get current OBI state summary."""
        current_obi, obi5_avg, obi5_sustained, obi20_avg, obi20_sustained, _ = self._get_obi(int(time.time() * 1000))

        return {
            'current_obi': current_obi,
            'obi5_avg': obi5_avg,
            'obi5_sustained': obi5_sustained,
            'obi20_avg': obi20_avg,
            'obi20_sustained': obi20_sustained,
            'sample_count_obi5': len(self._obi5_samples),
            'sample_count_obi20': len(self._obi20_samples),
        }
