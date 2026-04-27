from typing import Dict, Any, Tuple
import os

class TickDQPolicy:
    """
    Centralized Data Quality policy for ticks and klines.
    Mirrors Liquidation DQ Policy (go-worker/internal/liquidation/dq.go).
    - Detects bad timestamps (bad_ts_unit/bad_ts)
    - Detects stale/future skew
    - Detects out-of-order events
    - Does NOT silently sanitize malformed data.
    """
    def __init__(self, 
                 max_event_age_ms: int = 10_000, 
                 max_future_skew_ms: int = 2_000,
                 max_out_of_order_ms: int = 2_000,
                 latency_lenient_mode: bool = False):
        
        # When True, we increase staleness budget (e.g. for klines)
        if latency_lenient_mode:
            self.max_event_age_ms = int(os.getenv("KLINE_DQ_MAX_AGE_MS", "65000")) 
        else:
            self.max_event_age_ms = int(os.getenv("TICK_DQ_MAX_AGE_MS", str(max_event_age_ms)))

        self.max_future_skew_ms = int(os.getenv("TICK_DQ_MAX_SKEW_MS", str(max_future_skew_ms)))
        self.max_out_of_order_ms = int(os.getenv("TICK_DQ_MAX_OOO_MS", str(max_out_of_order_ms)))
        
        self.last_ts_ms: Dict[str, int] = {}
        
    def validate(self, payload: Dict[str, Any], current_ts_ms: int) -> Tuple[bool, str]:
        """
        Validate tick or kline payload.
        Returns: (is_valid, reason)
        """
        symbol = str(payload.get("symbol") or payload.get("s") or "").upper()
        if not symbol:
            return False, "missing_symbol"
            
        # Extract raw ts (before heuristic mutation ideally)
        ts_val = payload.get("ts_ms") or payload.get("ts") or payload.get("event_time") or payload.get("E") or payload.get("time") or payload.get("written_at")
        if ts_val is None or ts_val is False:
            return False, "bad_ts"
            
        try:
            ts_ms = int(float(str(ts_val).strip()))
        except (ValueError, TypeError):
            return False, "bad_ts"

        if ts_ms <= 0:
            return False, "bad_ts"

        # Explicit check for bad units (likely seconds instead of ms): < 1e11
        if ts_ms < 100_000_000_000:
            return False, "bad_ts_unit"

        # Age and Skew
        if self.max_event_age_ms > 0:
            if current_ts_ms - ts_ms > self.max_event_age_ms:
                return False, "stale"
                
        if self.max_future_skew_ms > 0:
            if ts_ms - current_ts_ms > self.max_future_skew_ms:
                return False, "future_skew"
                
        # Out of order
        if self.max_out_of_order_ms > 0:
            last = self.last_ts_ms.get(symbol, 0)
            if ts_ms >= last:
                self.last_ts_ms[symbol] = ts_ms
                return True, "pass"
            
            # Allow small out-of-order window
            if last - ts_ms <= self.max_out_of_order_ms:
                return True, "pass"
                
            return False, "out_of_order"
            
        return True, "pass"
