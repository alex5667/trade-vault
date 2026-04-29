import logging
from typing import Any
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.configuration import _safe_int
from core.book_churn import compute_churn_from_z
from services.orderflow.metrics import book_rate_ema_gauge, book_rate_z_gauge

logger = logging.getLogger("orderflow_book_rate_tracker")

class BookRateTracker:
    @staticmethod
    def update(processor: Any, runtime: SymbolRuntime, book_ts_ms: int, prev_ts_ms: int) -> None:
        """
        Updates book rate EMA, calibration, Z-Score and churn metrics.
        """
        if book_ts_ms <= 0:
            return

        prev = _safe_int(prev_ts_ms)
        runtime.prev_book_ts_ms = _safe_int(prev)
        runtime.last_book_ts_ms = _safe_int(book_ts_ms)

        # P0 audit fix: propagate book_health from indicators into runtime so
        # that decision-snapshot reads real state.
        if not hasattr(runtime, "_book_health_initialized"):
            runtime.last_book_health_ok = 1
            runtime.last_book_health = "OK"
            runtime._book_health_initialized = True

        inst = 0.0
        if runtime.prev_book_ts_ms > 0 and book_ts_ms > runtime.prev_book_ts_ms:
            dt = book_ts_ms - runtime.prev_book_ts_ms
            inst = 1000.0 / float(max(1, dt))
            a = float(runtime.config.get("book_rate_ema_alpha", 0.2))
            runtime.book_rate_ema = a * inst + (1.0 - a) * float(runtime.book_rate_ema or 0.0)
            
            # Calibration
            try:
                rg = str(getattr(runtime, "last_regime", "na") or "na")
                runtime.br_calib.update(regime=rg, inst_hz=float(inst), dt_ms=int(dt))
            except Exception:
                pass
            
            # Z-Score
            try:
                runtime.book_rate_stats.update(float(inst))
                runtime.book_rate_z = float(runtime.book_rate_stats.z(float(inst)))
            except Exception:
                pass
        
        # Churn
        try:
            ch = compute_churn_from_z(
                rate_hz=float(inst), 
                rate_z=float(runtime.book_rate_z), 
                z_start=processor.book_churn_z_start, 
                z_full=processor.book_churn_z_full, 
                z_hi=processor.book_churn_z_hi
            )
            runtime.book_churn_score = float(ch.churn_score)
            runtime.book_churn_hi = int(ch.churn_hi)
            
            if book_rate_ema_gauge:
                book_rate_ema_gauge.labels(symbol=runtime.symbol).set(float(runtime.book_rate_ema))
            if book_rate_z_gauge:
                book_rate_z_gauge.labels(symbol=runtime.symbol).set(float(runtime.book_rate_z))
        except Exception:
            pass
