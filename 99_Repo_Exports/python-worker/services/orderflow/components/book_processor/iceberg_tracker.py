import logging
from typing import Dict, Any

from services.orderflow.runtime import SymbolRuntime

logger = logging.getLogger("orderflow_iceberg_tracker")

class IcebergTracker:
    @staticmethod
    def update(runtime: SymbolRuntime, book_raw: Dict[str, Any], book_ts_ms: int) -> None:
        try:
            iceberg_event = runtime.iceberg_detector.push(book_raw)
            if iceberg_event:
                # B2: Check USD threshold (if configured)
                min_usd_ice = float(runtime.config.get("iceberg_refresh_min_notional_usd", 0.0) or 0.0)
                pass_ice = True
                if min_usd_ice > 1.0:
                    qty_ref = float(iceberg_event.get("total_refresh_qty", 0.0))
                    prc_ice = float(iceberg_event.get("price", 0.0))
                    val_usd = qty_ref * prc_ice
                    if val_usd < min_usd_ice:
                        pass_ice = False
                
                if pass_ice:
                    runtime.last_iceberg_event = {
                        "side": iceberg_event.get("side")
                        "refresh": iceberg_event.get("refresh")
                        "duration": iceberg_event.get("duration")
                        "price": iceberg_event.get("price")
                        "ts_ms": book_ts_ms
                        "total_refresh_qty": iceberg_event.get("total_refresh_qty", 0.0)
                    }
        except Exception:
            pass
