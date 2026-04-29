import logging
from typing import Dict, Any

from services.orderflow.runtime import SymbolRuntime

logger = logging.getLogger("orderflow_obi_tracker")

class OBITracker:
    @staticmethod
    def update(runtime: SymbolRuntime, book_raw: Dict[str, Any], book_ts_ms: int) -> None:
        try:
            obi_event = runtime.obi_detector.push(book_raw)
            if obi_event:
                try:
                    obi_val = float(obi_event.get("obi", 0.0) or 0.0)
                    q, secs = runtime.obi_tracker.update(ts_ms=book_ts_ms, obi=obi_val)
                    runtime.obi_stability_score = float(q)
                    runtime.obi_stable_secs = float(secs)
                    min_secs = float(runtime.config.get("obi_stable_min_secs", 1.0) or 1.5)
                    min_q = float(runtime.config.get("obi_stable_score_min", 0.60) or 0.60)
                    runtime.obi_stable = bool((secs >= min_secs) and (q >= min_q))
                except Exception:
                    pass
                runtime.last_obi_event = {
                    "direction": obi_event.get("direction"),
                    "obi": obi_event.get("obi"),
                    "ts_ms": book_ts_ms,
                    "stable_secs": float(getattr(runtime, "obi_stable_secs", 0.0) or 0.0),
                    "stability_score": float(getattr(runtime, "obi_stability_score", 0.0) or 0.0),
                    "obi_z": float(obi_event.get("obi_z", 0.0) or 0.0),
                    "stacking": float(obi_event.get("stacking", 0.0) or 0.0),
                    "concentration": float(obi_event.get("concentration", 0.0) or 0.0),
                }

            # OBI fallback: feed obi_tracker unconditionally from raw BBO imbalance.
            # OBIDetector fires only when |obi| >= threshold (0.4) — too rare in range regime.
            # OBIStabilityTracker with lower threshold (0.25) tracks persistence continuously.
            # When stable, synthesize last_obi_event so compute_obi_flags() can use it.
            if not obi_event:
                try:
                    bids_raw = book_raw.get("bids") or []
                    asks_raw = book_raw.get("asks") or []
                    if bids_raw and asks_raw:
                        bid_vol = sum(float(lv[1]) for lv in bids_raw[:5])
                        ask_vol = sum(float(lv[1]) for lv in asks_raw[:5])
                        tot = bid_vol + ask_vol
                        if tot > 0:
                            raw_obi = (bid_vol - ask_vol) / tot
                            q, secs = runtime.obi_tracker.update(ts_ms=book_ts_ms, obi=raw_obi)
                            runtime.obi_stability_score = float(q)
                            runtime.obi_stable_secs = float(secs)
                            min_secs = float(runtime.config.get("obi_stable_min_secs", 1.0) or 1.5)
                            min_q = float(runtime.config.get("obi_stable_score_min", 0.60) or 0.60)
                            runtime.obi_stable = bool((secs >= min_secs) and (q >= min_q))
                            # Synthesize last_obi_event when stable
                            if runtime.obi_stable:
                                direction = "LONG" if raw_obi > 0 else "SHORT"
                                runtime.last_obi_event = {
                                    "direction": direction,
                                    "obi": float(raw_obi),
                                    "ts_ms": book_ts_ms,
                                    "stable_secs": float(secs),
                                    "stability_score": float(q),
                                    "obi_z": float(getattr(runtime, "dw_obi_z", 0.0) or 0.0),
                                    "stacking": 0.0,
                                    "concentration": 0.0,
                                }
                except Exception:
                    pass
        except Exception:
            pass
