import logging

from core.lob_pressure import compute_lob_pressure
from services.orderflow.metrics import (
    log_silent_error,
    of_lob_depth_convexity_gauge,
    of_lob_depth_slope_gauge,
    of_lob_dw_obi_gauge,
    of_lob_dw_obi_stability_score_gauge,
    of_lob_dw_obi_stable_gauge,
    of_lob_dw_obi_stable_secs_gauge,
    of_lob_dw_obi_z_gauge,
    of_lob_micro_mid_div_bps_gauge,
    of_lob_micro_shift_bps_gauge,
    of_lob_queue_imbalance_gauge,
    of_lob_queue_imbalance_max_abs_gauge,
    of_lob_queue_imbalance_mean_gauge,
    of_lob_queue_imbalance_slope_gauge,
)
from services.orderflow.runtime import BookSnapshot, SymbolRuntime

logger = logging.getLogger("orderflow_lob_pressure_tracker")

class LOBPressureTracker:
    @staticmethod
    def update(runtime: SymbolRuntime, snap: BookSnapshot, prev_snap: BookSnapshot, book_ts_ms: int) -> None:
        try:
            lp = compute_lob_pressure(
                bids=list(snap.top5_bids or []),
                asks=list(snap.top5_asks or []),
                prev_bids=list(prev_snap.top5_bids) if prev_snap else None,
                prev_asks=list(prev_snap.top5_asks) if prev_snap else None,
                depth=int(runtime.config.get("lob_depth", 5) or 5),
            )

            # Store aggregates on runtime for tick_processor to read
            runtime.lob_qi_mean = float(lp.get("qi_mean", 0.0) or 0.0)
            runtime.lob_qi_max_abs = float(lp.get("qi_max_abs", 0.0) or 0.0)
            runtime.lob_qi_slope = float(lp.get("qi_slope", 0.0) or 0.0)

            runtime.lob_micro_mid_div_bps = float(lp.get("micro_mid_div_bps", 0.0) or 0.0)
            runtime.lob_micro_shift_bps = float(lp.get("micro_shift_bps", 0.0) or 0.0)

            runtime.lob_depth_slope_bid = float(lp.get("depth_slope_bid", 0.0) or 0.0)
            runtime.lob_depth_slope_ask = float(lp.get("depth_slope_ask", 0.0) or 0.0)
            runtime.lob_depth_slope_imb = float(lp.get("depth_slope_imb", 0.0) or 0.0)

            runtime.lob_depth_convexity_bid = float(lp.get("depth_convexity_bid", 0.0) or 0.0)
            runtime.lob_depth_convexity_ask = float(lp.get("depth_convexity_ask", 0.0) or 0.0)
            runtime.lob_depth_convexity_imb = float(lp.get("depth_convexity_imb", 0.0) or 0.0)

            runtime.lob_dw_obi = float(lp.get("dw_obi", 0.0) or 0.0)

            # dw_obi robust z-score (deterministic, fail-open)
            try:
                runtime.dw_obi_stats.update(float(runtime.lob_dw_obi))
                runtime.dw_obi_z = float(runtime.dw_obi_stats.z(float(runtime.lob_dw_obi)))
            except Exception:
                runtime.dw_obi_z = 0.0

            # dw_obi stability tracking via OBIStabilityTracker (deterministic, fail-open)
            try:
                q, secs = runtime.dw_obi_tracker.update(
                    ts_ms=int(book_ts_ms), obi=float(runtime.lob_dw_obi)
                )
                runtime.dw_obi_stability_score = float(q)
                runtime.dw_obi_stable_secs = float(secs)
                min_secs = float(runtime.config.get("dw_obi_stable_min_secs", 1.5) or 1.5)
                min_q = float(runtime.config.get("dw_obi_stable_score_min", 0.60) or 0.60)
                runtime.dw_obi_stable = bool((secs >= min_secs) and (q >= min_q))
            except Exception:
                runtime.dw_obi_stability_score = 0.0
                runtime.dw_obi_stable_secs = 0.0
                runtime.dw_obi_stable = False

            # Atomic snapshot dict (consumed by tick_processor indicators)
            runtime.last_lob_event = {
                "ts_ms": int(book_ts_ms),
                **{k: float(v) for k, v in lp.items() if isinstance(v, (int, float))},
                "dw_obi_z": float(getattr(runtime, "dw_obi_z", 0.0) or 0.0),
                "dw_obi_stability_score": float(getattr(runtime, "dw_obi_stability_score", 0.0) or 0.0),
                "dw_obi_stable_secs": float(getattr(runtime, "dw_obi_stable_secs", 0.0) or 0.0),
                "dw_obi_stable": 1 if bool(getattr(runtime, "dw_obi_stable", False)) else 0,
            }

            # Prometheus gauges (best-effort, never break book pipeline)
            sym = str(runtime.symbol)
            try:
                for i in range(1, 6):
                    lv = f"L{i}"
                    qv = float(lp.get(f"qi_l{i}", 0.0) or 0.0)
                    if of_lob_queue_imbalance_gauge:
                        of_lob_queue_imbalance_gauge.labels(symbol=sym, level=lv).set(qv)
                if of_lob_queue_imbalance_mean_gauge:
                    of_lob_queue_imbalance_mean_gauge.labels(symbol=sym).set(float(runtime.lob_qi_mean))
                if of_lob_queue_imbalance_max_abs_gauge:
                    of_lob_queue_imbalance_max_abs_gauge.labels(symbol=sym).set(float(runtime.lob_qi_max_abs))
                if of_lob_queue_imbalance_slope_gauge:
                    of_lob_queue_imbalance_slope_gauge.labels(symbol=sym).set(float(runtime.lob_qi_slope))

                if of_lob_micro_mid_div_bps_gauge:
                    of_lob_micro_mid_div_bps_gauge.labels(symbol=sym).set(float(runtime.lob_micro_mid_div_bps))
                if of_lob_micro_shift_bps_gauge:
                    of_lob_micro_shift_bps_gauge.labels(symbol=sym).set(float(runtime.lob_micro_shift_bps))

                if of_lob_depth_slope_gauge:
                    of_lob_depth_slope_gauge.labels(symbol=sym, side="bid").set(float(runtime.lob_depth_slope_bid))
                    of_lob_depth_slope_gauge.labels(symbol=sym, side="ask").set(float(runtime.lob_depth_slope_ask))
                    of_lob_depth_slope_gauge.labels(symbol=sym, side="imb").set(float(runtime.lob_depth_slope_imb))
                if of_lob_depth_convexity_gauge:
                    of_lob_depth_convexity_gauge.labels(symbol=sym, side="bid").set(float(runtime.lob_depth_convexity_bid))
                    of_lob_depth_convexity_gauge.labels(symbol=sym, side="ask").set(float(runtime.lob_depth_convexity_ask))
                    of_lob_depth_convexity_gauge.labels(symbol=sym, side="imb").set(float(runtime.lob_depth_convexity_imb))

                if of_lob_dw_obi_gauge:
                    of_lob_dw_obi_gauge.labels(symbol=sym).set(float(runtime.lob_dw_obi))
                if of_lob_dw_obi_z_gauge:
                    of_lob_dw_obi_z_gauge.labels(symbol=sym).set(float(runtime.dw_obi_z))
                if of_lob_dw_obi_stability_score_gauge:
                    of_lob_dw_obi_stability_score_gauge.labels(symbol=sym).set(float(runtime.dw_obi_stability_score))
                if of_lob_dw_obi_stable_secs_gauge:
                    of_lob_dw_obi_stable_secs_gauge.labels(symbol=sym).set(float(runtime.dw_obi_stable_secs))
                if of_lob_dw_obi_stable_gauge:
                    of_lob_dw_obi_stable_gauge.labels(symbol=sym).set(1.0 if bool(runtime.dw_obi_stable) else 0.0)
            except Exception:
                pass  # metrics never break the pipeline

        except Exception as exc:
            log_silent_error(exc, "lob_pressure_failure", runtime.symbol, "LOBPressureTracker:update")
