from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from services.orderflow.runtime import SymbolRuntime, BookSnapshot, BookState
from services.orderflow.configuration import _safe_int, _safe_float
from services.orderflow.metrics import (
    log_silent_error, book_missing_seq_events_total, book_missing_seq_ema_gauge, book_rate_ema_gauge, book_rate_z_gauge,
    of_lob_queue_imbalance_gauge, of_lob_queue_imbalance_mean_gauge,
    of_lob_queue_imbalance_max_abs_gauge, of_lob_queue_imbalance_slope_gauge,
    of_lob_micro_mid_div_bps_gauge, of_lob_micro_shift_bps_gauge,
    of_lob_depth_slope_gauge, of_lob_depth_convexity_gauge,
    of_lob_dw_obi_gauge, of_lob_dw_obi_z_gauge, of_lob_dw_obi_stability_score_gauge,
    of_lob_dw_obi_stable_secs_gauge, of_lob_dw_obi_stable_gauge,
)

# P112: minimal DQ/book-seq metrics live in a dedicated module to avoid
# duplicate metric registration across SoT/mirror import paths.
from services.orderflow.metrics_bookseq_dq_p112 import (
    book_missing_seq_ema_gauge,
    book_seq_last_gap_gauge,
)

# P112: minimal DQ/book-seq metrics live in a dedicated module to avoid
# duplicate metric registration across SoT/mirror import paths.
from services.orderflow.metrics_bookseq_dq_p112 import (
    book_missing_seq_ema_gauge,
    book_seq_last_gap_gauge,
)
from services.orderflow.utils import _fields_to_dict
from services.orderflow.components.parsing import OrderFlowParsing

# P5: book sanity + stream integrity
from services.orderflow.book_sanity import check_book_sanity
from services.orderflow.metrics_stream_integrity_p5 import emit_integrity_metrics
from services.orderflow.metrics_book_sanity_p5 import book_crossed_total, book_sanity_flags_total
from core.book_churn import compute_churn_from_z
from core.lob_pressure import compute_lob_pressure  # LOB pressure features (P91)
from core.ofi_tracker import OFIEvent

# GPU L2 processor — lazy import, no hard dependency
try:
    from gpu.l2_processor import L2GPUProcessor as _L2GPU
    _L2GPU_AVAILABLE = True
except ImportError:
    _L2GPU_AVAILABLE = False

logger = logging.getLogger("orderflow_book_processor")

# Per-symbol GPU processor cache (created on first use)
_l2gpu_cache: Dict[str, Any] = {}

class BookProcessor:
    """
    Handles book snapshot processing:
    1. Parsing (delegated)
    2. State Update (BookSnapshot/BookState)
    3. Metrics (Book Rate, Churn)
    4. Detectors Feed (OBI, Iceberg, OFI)
    5. Liquidity Scoring
    """
    def __init__(self, book_churn_z_start: float = 2.0, book_churn_z_full: float = 5.0, book_churn_z_hi: float = 4.0):
        self.book_churn_z_start = book_churn_z_start
        self.book_churn_z_full = book_churn_z_full
        self.book_churn_z_hi = book_churn_z_hi


    def _update_book_missing_seq(self, runtime: SymbolRuntime, book_raw: Dict[str, Any]) -> None:
        """Update runtime.book_missing_seq_ema using Binance depthUpdate continuity.

        The plan requires this telemetry to be:
          - bounded (EMA in [0..1])
          - deterministic
          - symmetric with tick_* analogs (surface reason + EMA to indicators for DQ gate)

        Two continuity modes are supported:
          1) Strict U/u mode (preferred): detects true gaps even with overlapping ranges.
          2) Fallback u-only mode: when parser only provides `u` (no `U`).

        Notes:
        - Partial depth snapshots (@depth5/@depth10/@depth20) usually do not include U/u.
          In that case we keep book_missing_seq_ema unchanged and set reason=no_seq_fields.
        - Duplicates / reorder / resets do not count as missing updates.
        """

        # Local import to reduce patch conflict risk between SoT/mirror trees.
        try:
            from services.orderflow.components.book_seq_tracker_uu import (
                decide_book_seq_uu, ema_update_clamped, resolve_book_seq_ema_alpha,
            )
        except Exception:
            from .book_seq_tracker_uu import decide_book_seq_uu, ema_update_clamped, resolve_book_seq_ema_alpha

        cfg = getattr(runtime, "config", None) or {}

        # Alpha must be consistent with DQ gate thresholds (train==serve contract).
        # Key name per rollout plan: dq_book_seq_ema_alpha.
        alpha = float(resolve_book_seq_ema_alpha(cfg))
        runtime.dq_book_seq_ema_alpha = alpha  # exposed for runbook/debug

        cur_u = _safe_int(book_raw.get("u") or 0)
        cur_U = _safe_int(book_raw.get("U") or 0)

        prev_u = _safe_int(getattr(runtime, "book_seq_last_u", 0) or 0)
        prev_ema = _safe_float(getattr(runtime, "book_missing_seq_ema", 0.0) or 0.0)

        # Default outputs
        reason = "init"
        miss_event = 0.0
        gap = 0
        next_last_u = prev_u

        if cur_u <= 0:
            reason = "no_u"

        elif cur_U > 0:
            # Preferred strict continuity when both U/u are available.
            dec = decide_book_seq_uu(prev_u=prev_u, cur_U=cur_U, cur_u=cur_u)
            reason = str(dec.reason)
            miss_event = float(dec.missing_event)
            gap = int(dec.gap)
            next_last_u = int(dec.next_last_u)

        else:
            # Fallback continuity when only `u` is present.
            # delta = u - last_u:
            #   1 -> ok
            #   >1 -> gap
            #   0 -> dup
            #   <0 -> reorder
            if prev_u <= 0:
                reason = "init"
                next_last_u = cur_u
            else:
                delta = int(cur_u - prev_u)
                if delta == 1:
                    reason = "ok"
                    next_last_u = cur_u
                elif delta > 1:
                    reason = "gap"
                    gap = int(delta - 1)
                    miss_event = 1.0
                    next_last_u = cur_u
                elif delta == 0:
                    reason = "dup"
                else:
                    reason = "reorder"

        # Runbook-friendly diagnostics (always set, even during warmup).
        runtime.book_missing_seq_last_gap = int(gap)
        runtime.book_seq_last_reason = str(reason)

        # Optional counters (safe defaults).
        if reason == "gap":
            runtime.book_seq_gap_count = _safe_int(getattr(runtime, "book_seq_gap_count", 0) or 0) + 1
        elif reason == "dup":
            runtime.book_seq_dup_count = _safe_int(getattr(runtime, "book_seq_dup_count", 0) or 0) + 1
        elif reason == "reorder":
            runtime.book_seq_reorder_count = _safe_int(getattr(runtime, "book_seq_reorder_count", 0) or 0) + 1

        # EMA update:
        # - init/no_seq_fields/no_u: keep unchanged to avoid false positives on startup or partial depth.
        # - ok/overlap/dup/reorder: update with x=0.0 (decay towards 0)
        # - gap: update with x=1.0
        if reason in ("ok", "overlap", "gap", "dup", "reorder"):
            runtime.book_missing_seq_ema = float(ema_update_clamped(prev_ema, miss_event, alpha))
        else:
            runtime.book_missing_seq_ema = float(prev_ema)

        # Prom counter: one increment per detected gap event.
        if reason == "gap":
            try:
                book_missing_seq_events_total.labels(symbol=str(runtime.symbol)).inc()
            except Exception:
                pass

        # Prom gauges: always set to keep dashboards stable.
        try:
            if book_missing_seq_ema_gauge is not None:
                book_missing_seq_ema_gauge.labels(symbol=str(runtime.symbol)).set(float(runtime.book_missing_seq_ema))
            if book_seq_last_gap_gauge is not None:
                book_seq_last_gap_gauge.labels(symbol=str(runtime.symbol)).set(float(getattr(runtime, "book_missing_seq_last_gap", 0) or 0))
        except Exception:
            # Metrics are fail-open by design.
            pass

        # Prom gauges: always set to keep dashboards stable.
        try:
            if book_missing_seq_ema_gauge is not None:
                book_missing_seq_ema_gauge.labels(symbol=str(runtime.symbol)).set(float(runtime.book_missing_seq_ema))
            if book_seq_last_gap_gauge is not None:
                book_seq_last_gap_gauge.labels(symbol=str(runtime.symbol)).set(float(getattr(runtime, "book_missing_seq_last_gap", 0) or 0))
        except Exception:
            runtime.book_missing_seq_ema = float(prev_ema)

        # Export EMA to Prometheus at book rate as well (not only on ticks).
        # This avoids staleness when book stream is live but tick stream is quiet.
        try:
            book_missing_seq_ema_gauge.labels(symbol=str(runtime.symbol)).set(float(runtime.book_missing_seq_ema))
        except Exception:
            pass

        # Advance last_u only when monotonic; this is robust against duplicates / reorders.
        if next_last_u > prev_u:
            runtime.book_seq_last_u = int(next_last_u)

    def process_book(self, runtime: SymbolRuntime, payload: Dict[str, Any], ingest_ts_ms: int) -> bool:
        """
        Processes a raw book payload from Redis stream.
        Returns True if processed successfully, False otherwise.
        """
        try:
            # 1. Parsing
            raw = _fields_to_dict(payload)
            book_raw = OrderFlowParsing.parse_book_payload(raw, runtime.symbol)
            if not book_raw:
                return False

            # 2. Build Typed Snapshot
            prev_snap = getattr(runtime, "last_book", None)
            prev_ts_ms = _safe_int(getattr(runtime, "last_book_ts_ms", 0) or 0)
            snap = BookSnapshot.from_raw(book_raw)

            # Basic timestamps
            book_ts_ms = _safe_int(book_raw.get("ts_ms") or book_raw.get("ts") or book_raw.get("timestamp") or 0)

            # -------------------------------------------------------------
            # P5: Stream integrity + schema drift (book stream)
            #
            # We treat `u` (final update id) as the sequence when present.
            # This is a *telemetry* layer and must be fail-open.
            # -------------------------------------------------------------
            try:
                seq = _safe_int(book_raw.get("u") or book_raw.get("final_id") or 0)
                # schema hash drift: hash only top-level keys of book_raw
                runtime.book_integrity.update_schema(book_raw.keys())
                if seq > 0 and book_ts_ms > 0:
                    snap_i = runtime.book_integrity.update_seq(seq=seq, ts_ms=int(book_ts_ms))
                    emit_integrity_metrics(symbol=str(runtime.symbol), stream="book", snap=snap_i)
            except Exception:
                pass

            # -------------------------------------------------------------
            # P5: Book sanity (crossed BBO / NaNs / negative qty)
            # The gate decision is taken later (BookSanityGate), here we only
            # annotate runtime and emit metrics.
            # -------------------------------------------------------------
            try:
                bs = check_book_sanity(book=snap)
                runtime.book_sanity_ok = int(1 if bs.ok else 0)
                runtime.book_sanity_flags = ",".join(bs.flags)
                if not bs.ok:
                    try:
                        if book_sanity_flags_total is not None:
                            book_sanity_flags_total.labels(symbol=str(runtime.symbol)).inc()
                    except Exception:
                        pass
                if "crossed_bbo" in bs.flags:
                    try:
                        if book_crossed_total is not None:
                            book_crossed_total.labels(symbol=str(runtime.symbol)).inc()
                    except Exception:
                        pass
            except Exception:
                pass

            # Strict DQ: book missing-seq continuity (Binance depthUpdate U/u)
            # Fail-open: never break book processing on DQ telemetry.
            try:
                self._update_book_missing_seq(runtime, book_raw)
            except Exception:
                pass

            # Atomic Snapshot
            try:
                runtime.book_state = BookState(
                    raw=book_raw,
                    snap=snap,
                    prev_snap=prev_snap,
                    ts_ms=_safe_int(book_ts_ms),
                    prev_ts_ms=_safe_int(prev_ts_ms),
                    ingest_ts_ms=_safe_int(ingest_ts_ms),
                )
            except Exception as exc:
                log_silent_error(exc, 'book_state_failure', runtime.symbol, 'BookProcessor:book_state')

            # Backward compatibility
            runtime.last_book_raw = book_raw
            runtime.prev_book = prev_snap
            runtime.last_book = snap

            # 2b. LOB pressure features (P91) — queue imbalance / microprice / slope / dw_obi
            # Fail-open: any exception here must NOT stop the book processing pipeline.
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
                    min_q = float(runtime.config.get("dw_obi_stability_min_score", 0.60) or 0.60)
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
                log_silent_error(exc, "lob_pressure_failure", runtime.symbol, "BookProcessor:lob_pressure")

            # 3. Book Rate & Churn Metrics
            if book_ts_ms > 0:
                prev = _safe_int(prev_ts_ms)
                runtime.prev_book_ts_ms = _safe_int(prev)
                runtime.last_book_ts_ms = _safe_int(book_ts_ms)
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
                        z_start=self.book_churn_z_start, 
                        z_full=self.book_churn_z_full, 
                        z_hi=self.book_churn_z_hi
                    )
                    runtime.book_churn_score = float(ch.churn_score)
                    runtime.book_churn_hi = int(ch.churn_hi)
                    
                    if book_rate_ema_gauge:
                        book_rate_ema_gauge.labels(symbol=runtime.symbol).set(float(runtime.book_rate_ema))
                    if book_rate_z_gauge:
                        book_rate_z_gauge.labels(symbol=runtime.symbol).set(float(runtime.book_rate_z))
                except Exception:
                    pass
            else:
                 # If missing ts, treat as now for lag checks? No, just skip rate
                 pass

            # 4. Detectors Feed
            
            # OBI
            obi_event = runtime.obi_detector.push(book_raw)
            if obi_event:
                try:
                    obi_val = float(obi_event.get("obi", 0.0) or 0.0)
                    q, secs = runtime.obi_tracker.update(ts_ms=book_ts_ms, obi=obi_val)
                    runtime.obi_stability_score = float(q)
                    runtime.obi_stable_secs = float(secs)
                    min_secs = float(runtime.config.get("obi_stable_min_secs", 1.5) or 1.5)
                    min_q = float(runtime.config.get("obi_stability_min_score", 0.60) or 0.60)
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

            # Iceberg
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
                        "side": iceberg_event.get("side"),
                        "refresh": iceberg_event.get("refresh"),
                        "duration": iceberg_event.get("duration"),
                        "price": iceberg_event.get("price"),
                        "ts_ms": book_ts_ms,
                        "total_refresh_qty": iceberg_event.get("total_refresh_qty", 0.0),
                    }
            
            # OFI (requires prev_snap)
            self._update_ofi(runtime, snap, prev_snap, book_ts_ms, book_raw)

            # Liquidity Regime
            self._update_liquidity(runtime, book_ts_ms, book_raw)

            # L3 Lite (Book Totals)
            try:
                bid_tot = sum(float(lv[1]) for lv in book_raw.get("bids", []))
                ask_tot = sum(float(lv[1]) for lv in book_raw.get("asks", []))
                runtime.l3_queue.on_l2_totals(bid_total=bid_tot, ask_total=ask_tot)
            except Exception:
                pass

            # Phase E / P4: book message rate tracking (book_update_rate_hz/z, OTR denominator)
            # Fail-open: never break book processing on tracker error.
            try:
                if book_ts_ms > 0 and getattr(runtime, "msg_rate", None) is not None:
                    runtime.msg_rate.on_book_msg(int(book_ts_ms))
                    # Push cancel EMA from L3-lite if available (cancel_rate_z for OTR/quote-stuffing)
                    l3 = getattr(runtime, "l3_stats", None)
                    if l3 is not None:
                        try:
                            # L3LiteTracker.snap has cancel_bid_rate_ema and cancel_ask_rate_ema
                            sn = getattr(l3, "snap", None)
                            if sn is not None:
                                c_bid = float(getattr(sn, "cancel_bid_rate_ema", 0.0) or 0.0)
                                c_ask = float(getattr(sn, "cancel_ask_rate_ema", 0.0) or 0.0)
                                runtime.msg_rate.observe_cancel_rate_ema(max(c_bid, c_ask))
                        except Exception:
                            pass
                    # Sync cached outputs into runtime fields for fast reads by tick_processor
                    runtime.book_update_rate_hz = float(runtime.msg_rate.book_update_rate_hz)
                    runtime.book_update_rate_z = float(runtime.msg_rate.book_update_rate_z)
                    runtime.trade_msg_rate_hz = float(runtime.msg_rate.trade_msg_rate_hz)
                    runtime.trade_msg_rate_z = float(runtime.msg_rate.trade_msg_rate_z)
                    runtime.cancel_rate_z = float(runtime.msg_rate.cancel_rate_z)
                    runtime.otr = float(runtime.msg_rate.otr)
                    runtime.otr_z = float(runtime.msg_rate.otr_z)
            except Exception:
                pass

            # Phase E / P4: manipulation pattern update (quote stuffing score + layering score)
            # Uses pre-committed book snapshot spread depth and msg_rate z-scores.
            try:
                if getattr(runtime, "manip", None) is not None:
                    mid = float(getattr(runtime, "last_book_mid", 0.0) or 0.0)
                    bid_d = float(getattr(runtime, "last_depth_bid_5", 0.0) or 0.0) * max(mid, 1.0)
                    ask_d = float(getattr(runtime, "last_depth_ask_5", 0.0) or 0.0) * max(mid, 1.0)
                    runtime.manip.update_from_book(
                        ts_ms=int(book_ts_ms),
                        bid_depth_usd=bid_d,
                        ask_depth_usd=ask_d,
                        book_update_rate_z=float(getattr(runtime, "book_update_rate_z", 0.0)),
                        cancel_rate_z=float(getattr(runtime, "cancel_rate_z", 0.0)),
                        trade_msg_rate_hz=float(getattr(runtime, "trade_msg_rate_hz", 0.0)),
                        mid_px=mid,
                    )
                    runtime.quote_stuffing_score = float(runtime.manip.quote_stuffing_score)
                    runtime.layering_score = float(runtime.manip.layering_score)
                    runtime.manip_flags = str(runtime.manip.manip_flags)
            except Exception:
                pass

            # GPU L2 Microstructure (spread, imbalance, microprice, walls)
            self._update_l2_gpu(runtime, book_raw)

            return True

        except Exception as exc:
            log_silent_error(exc, 'book_process_failure', runtime.symbol, 'BookProcessor:process_book')
            return False

    def _update_l2_gpu(self, runtime: SymbolRuntime, book_raw: Dict[str, Any]) -> None:
        """Run GPU-accelerated L2 microstructure computation.

        Computes spread, depth imbalance, microprice, and liquidity walls
        on the full L2 book using CuPy. Results are stored on runtime.
        Falls back silently if GPU is unavailable or book data is missing.
        """
        if not _L2GPU_AVAILABLE:
            return
        try:
            bids_raw = book_raw.get("bids") or []
            asks_raw = book_raw.get("asks") or []
            if len(bids_raw) < 3 or len(asks_raw) < 3:
                return  # Too small — GPU transfer overhead not worth it

            sym = runtime.symbol
            if sym not in _l2gpu_cache:
                _l2gpu_cache[sym] = _L2GPU(sym, batch_size=1000)
            proc: _L2GPU = _l2gpu_cache[sym]

            bids = [(float(lv[0]), float(lv[1])) for lv in bids_raw if len(lv) >= 2]
            asks = [(float(lv[0]), float(lv[1])) for lv in asks_raw if len(lv) >= 2]
            result = proc.process_l2_snapshot(bids, asks)

            if result and result.get("gpu_used"):
                # Store GPU-computed metrics on runtime for downstream signal use
                runtime.last_gpu_l2 = result
                # Override book-level spread/imbalance with GPU-accurate values
                if result.get("spread", 0.0) > 0:
                    mid = (result["best_bid"] + result["best_ask"]) * 0.5
                    if mid > 0:
                        runtime.last_spread_bps_l2 = float(result["spread"] / mid * 10_000.0)
                runtime.last_depth_bid_5 = float(result.get("bid_depth", runtime.last_depth_bid_5 or 0.0))
                runtime.last_depth_ask_5 = float(result.get("ask_depth", runtime.last_depth_ask_5 or 0.0))
                # Microprice (better than (bid+ask)/2 for signal quality)
                mp = result.get("microprice", 0.0)
                if mp > 0:
                    runtime.last_book_mid = float(mp)
                # Wall detection
                runtime.last_gpu_wall_bid_price = float(result.get("wall_bid_price", 0.0))
                runtime.last_gpu_wall_ask_price = float(result.get("wall_ask_price", 0.0))
        except Exception:
            pass  # Fail open — GPU errors must never break the hot path

    def _update_ofi(self, runtime: SymbolRuntime, snap: BookSnapshot, prev_snap: Optional[BookSnapshot], book_ts_ms: int, book_raw: Dict[str, Any]):
        try:
             bids = book_raw.get("bids") or []
             asks = book_raw.get("asks") or []
             if bids and asks:
                 bb_px = float(bids[0][0] or 0.0); bb_q = float(bids[0][1] or 0.0)
                 ba_px = float(asks[0][0] or 0.0); ba_q = float(asks[0][1] or 0.0)
                 
                 if bb_px > 0 and ba_px > 0:
                     mid = 0.5 * (bb_px + ba_px)
                     runtime.last_book_mid = float(mid)
                     spr = float(ba_px - bb_px)
                     # Fix: guard against crossed BBO (ask <= bid).
                     # If crossed, we do NOT zero-out last_spread_bps_l2 (that would
                     # trigger spread_missing on next tick).  Instead we keep the last
                     # good value and set a diagnostic flag on runtime.
                     if spr > 0 and mid > 0:
                         runtime.last_spread_bps_l2 = float((spr / mid) * 10_000.0)
                         runtime.last_spread_bps_l2_ts_ms = int(book_ts_ms)  # track freshness
                         runtime.book_crossed = 0
                         # Mark first successful book snapshot (used for cold-start grace period)
                         if not getattr(runtime, "first_book_ts_ms", 0):
                             runtime.first_book_ts_ms = int(book_ts_ms)
                     else:
                         # Crossed or zero spread: annotate but preserve last good value
                         runtime.book_crossed = 1
                     
                     # Depth USD (Top 5)
                     db = 0.0; da = 0.0
                     for lv in bids[:5]:
                         try: db += float(lv[1] or 0.0)
                         except Exception: pass
                     for lv in asks[:5]:
                         try: da += float(lv[1] or 0.0)
                         except Exception: pass
                     runtime.last_depth_bid_5 = float(db)
                     runtime.last_depth_ask_5 = float(da)
                     runtime.last_depth_min_5_usd = float(min(db, da) * mid)

                     # L3-lite book updates (ETA fill, cancel-to-trade proxies)
                     try:
                         if getattr(runtime, "l3_stats", None) is not None:
                             runtime.l3_stats.on_book(book_ts_ms, depth_bid_5=float(db), depth_ask_5=float(da))
                     except Exception:
                         pass

                     # Resilience tracker: depth replenishment after sweep (use USD notional)
                     try:
                         if getattr(runtime, "resilience", None) is not None:
                             runtime.resilience.on_book(
                                 book_ts_ms,
                                 bid_depth_usd=float(db) * float(mid),
                                 ask_depth_usd=float(da) * float(mid),
                             )
                     except Exception:
                         pass

                     # ------------------------------------------------------------------
                     # Resilience / Replenishment proxy (book_resilience secondary tracker)
                     #
                     # If BookResilienceTracker is enabled in runtime (injected via
                     # __post_init__), update it on every book snapshot using
                     # min(topN bid, topN ask) * mid as depth proxy.
                     #
                     # Exposes state into runtime.dynamic_cfg for gates / ML features:
                     #   res_active      -- 1 while tracking post-sweep recovery
                     #   res_recovered   -- 1 once depth crossed target_recovery_ratio
                     #   res_recovery_ms -- ms elapsed until first recovery
                     #   res_min_ratio   -- minimum depth ratio observed (worst point)
                     #   res_curr_ratio  -- current depth ratio vs baseline
                     #   res_speed_per_s -- replenishment speed proxy (ratio/s)
                     #
                     # Fail-open: never break book processing if tracker is missing
                     # or its interface changes.
                     # ------------------------------------------------------------------
                     try:
                         br = getattr(runtime, "book_resilience", None)
                         if br is not None and book_ts_ms > 0:
                             depth_min_usd = float(runtime.last_depth_min_5_usd)
                             # Real facade API: on_book(ts_ms, depth_now_usd=...)
                             # Full tracker API: on_book(ts_ms, bid_depth_usd=..., ask_depth_usd=...)
                             if hasattr(br, "on_book"):
                                 br.on_book(int(book_ts_ms), depth_now_usd=depth_min_usd)
                             elif hasattr(br, "on_depth"):
                                 # Alternate naming convention (reference API)
                                 br.on_depth(ts_ms=int(book_ts_ms), depth_min_usd=depth_min_usd)
                             elif hasattr(br, "update"):
                                 br.update(ts_ms=int(book_ts_ms), depth_min_usd=depth_min_usd)

                             # Expose snapshot into dynamic_cfg
                             st = None
                             try:
                                 if hasattr(br, "snapshot"):
                                     st = br.snapshot()
                                 elif hasattr(br, "state"):
                                     st = br.state()
                             except Exception:
                                 st = None

                             if isinstance(st, dict):
                                 runtime.dynamic_cfg["res_active"]     = int(st.get("res_active",     st.get("active",     0)) or 0)
                                 runtime.dynamic_cfg["res_recovered"]   = int(st.get("res_recovered",  st.get("recovered",  0)) or 0)
                                 runtime.dynamic_cfg["res_recovery_ms"] = int(st.get("res_recovery_ms", st.get("t_recover_ms", 0)) or 0)
                                 runtime.dynamic_cfg["res_min_ratio"]   = float(st.get("res_min_ratio", st.get("depth_ratio", 0.0)) or 0.0)
                                 runtime.dynamic_cfg["res_curr_ratio"]  = float(st.get("res_curr_ratio", st.get("curr_ratio", 0.0)) or 0.0)
                                 runtime.dynamic_cfg["res_speed_per_s"] = float(st.get("res_speed_per_s", st.get("speed", 0.0)) or 0.0)
                             else:
                                 # Best-effort attribute access fallback
                                 runtime.dynamic_cfg["res_active"]     = int(getattr(br, "res_active",     getattr(br, "active",     0)) or 0)
                                 runtime.dynamic_cfg["res_recovered"]   = int(getattr(br, "res_recovered",  getattr(br, "recovered",  0)) or 0)
                                 runtime.dynamic_cfg["res_recovery_ms"] = int(getattr(br, "res_recovery_ms", getattr(br, "t_recover_ms", 0)) or 0)
                                 runtime.dynamic_cfg["res_min_ratio"]   = float(getattr(br, "res_min_ratio", getattr(br, "depth_ratio", 0.0)) or 0.0)
                                 runtime.dynamic_cfg["res_curr_ratio"]  = float(getattr(br, "res_curr_ratio", getattr(br, "curr_ratio", 0.0)) or 0.0)
                                 runtime.dynamic_cfg["res_speed_per_s"] = float(getattr(br, "res_speed_per_s", getattr(br, "speed", 0.0)) or 0.0)
                     except Exception:
                         pass

                     # OFI Update

                     try:
                         ev = runtime.ofi_tracker.update(
                             ts_ms=book_ts_ms,
                             bid_px=bb_px, bid_qty=bb_q,
                             ask_px=ba_px, ask_qty=ba_q,
                         )
                         if ev is not None:
                             # Reclaim Bonus Logic
                             try:
                                 bias = str(getattr(ev, "direction_bias", "") or "").upper()
                                 if runtime.last_sweep_ts_ms > 0 and bias in ("LONG", "SHORT"):
                                     # Assuming bar available in scope? No. We use runtime.last_bar?
                                     # Original code used `bar.cvd_close` which is tricky if bar is not passed.
                                     # We will skip CVD reclaim specific logic here if it depends on bar context irrelevant to book.
                                     # Or fallback to runtime.cvd_state?
                                     pass
                             except Exception:
                                 pass
                             
                             runtime.last_ofi_event = {
                                 "ts_ms": _safe_int(ev.ts_ms),
                                 "direction": str(ev.direction),
                                 "ofi": float(ev.ofi),
                                 "ofi_usd": float(ev.ofi_usd),
                                 "ofi_z": float(ev.ofi_z),
                                 "stable_secs": float(ev.stable_secs),
                                 "stability_score": float(ev.stability_score),
                             }
                     except Exception:
                         pass
                     
                     # Best Level OFI (if prev_snap)
                     if prev_snap is not None:
                         try:
                             ofi_raw = runtime.ofi_tracker.compute_ofi_best_level(
                                 prev_bid_px=float(prev_snap.best_bid_px),
                                 prev_bid_qty=float(prev_snap.best_bid_qty),
                                 prev_ask_px=float(prev_snap.best_ask_px),
                                 prev_ask_qty=float(prev_snap.best_ask_qty),
                                 bid_px=float(snap.best_bid_px),
                                 bid_qty=float(snap.best_bid_qty),
                                 ask_px=float(snap.best_ask_px),
                                 ask_qty=float(snap.best_ask_qty),
                             )
                             depth_qty = float(min(snap.depth_5_bid_vol, snap.depth_5_ask_vol))
                             ofi_z, stable_secs, score = runtime.ofi_tracker.update(
                                 ts_ms=_safe_int(book_ts_ms),
                                 ofi=float(ofi_raw),
                                 depth_qty=depth_qty,
                                 deadband_abs=float(runtime.config.get("ofi_deadband_abs", 0.0) or 0.0),
                                 deadband_frac_depth=float(runtime.config.get("ofi_deadband_frac_depth", 0.02) or 0.02),
                                 z_full=float(runtime.config.get("ofi_z_full", 3.0) or 3.0),
                             )
                             is_stable = bool(stable_secs >= 1.0 and score >= 0.8)
                             ev_ofi = OFIEvent(
                                 ts_ms=_safe_int(book_ts_ms),
                                 ofi=float(ofi_raw),
                                 ofi_z=float(ofi_z),
                                 stable_secs=float(stable_secs),
                                 stability_score=float(score),
                                 stable=_safe_int(is_stable),
                             )
                             runtime.last_ofi_event = ev_ofi.to_dict()
                         except Exception:
                             pass

        except Exception:
            pass

    def _update_liquidity(self, runtime: SymbolRuntime, book_ts_ms: int, book_raw: Dict[str, Any]):
        if _safe_int(runtime.config.get("liq_enable", 1) or 0) == 1:
            try:
                liq = runtime.liq_guard.update(
                    ts_ms=_safe_int(book_ts_ms),
                    spread_bps=float(runtime.last_spread_bps_l2),
                    depth_min_5_usd=float(runtime.last_depth_min_5_usd),
                    book_rate_hz=float(getattr(runtime, "book_rate_ema", 0.0) or 0.0),
                )
                runtime.last_liq_score = float(liq.score)
                runtime.last_liq_regime = str(liq.regime)
                runtime.dynamic_cfg["liq_score"] = float(liq.score)
                runtime.dynamic_cfg["liq_regime"] = str(liq.regime)

                # Stressed logic override
                rg_liq = str(getattr(runtime, "last_liq_regime", "") or "")
                if rg_liq == "stressed":
                    runtime.dynamic_cfg["strong_need_reversal"] = max(_safe_int(runtime.dynamic_cfg.get("strong_need_reversal", 0)), 3)
                    runtime.dynamic_cfg["strong_need_continuation"] = max(_safe_int(runtime.dynamic_cfg.get("strong_need_continuation", 0)), 3)
            except Exception:
                pass
