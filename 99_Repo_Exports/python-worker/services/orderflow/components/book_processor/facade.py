from __future__ import annotations

import logging
from typing import Any

from core.dyn_cfg_keys import DynCfgKeys as DK

# P5: book sanity + stream integrity
from services.orderflow.configuration import _safe_float, _safe_int
from services.orderflow.metrics import (
    book_missing_seq_ema_gauge,
    book_missing_seq_events_total,
    # P0/P1 audit: book observability metrics
    log_silent_error,
)

# P112: minimal DQ/book-seq metrics live in a dedicated module to avoid
# duplicate metric registration across SoT/mirror import paths.
from services.orderflow.metrics_bookseq_dq_p112 import (
    book_missing_seq_ema_gauge,
    book_seq_last_gap_gauge,
)
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.utils import _fields_to_dict

from .book_rate_tracker import BookRateTracker
from .iceberg_tracker import IcebergTracker
from .lob_pressure_tracker import LOBPressureTracker
from .obi_tracker import OBITracker
from .ofi_tracker import OFITracker
from .state_updater import BookStateUpdater
import contextlib

# GPU L2 processor — lazy import, no hard dependency
try:
    from gpu.l2_processor import L2GPUProcessor as _L2GPU
    _L2GPU_AVAILABLE = True
except ImportError:
    _L2GPU_AVAILABLE = False

logger = logging.getLogger("orderflow_book_processor")

# Per-symbol GPU processor cache (created on first use)
_l2gpu_cache: dict[str, Any] = {}

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


    def _update_book_missing_seq(self, runtime: SymbolRuntime, book_raw: dict[str, Any]) -> None:
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
                decide_book_seq_uu,
                ema_update_clamped,
                resolve_book_seq_ema_alpha,
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
        runtime.book_seq_last_reason = reason

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
            with contextlib.suppress(Exception):
                book_missing_seq_events_total.labels(symbol=str(runtime.symbol)).inc()

        # Prom gauges: always set to keep dashboards stable.
        try:
            if book_missing_seq_ema_gauge is not None:
                book_missing_seq_ema_gauge.labels(symbol=str(runtime.symbol)).set(float(runtime.book_missing_seq_ema))
            if book_seq_last_gap_gauge is not None:
                book_seq_last_gap_gauge.labels(symbol=str(runtime.symbol)).set(float(getattr(runtime, "book_missing_seq_last_gap", 0) or 0))
        except Exception:
            # Metrics are fail-open by design.
            pass

        # Export EMA to Prometheus at book rate as well (not only on ticks).
        # This avoids staleness when book stream is live but tick stream is quiet.
        with contextlib.suppress(Exception):
            book_missing_seq_ema_gauge.labels(symbol=str(runtime.symbol)).set(float(runtime.book_missing_seq_ema))

        # Advance last_u only when monotonic; this is robust against duplicates / reorders.
        if next_last_u > prev_u:
            runtime.book_seq_last_u = int(next_last_u)

    def process_book(self, runtime: SymbolRuntime, payload: dict[str, Any], ingest_ts_ms: int) -> bool:
        """
        Processes a raw book payload from Redis stream.
        Returns True if processed successfully, False otherwise.
        """
        try:
            # 1. Parsing & State Update
            raw = _fields_to_dict(payload)
            success, book_raw, snap, prev_snap, book_ts_ms, prev_ts_ms = BookStateUpdater.parse_and_update(
                self, runtime, raw, ingest_ts_ms
            )
            if not success or not book_raw or not snap:
                return False

            # 2b. LOB pressure features (P91) — queue imbalance / microprice / slope / dw_obi
            # Fail-open: any exception here must NOT stop the book processing pipeline.
            LOBPressureTracker.update(runtime, snap, prev_snap, book_ts_ms)

            # 3. Book Rate & Churn Metrics
            # Fail-open: any exception here must NOT stop the book processing pipeline.
            try:
                BookRateTracker.update(self, runtime, book_ts_ms, prev_ts_ms)
            except Exception as exc:
                log_silent_error(exc, "book_rate_failure", runtime.symbol, "BookProcessor:book_rate")

            # 4. Detectors Feed

            # OBI
            OBITracker.update(runtime, book_raw, book_ts_ms)

            # Iceberg
            IcebergTracker.update(runtime, book_raw, book_ts_ms)

            # OFI, Depth (L3-lite) and Resilience
            OFITracker.update(runtime, snap, prev_snap, book_ts_ms, book_raw)

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

    def _update_l2_gpu(self, runtime: SymbolRuntime, book_raw: dict[str, Any]) -> None:
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

    def _update_liquidity(self, runtime: SymbolRuntime, book_ts_ms: int, book_raw: dict[str, Any]):
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
                runtime.dynamic_cfg[DK.LIQ_SCORE] = float(liq.score)
                runtime.dynamic_cfg[DK.LIQ_REGIME] = str(liq.regime)

                # Stressed logic override
                rg_liq = str(getattr(runtime, "last_liq_regime", "") or "")
                if rg_liq == "stressed":
                    runtime.dynamic_cfg[DK.STRONG_NEED_REVERSAL] = max(_safe_int(runtime.dynamic_cfg.get(DK.STRONG_NEED_REVERSAL, 0)), 3)
                    runtime.dynamic_cfg[DK.STRONG_NEED_CONTINUATION] = max(_safe_int(runtime.dynamic_cfg.get(DK.STRONG_NEED_CONTINUATION, 0)), 3)
            except Exception:
                pass
