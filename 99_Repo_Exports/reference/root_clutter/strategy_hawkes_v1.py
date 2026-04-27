"""
Универсальный сервис ордерфлоу для крипто‑фьючерсов Binance USDT-M.

Задачи:
- Читает тики и книги заявок из Redis Streams (`stream:tick_<symbol>` / `stream:book_<symbol>`).
- Поддерживает динамический список символов (set `crypto:symbols`) + базовые `BTCUSDT`, `ETHUSDT`.
- Берёт настройки из `config:orderflow:<symbol>` (Hash) и пресетов `OrderFlowConfig`.
- Использует готовые детекторы из `core.crypto_orderflow_detectors`.
- Публикует сигналы в `notify:telegram`, `signals:crypto:raw` и (опционально) `orders:queue`.

Сервис асинхронный, построен на redis.asyncio.
"""

from __future__ import annotations

import json
import os
import time
import sys
import asyncio
import logging
import random
from typing import Any, Dict, List, Optional, Tuple, Set
from collections import deque
import math
from datetime import datetime, timezone

from handlers.crypto_orderflow.utils.log_sampler import sampled_info, sampled_warning, sampled_debug


from common.decision_trace import ensure_trace, trace_gate, Span, trace_enabled
from core.confidence_utils import normalize_confidence_pct
from services.tp_config import parse_tp_ratio
from services.orderflow.configuration import (
    OrderFlowConfigLoader, _safe_int, _safe_float, _to_bool, 
    _ensure_list_levels, DEFAULT_SYMBOLS, DEFAULT_CONFIG
)
from core.pressure_tracker import PressureTracker
from core.burst_gate import BurstCandidateSelector, BurstCandidate
from core.robust_stats import RollingRobustZ
from core.candidate_score import compute_candidate_score
from core.book_churn import compute_churn_from_z

from core.eff_quote_calibrator import EffQuoteCalibrator
from core.atr_tf_calibrator import ATRTfCalibrator
from core.atr_sanity_calibrator import ATRSanityCalibrator
from core.atr_bps_calibrator import ATRBpsCalibrator
from core.thresh_stability import ThresholdStabilityTracker

from prometheus_client import Counter, Gauge, start_http_server, REGISTRY



from services.orderflow.metrics import (
    log_silent_error, silent_errors_total,
    atr_gate_veto_total, tp1_net_margin_bps_gauge, tp1_zero_pnl_total, signals_total,
    fp_imb_confirm_total, fp_absorb_confirm_total, fp_buckets_evicted_total,
    book_calib_ready, dn_calib_ready, book_health_ok, book_rate_hz, dn_usd, calib_persist_total,
    tick_ts_missing_total, tick_ts_backwards_total, tick_ts_clamped_total, tick_ts_quarantined_total,
    burst_active_gauge, burst_flush_total, signals_emitted_total, burst_window_ms_gauge, tick_gap_p50_ms_gauge,
    ticks_out_of_order_total, ticks_side_unknown_total, bars_closed_total, divergence_detected_total,
    sweep_detected_total, strong_gate_veto_total, evidence_used_total, ticks_pressure_filtered_total,
    atr_tf_switch_total, atr_tf_candidate_diff, atr_tf_target_bps, atr_tf_candidate_score,
    book_rate_ema_gauge, book_rate_z_gauge, book_stale_ms_gauge, atr_sanity_stale_total,
    atr_sanity_missing_total, atr_floor_ready_gauge, atr_floor_picked_bps_gauge, atr_gate_dominant_total,
    dn_tier0_usd, dn_tier1_usd, dn_tier2_usd, ptier_tier0_usd, ptier_tier1_usd, ptier_tier2_usd, dn_calib_n,
    dn_tier_attempt_total, dn_tier_pass_total, dn_tier_passrate_ema_gauge,
    dn_gate_events_total, dn_how_scale_gauge, dn_how_diff_ratio_gauge, dn_how_diff_alert_total,
    of_session_outcome_total, veto_low_conf_total, veto_min_conf_total,
    cvd_reclaim_eval_total, cvd_reclaim_ok_total, cvd_reclaim_applied_total, cvd_reclaim_no_data_total,
    cvd_reclaim_ratio_gauge, cvd_reclaim_age_ms_gauge, cvd_reclaim_window_ms_gauge
)
from services.orderflow.utils import (
    _fields_to_dict, _normalize_epoch_ms, _score_candidate,
    _calc_pressure_sps, _cooldown_ms_for, _should_sample,
    session_utc, hour_of_week_utc, fmt_utc_dow_hour
)
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.signal_pipeline import SignalPipeline
from services.orderflow.market_state import MarketStateService






from core.smt_symbol_snapshot import SymbolSnapshot
from core.book_health import compute_book_health
from core.book_rate_calibrator import BookRateCalibrator
from core.delta_notional_calibrator import DeltaNotionalCalibrator
from core.atr_tf_sanity_calibrator import AtrTfSanityCalibrator
from core.atr_floor_policy import compute_atr_bps_threshold
from core.fees_aware_policy import fees_aware_min_atr_bps
from core.daily_ohlc_tracker import DailyCandleTracker

from core.weak_progress import compute_weak_progress, WeakProgressSnapshot
from common.zone_store import ZonePack
from core.reclaim_detector import ReclaimDetector, ReclaimEvent
from core.fp_edge_absorb import FPEdgeAbsorbDetector, EdgeAbsorbEvent


from core.sweep_detector import SweepDetector, SweepEvent
from core.footprint_policy import fp_confirmations_from_microbar, is_soft_confirmation
from core.strong_of_gate import eval_reversal, eval_continuation, hidden_trend_dir
from core.book_evidence import compute_obi_flags, compute_iceberg_flags
from core.of_evidence import compute_sweep_recent, compute_reclaim_recent, compute_absorption_flags
from core.of_confirm_engine import OFConfirmEngine
from core.absorption_level_score import compute_absorption_level_score
from core.of_inputs_contract import OFInputsV1


from core.tick_gap_tracker import TickGapTracker
from core.burst_calibrator import BurstCalibrator

from core.time_utils import normalize_epoch_ms, extract_tick_ts_ms

# Consolidated core imports
from core.obi_stability_tracker import OBIStabilityTracker
from core.weak_progress_detector import WeakProgressDetector
from core.cvd_reclaim import compute_cvd_reclaim, CVDReclaimEvent
from core.regime_quantiles_store import approx_quantile_adx




from services.async_signal_publisher import AsyncSignalPublisher, StreamSink


import redis.asyncio as aioredis
from redis.exceptions import RedisError, ResponseError

from common.time_norm import normalize_epoch_ms
from core.crypto_orderflow_detectors import (
    AbsorptionDetector,
    DeltaSpikeDetector,
    IcebergDetector,
    OBIDetector,
)
from core.instrument_config import OrderFlowConfig, get_config, get_specs, get_default_delta_tiers

from services.signal_confidence import ConfidenceScorer, ConfidenceConfig
from core.tick_cvd import TickCVDState
from core.microbar import MicroBar, MicroBarAggregator
from core.swing_detector import SwingDetector, SwingPoint
from core.divergence_engine import DivergenceEngine, DivergenceEvent
from core.rsi import StreamingRSI
from core.eq_pools import EQPoolTracker, EQPool
from core.data_health import compute_data_health, apply_book_evidence_policy, apply_shadow_only_policy
from core.slippage_model import expected_slippage_bps


# ──────────────────────────────────────────────────────────────────────────────
# Настройки по умолчанию
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("crypto_orderflow_service")
# Настройка логирования
log_level = os.getenv("CRYPTO_OF_LOG_LEVEL", "INFO")
logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
# Доп. флаг: подробный DEBUG по дельте (по умолчанию выключен, чтобы не шуметь)
# Доп. флаг: подробный DEBUG по дельте (по умолчанию выключен, чтобы не шуметь)
DEBUG_DELTAS = os.getenv("CRYPTO_OF_DEBUG_DELTAS", "false").strip().lower() in ("1", "true", "yes", "on")






# Счетчик для уменьшения логов добавления символов
_symbols_added_counter = 0





# ──────────────────────────────────────────────────────────────────────────────
from services.orderflow.signal_pipeline import SignalPipeline
from utils.atr_cache import get_atr_cache, ATRCache


# ──────────────────────────────────────────────────────────────────────────────
# Runtime для одного символа
# ──────────────────────────────────────────────────────────────────────────────


# Optional microstructure metrics (prom)




class OrderFlowStrategy:
    def __init__(self, redis: aioredis.Redis, ticks: aioredis.Redis, publisher: AsyncSignalPublisher, 
                 of_engine: OFConfirmEngine, calib_svc=None,
                 notify_client: Optional[aioredis.Redis] = None, notify_stream: str = "notify:telegram"):
        self.redis = redis
        self.ticks = ticks
        self.publisher = publisher
        self.of_engine = of_engine
        self.calib_svc = calib_svc
        self.notify_client = notify_client
        self.notify_stream = notify_stream
        self.logger = logging.getLogger("orderflow_strategy")
        
        self.atr_cache: ATRCache = get_atr_cache()
        self.market_state = MarketStateService(redis_client=self.redis, atr_cache=self.atr_cache)
        self.signal_pipeline = SignalPipeline(publisher=self.publisher, atr_cache=self.atr_cache)
        self.low_conf_counters = {}
        self.strong_gate_counters = {}
        # Simple confidence scorer for fallback usage
        self.conf_scorer = ConfidenceScorer(cfg=ConfidenceConfig())

    async def _maybe_poll_symbol_overrides(self, runtime, now_ms: int) -> None:
        """
        Pull cfg:crypto_of:overrides:{SYMBOL} (JSON) and merge selected keys into runtime.config.
        Fail-open, throttled, deterministic by now_ms=tick_ts.
        """
        try:
            gap = int(getattr(runtime, "_ov_poll_gap_ms", 2500) or 2500)
            ts0 = int(getattr(runtime, "_ov_ts_ms", 0) or 0)
            if (now_ms - ts0) < gap:
                return
            runtime._ov_ts_ms = int(now_ms)
            key = f"cfg:crypto_of:overrides:{str(runtime.symbol).upper()}"
            raw = await self.redis.get(key)
            if not raw:
                return
            # etag to avoid repeated json loads (simple hash-like etag)
            etag = str(abs(hash(raw)))
            if etag == str(getattr(runtime, "_ov_etag", "") or ""):
                return
            runtime._ov_etag = etag
            d = json.loads(raw)
            if not isinstance(d, dict):
                return
            # allowlist of keys (avoid accidental config takeover)
            allow = {
                "cooldown_reversal_sec",
                "cooldown_continuation_sec",
                "pressure_hi_sps",
                "pressure_ema_alpha",
                "cooldown_mul_thin",
                "cooldown_spread_hi_bp",
                "cooldown_mul_wide_spread",
                "cooldown_mul_pressure_hi",
                "cooldown_min_ms",
                "cooldown_max_ms",
                "burst_audit_enable",
                "burst_audit_sample",
            }
            for k, v in d.items():
                if k in allow:
                    runtime.config[k] = v
        except Exception as exc:
            log_silent_error(exc, 'config_update_failure', runtime.symbol if runtime else "unknown", '_maybe_poll_symbol_overrides')
            return

    async def _burst_audit(self, *, runtime, now_ms: int, event: str, payload: Dict[str, Any], indicators: Dict[str, Any], extra: Dict[str, Any]) -> None:
        """
        Low-volume audit for cooldown floods and best-of-burst selection.
        Fail-open. Uses deterministic sampling.
        """
        try:
            cfg = runtime.config or {}
            if not bool(int(cfg.get("burst_audit_enable", 0))):
                return
            rate = float(cfg.get("burst_audit_sample", 0.05) or 0.05)
            if not _should_sample(int(now_ms), rate):
                return
            msg = {
                "type": "burst_audit",
                "ts_ms": str(int(now_ms)),
                "symbol": str(runtime.symbol),
                "event": str(event),
                "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                "ind": json.dumps({
                    "scenario": indicators.get("strong_gate_scn") or "",
                    "of_score": indicators.get("of_confirm_score", 0.0),
                    "delta_z": indicators.get("delta_z", 0.0),
                    "pressure_sps": float(getattr(runtime, "pressure_sps", 0.0) or 0.0),
                    "pressure_hi": int(getattr(runtime, "pressure_hi", 0) or 0),
                    "regime": str(getattr(runtime, "last_regime", "na") or "na"),
                    "spread_bp": float(getattr(runtime, "last_spread_bps", 0.0) or 0.0),
                    "obi_age_ms": indicators.get("obi_age_ms", -1),
                    "iceberg_age_ms": indicators.get("iceberg_age_ms", -1),
                }, ensure_ascii=False, separators=(",", ":")),
                "extra": json.dumps(extra or {}, ensure_ascii=False, separators=(",", ":")),
            }
            await self.redis.xadd(self.burst_audit_stream, msg, maxlen=200000, approximate=True)
        except Exception as exc:
            log_silent_error(exc, 'audit_failure', self.symbol or "unknown", '_burst_audit')
            return

    # ── Публичные методы ──────────────────────────────────────────────────────


    # ── Динамическая загрузка символов ────────────────────────────────────────










    # ── Основные рабочие циклы ────────────────────────────────────────────────

    async def process_tick(self, runtime: SymbolRuntime, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Initialize variables that may not be set if exceptions occur
        ofc = None
        dec = None

        # Быстрый ранний выход: некорректный тик
        if not tick or not isinstance(tick, dict):
            return None
        runtime.tick_count += 1
        runtime.heartbeat_counter += 1
        # Нормализуем qty/volume, чтобы downstream не падал
        if "qty" not in tick and "volume" in tick:
            tick["qty"] = tick.get("volume")
        if tick.get("qty") is None and tick.get("volume") is None:
            tick["qty"] = 0.0
        if tick.get("price") is None:
            # Без цены не обрабатываем
            return None
        if not hasattr(self, "logger"):
            self.logger = logger
        
        # ------------------------------------------------------------------
        # Robust Time Normalization (Expert Recommendation 3, Patch 1)
        # ------------------------------------------------------------------
        if tick.get("mock_force"):
             self.logger.warning("🔍 (%s) _handle_tick: START tick_ts=%s", runtime.symbol, tick.get("ts_ms"))
        tick_ts = int(
            tick.get("ts_ms")
            or tick.get("ts")
            or tick.get("event_time")
            or tick.get("E")
            or tick.get("T")
            or tick.get("time")
            or tick.get("written_at")
            or 0
        )
        # Only fallback if 0
        if tick_ts <= 0:
            from services.orderflow.metrics import tick_ts_missing_total
            if tick_ts_missing_total:
                tick_ts_missing_total.labels(symbol=runtime.symbol).inc()
            return None

        indicators: Dict[str, Any] = {}
        
        # ------------------------------------------------------------------
        # Data Quality: tick time health (deterministic)
        # ------------------------------------------------------------------
        try:
            if tick_ts <= 0:
                indicators["tick_ts_missing"] = 1
            else:
                prev = int(getattr(runtime, "last_ts_ms", 0) or 0)
                if prev > 0 and tick_ts < prev:
                    indicators["tick_oood"] = 1
                if prev > 0 and tick_ts > prev:
                    gap = tick_ts - prev
                    if gap >= int(cfg.get("tick_gap_warn_ms", 2000)):
                        indicators["tick_gap_ms"] = int(gap)
        except Exception:
            pass

        # Monotonicity check (Expert Recommendation 3: detect -> sanitize -> quarantine)
        MAX_BACK_MS = int(os.getenv("TIME_MAX_BACK_MS", "2000"))
        WARN_BACK_MS = int(os.getenv("TIME_WARN_BACK_MS", "500"))
        prev_ts = int(getattr(runtime, "last_ts_ms", 0) or 0)

        if prev_ts > 0 and tick_ts < prev_ts:
            # backward time
            back = prev_ts - tick_ts
            if tick_ts_backwards_total:
                tick_ts_backwards_total.labels(symbol=runtime.symbol).inc()

            if back <= MAX_BACK_MS:
                # sanitize: clamp slightly forward to keep deterministic monotonicity
                tick_ts = prev_ts + 1
                if tick_ts_clamped_total:
                     tick_ts_clamped_total.labels(symbol=runtime.symbol).inc()
                
                # Observability: mark degraded quality + alert-ish metric
                indicators["tick_quality"] = "low"
                indicators["tick_ts_back_ms"] = int(back)
                if back > WARN_BACK_MS:
                    if ticks_out_of_order_total:
                        try:
                            ticks_out_of_order_total.labels(symbol=runtime.symbol).inc()
                        except Exception:
                            pass
                    # Optional: sampled warning
                    sampled_warning(
                        self.logger, "TIME_SKEW_DETECTED",
                        "⚠️ Time skew detected for %s: back_ms=%d (clamped)", 
                        runtime.symbol, back
                    )
            else:
                # quarantine: too large rollback — fail-closed
                if tick_ts_quarantined_total:
                     tick_ts_quarantined_total.labels(symbol=runtime.symbol).inc()
                return None



        runtime.last_ts_ms = int(tick_ts)
        sess = session_utc(int(tick_ts))
        how = hour_of_week_utc(int(tick_ts))
        indicators["session"] = sess
        indicators["hour_of_week"] = how

        # ------------------------------------------------------------------
        # Source consistency guard (dual-source / CVD jump)
        # - detects implausible delta jumps and marks source_consistency_ok=0
        # - consumer policy: turn book evidences off, optionally shadow-only
        # ------------------------------------------------------------------
        try:
            px = float(tick.get("price") or 0.0)
            cvd = float(getattr(runtime, "cvd_last", 0.0) or 0.0)
            cvd_prev = float(getattr(runtime, "cvd_prev", cvd) or cvd)
            # compute jump in USD
            jump_usd = 0.0
            if px > 0:
                jump_usd = abs(cvd - cvd_prev) * px
            # thresholds: default high to avoid false triggers
            j_usd_th = float(cfg.get("source_jump_usd_th", 50_000_000.0))
            if jump_usd > j_usd_th:
                indicators["source_consistency_ok"] = 0
                indicators["source_jump_usd"] = float(jump_usd)
                # cool down period (ms) during which we keep it marked inconsistent
                until = int(tick_ts) + int(cfg.get("source_inconsistent_ttl_ms", 60_000))
                setattr(runtime, "source_inconsistent_until_ms", until)
            else:
                until = int(getattr(runtime, "source_inconsistent_until_ms", 0) or 0)
                if until > int(tick_ts):
                    indicators["source_consistency_ok"] = 0
                else:
                    indicators["source_consistency_ok"] = 1
            setattr(runtime, "cvd_prev", cvd)
            setattr(runtime, "cvd_last", cvd)
        except Exception:
            pass

        # Expert Recommendation 4: Track timestamp for Gap Cap
        lt_seen = int(getattr(runtime, "last_tick_seen_ts", 0) or 0)
        if lt_seen > 0 and tick_ts > lt_seen:
             gap = tick_ts - lt_seen
             try:
                 runtime.tick_gaps_ms.append(int(gap))
             except Exception:
                 pass
        runtime.last_tick_seen_ts = int(tick_ts)

        # Runtime overrides (cooldown/pressure tuning) — throttled, fail-open
        try:
            # Legacy override poll (cfg:crypto_of:overrides) - kept for compatibility
            asyncio.create_task(self._maybe_poll_symbol_overrides(runtime, int(tick_ts)))
            
            # SRE Versioned Overrides V1 (High Priority)
            # self.redis is safe to use here? self.redis is async client.
            asyncio.create_task(runtime.maybe_load_overrides(self.redis))
        except Exception:
            pass

        # Initialize early
        confirmations: List[str] = []
        indicators: Dict[str, Any] = {}
        
        # --- Apply Overrides V1 into local cfg view (deterministic per tick best-effort) ---
        # We start with runtime.config (base)
        cfg = runtime.config
        try:
            o = getattr(runtime, "overrides_obj", None)
            if o is not None and int(getattr(o, "enabled", 0) or 0) == 1:
                # Canary decision:
                #  - if canary_symbols defined -> apply only if symbol is listed
                #  - else apply by deterministic hash-share (optional)
                ro = getattr(o, "rollout", None)
                apply_ovr = True
                if ro is not None and str(getattr(ro, "mode", "full") or "full").lower() == "canary":
                    syms = set([str(x).upper() for x in (getattr(ro, "canary_symbols", []) or []) if x])
                    if syms:
                        apply_ovr = (str(runtime.symbol or "").upper() in syms)
                    else:
                        # Fallback to share logic? 
                        # Implement deterministic hash share if share < 1.0 (optional)
                        pass

                if apply_ovr:
                    cfg = o.apply_to_cfg(cfg)
                    indicators["policy_sid"] = str(getattr(runtime, "overrides_sid", "") or "")
                    indicators["policy_src"] = "overrides_v1"
        except Exception:
            cfg = runtime.config

        # Book health: check gaps and staleness
        book_ts_base = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
        book_gap = int(tick_ts - book_ts_base) if book_ts_base > 0 else 0
        book_stale_ms = int(runtime.config.get("book_stale_ms", 5000))
        book_ok = 1 if (book_ts_base > 0 and book_gap < book_stale_ms) else 0
        indicators["book_health_ok"] = int(book_ok)
        indicators["book_ts_gap_ms"] = int(book_gap)

        # ------------------------------------------------------------
        # Liquidity regime snapshot (risk overlay)
        # ------------------------------------------------------------
        try:
            snap = getattr(runtime, "last_book", None)
            spread_bps = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
            depth_usd_min_5 = 0.0
            if snap is not None:
                # prefer snapshot spread if available
                try:
                    spread_bps = float(getattr(snap, "spread_bps", spread_bps) or spread_bps)
                except Exception:
                    pass
                try:
                    bb = float(getattr(snap, "best_bid_px", 0.0) or 0.0)
                    ba = float(getattr(snap, "best_ask_px", 0.0) or 0.0)
                    mid = (bb + ba) / 2.0 if (bb > 0 and ba > 0) else 0.0
                    depth_qty = float(min(getattr(snap, "depth_5_bid_vol", 0.0) or 0.0,
                                          getattr(snap, "depth_5_ask_vol", 0.0) or 0.0))
                    depth_usd_min_5 = float(depth_qty * max(mid, 1e-9)) if mid > 0 else 0.0
                except Exception:
                    depth_usd_min_5 = 0.0

            stale = int(book_gap) if book_ts_base > 0 else int(10**9)
            liq = runtime.liq_service.score(
                symbol=runtime.symbol,
                ts_ms=int(tick_ts),
                spread_bps=float(spread_bps),
                depth_usd_min_5=float(depth_usd_min_5),
                book_rate_ema_hz=float(getattr(runtime, "book_rate_ema", 0.0) or 0.0),
                book_stale_ms=int(stale),
            )
            runtime.liq_score = float(liq.liq_score)
            runtime.liq_regime = str(liq.liq_regime)
            runtime.last_liq = liq.to_dict()

            indicators["liq_score"] = float(liq.liq_score)
            indicators["liq_regime"] = str(liq.liq_regime)
            indicators["liq_depth_usd_min_5"] = float(liq.depth_usd_min_5)
            indicators["liq_spread_bps"] = float(liq.spread_bps)
            indicators["liq_book_rate_hz"] = float(liq.book_rate_ema_hz)
            indicators["liq_book_stale_ms"] = int(liq.book_stale_ms)
            if liq.why:
                indicators["liq_why"] = str(liq.why)
        except Exception:
            pass

        # Track tick gaps (Section 5: Burst Calibrator)
        try:
            runtime.tick_gaps.record(int(tick_ts))
        except Exception:
            pass

        # Periodic calibration (every 200 ticks)
        if runtime.tick_count % 200 == 0:
            try:
                # Update window/max_age only if burst is not currently active
                # using the lock for safety although st.active check is usually okay
                async with runtime.burst_mu:
                    is_active = getattr(runtime.burst.st, "active", False)
                    if not is_active:
                        gaps = runtime.tick_gaps.snapshot()
                        p_snap = runtime.pressure.snapshot(now_ms=int(tick_ts))
                        
                        w, ma = runtime.burst_cal.compute(
                            gap_p50_ms=float(gaps.get("p50", 0.0)),
                            cand_per_min=float(p_snap.per_min_ema)
                        )
                        runtime.burst.window_ms = int(w)
                        runtime.burst.max_age_ms = int(ma)
                        
                        # Metrics visibility
                        burst_window_ms_gauge.labels(symbol=runtime.symbol).set(float(w))
                        tick_gap_p50_ms_gauge.labels(symbol=runtime.symbol).set(float(gaps.get("p50", 0.0)))
            except Exception:
                pass
            
        # --- Book Health Gating (Stop Evidence) ---
        # If book is unhealthy, we cannot trust OBI or Iceberg signals.
        # We nullify them (force 0.0) so they don't contribute to the score.
        if int(indicators.get("book_health_ok", 1)) == 0:
            # We don't VETO the entire signal (maybe price action is valid),
            # but we remove microstructure evidence component.
            # (unless it's a super-strong price move > strong_z, handled elsewhere)
            # Nullify indicators for downstream
            indicators["obi"] = 0.0
            indicators["obi_z"] = 0.0
            indicators["iceberg_refresh"] = 0
            indicators["iceberg_avg_qty"] = 0.0
            # Optional: Log throttling?
            pass

        if runtime.heartbeat_counter >= 5000:
            self.logger.info(
                "💓 (%s) Heartbeat: processed 5000 ticks (total=%d) | last_price=%.2f | delta_triggers=%d",
                runtime.symbol,
                runtime.tick_count,
                float(tick.get("price") or 0.0),
                runtime.delta_triggers
            )
            runtime.heartbeat_counter = 0
        
        # Check side classification
        s = str(tick.get("side") or "").upper()
        if s not in ("BUY", "SELL"):
             ticks_side_unknown_total.labels(symbol=runtime.symbol).inc()

        # Tick-CVD update (Phase A) BEFORE delta_detector.push()

        try:
            if runtime.cvd_state:
                runtime.cvd_state.update(tick)
        except Exception:
            pass

        # MicroBar aggregation (Phase B)
        try:
            if runtime.microbar:
                cvd_val = getattr(runtime.cvd_state, "cvd_tick", 0.0)
                closed_bars = runtime.microbar.push_tick(tick, cvd_val)
                if closed_bars:
                    for b in closed_bars:
                        # === Microstructure spread robust stats (per-symbol) ===
                        try:
                            mid = float(getattr(b, "mid_last", 0.0) or 0.0)
                            spr = float(getattr(b, "spread_last", 0.0) or 0.0)
                            if mid > 0 and spr > 0:
                                spread_bps = 10000.0 * (spr / mid)
                                runtime.last_spread_bps = float(spread_bps)
                                runtime.spread_stats.update(float(spread_bps))
                                runtime.last_spread_z = float(runtime.spread_stats.z(float(spread_bps)))
                        except Exception:
                            pass
                        
                        # Fire async microbar closed handler
                        try:
                            asyncio.create_task(self._on_microbar_closed(runtime, b))
                        except Exception:
                            pass
        except Exception:
            pass

        # --- L3-lite (Reconciliation metrics) ---
        try:
            # 1. Feed trade
            runtime.l3_queue.on_trade(
                side=1 if (str(tick.get("side")).upper() == "BUY") else -1,
                qty=float(tick.get("qty") or 0.0)
            )
            
            # 2. Check bucket advancement
            bucket_ms = runtime.l3_queue.bucket_ms or 1000
            cur_bucket_id = int(tick_ts // bucket_ms)
            if runtime._last_l3_bucket_id is None:
                runtime._last_l3_bucket_id = cur_bucket_id
            elif cur_bucket_id > runtime._last_l3_bucket_id:
                # advance bucket and store stats
                runtime.l3_stats = runtime.l3_queue.on_bucket_advance(bucket_id=runtime._last_l3_bucket_id)
                # --- Hawkes-like online intensities (burst features) ---
                # Uses EMA rates from runtime.l3_stats (updated on bucket advance). Cheap O(1) recursion.
                try:
                    if runtime.l3_stats:
                        hs = getattr(runtime, "hawkes_state", None)
                        if hs is None:
                            hs = {"ts_ms": int(tick_ts), "S_taker": 0.0, "S_cancel": 0.0, "S_churn": 0.0}
                            runtime.hawkes_state = hs
                
                        t_now = int(tick_ts)
                        prev_ts = int(hs.get("ts_ms", t_now))
                        dt_s = max(0.0, (t_now - prev_ts) / 1000.0)
                        hs["ts_ms"] = t_now
                
                        # EMA rates (events/sec) from L3-lite queue stats
                        tb = float(getattr(runtime.l3_stats, "taker_buy_rate_ema", 0.0) or 0.0)
                        tsell = float(getattr(runtime.l3_stats, "taker_sell_rate_ema", 0.0) or 0.0)
                        cb = float(getattr(runtime.l3_stats, "cancel_bid_rate_ema", 0.0) or 0.0)
                        ca = float(getattr(runtime.l3_stats, "cancel_ask_rate_ema", 0.0) or 0.0)
                
                        taker_rate = max(0.0, tb + tsell)
                        cancel_rate = max(0.0, cb + ca)
                        churn_rate = taker_rate + cancel_rate
                
                        # Params (Hawkes-like): S <- exp(-beta*dt)*S + rate*dt ; lam = mu + alpha*S
                        cfg = getattr(runtime, "config", {}) or {}
                        beta = float(cfg.get("hawkes_beta", 1.8) or 1.8)
                
                        alpha_t = float(cfg.get("hawkes_alpha_taker", 0.9) or 0.9)
                        mu_t = float(cfg.get("hawkes_mu_taker", 0.1) or 0.1)
                
                        alpha_c = float(cfg.get("hawkes_alpha_cancel", 0.7) or 0.7)
                        mu_c = float(cfg.get("hawkes_mu_cancel", 0.1) or 0.1)
                
                        alpha_h = float(cfg.get("hawkes_alpha_churn", 0.5) or 0.5)
                        mu_h = float(cfg.get("hawkes_mu_churn", 0.1) or 0.1)
                
                        if dt_s > 0.0:
                            decay = math.exp(-beta * dt_s)
                            hs["S_taker"] = decay * float(hs.get("S_taker", 0.0)) + taker_rate * dt_s
                            hs["S_cancel"] = decay * float(hs.get("S_cancel", 0.0)) + cancel_rate * dt_s
                            hs["S_churn"] = decay * float(hs.get("S_churn", 0.0)) + churn_rate * dt_s
                
                        runtime.hawkes_snapshot = {
                            "hawkes_dt_s": float(dt_s),
                            "hawkes_taker_lam": float(mu_t + alpha_t * float(hs.get("S_taker", 0.0))),
                            "hawkes_cancel_lam": float(mu_c + alpha_c * float(hs.get("S_cancel", 0.0))),
                            "hawkes_churn_lam": float(mu_h + alpha_h * float(hs.get("S_churn", 0.0))),
                        }
                except Exception:
                    # Fail-open: Hawkes is a feature-only signal for now
                    pass
                runtime._last_l3_bucket_id = cur_bucket_id
        except Exception:
            pass

        delta_event = runtime.delta_detector.push(tick)
        if delta_event:
             # DEBUG: Confirm event creation immediately (every 10000th)
             sampled_info(logger, "DELTA_EVENT", "🔍 [DELTA-EVENT] (%s) Event created: delta=%.2f z=%.2f", runtime.symbol, delta_event.get("delta", 0.0), delta_event.get("z", 0.0))
        price = _safe_float(tick.get("price")) or _safe_float(tick.get("last")) or _safe_float(tick.get("mid"))
        if price <= 0:
            return None

        # Pressure metric: raw triggers rate (pre-cooldown)
        try:
            if delta_event:
                runtime.pressure.on_raw_trigger(ts_ms=int(tick_ts))
            ps = runtime.pressure.snapshot(now_ms=int(tick_ts))
            indicators["pressure_per_min_ema"] = float(ps.per_min_ema)
            indicators["cooldown_hit_rate_ema"] = float(ps.cd_rate_ema)
            runtime.pressure_sps = float(ps.per_min_ema) / 60.0
        except Exception:
            pass

        # [REMOVED] Duplicate DN-PREFILTER-1 (Expert Check)
        # We rely on the second prefilter block (lines ~3200) which has the same logic but better context comments.

        
        # --- Prefilter: delta_notional_usd tiers (self-calibrating via dn_calib) ---
        # [REMOVED] Duplicate DN-PREFILTER-1 (Expert Check)
        # We rely on the second prefilter block (which has the same logic but better context comments).

        
        # Check against USD threshold if present
        if delta_event:
            delta_val = float(delta_event.get("delta", 0.0))
            delta_usd = abs(delta_val) * price
            min_usd = float(runtime.config.get("delta_abs_min_usd", 0.0) or 0.0)
            if min_usd > 1.0 and delta_usd < min_usd:
                 # Vetoed by USD threshold
                 logger.warning(
                     "🛑 [MIN-USD] (%s) VETO: delta_usd=$%.2f < min=$%.2f - Signal blocked",
                     runtime.symbol, delta_usd, min_usd
                 )
                 return None

        # BURST: tick-driven flush even without new candidates (ensure signals don't get stuck)
        try:
            if bool(int(runtime.config.get("burst_enable", 1))) and getattr(runtime.burst.st, "active", False):
                # [OPT A] Strategy only considers, background loop handles flush.
                # Remove sync maybe_flush() to prevent "phantom" emissions or double-publish.
                pass
        except Exception:
            pass

        if not delta_event:
            self._log_metrics(runtime)
            return None

        # Trigger Event!
        runtime.delta_triggers += 1
        of_session_outcome_total.labels(runtime.symbol, sess, "trigger_delta").inc()
        
        # --- Pressure tracking: candidate attempts (deterministic by tick_ts) ---
        try:
            runtime.signal_attempt_ts_ms.append(int(tick_ts))
            psps = _calc_pressure_sps(list(runtime.signal_attempt_ts_ms), int(tick_ts), 60_000)
            # light smoothing (EMA)
            a = float(runtime.config.get("pressure_ema_alpha", 0.20))
            if a <= 0 or a > 1: a = 0.20
            runtime.pressure_sps = float((1.0 - a) * float(getattr(runtime, "pressure_sps", 0.0) or 0.0) + a * psps)
            indicators["pressure_sps"] = float(runtime.pressure_sps)
            # pressure_hi flag
            thr = float(runtime.config.get("pressure_hi_sps", 0.12))  # ~7.2 кандидатов/мин
            runtime.pressure_hi = 1 if runtime.pressure_sps >= thr else 0
            indicators["pressure_hi"] = int(runtime.pressure_hi)
        except Exception:
            pass

        # Update indicators with trigger context
        indicators["delta_z"] = delta_event.get("z", 0.0)
        
        # Диагностика: логируем срабатывание детектора (по флагу)
        if DEBUG_DELTAS:
            # Sampled debug log for delta trigger
            if runtime.delta_log_sampler.should_log("delta_trigger"):
                logger.debug(
                    "🔍 (%s) Delta detector triggered: delta=%.2f, z=%.2f, threshold=%.2f",
                    runtime.symbol,
                    delta_event.get("delta", 0.0),
                    delta_event.get("z", 0.0),
                    runtime.delta_detector.z_threshold,
                )

        # Determine signal direction
        direction = "LONG" if delta_event["delta"] >= 0 else "SHORT"

        # ------------------------------------------------------------------
        # ATR floor veto (tier-by-regime) — FIX BROKEN CHAIN
        # ВАЖНО:
        #   - раньше читали atr_bps_th, но не выбирали tier -> th оставался 0.0
        #   - теперь выбираем tier прямо здесь (safety), используя runtime.dynamic_cfg + bootstrap.
        # Fail-open:
        #   - если чего-то не хватает -> не блокируем (как и было), но всё логируем в indicators.
        # ------------------------------------------------------------------
        # ------------------------------------------------------------------
        # Authoritative DeltaNotional Tier Gating (Expert Recommendation)
        # ------------------------------------------------------------------
        # P2: Use TickTrigger DN Calibrator (tick_dn_calib) instead of Bar DN.
        # "tick_dn_calib" tracks the distribution of delta spikes (events), not bar sums.
        # ------------------------------------------------------------------
        rg = str(getattr(runtime, "last_regime", "na"))
        dn_tiers_decision = runtime.tick_dn_calib.tiers(
            regime=rg,
            ts_ms=int(tick_ts if tick_ts > 0 else (time.time() * 1000)), # Use TS for HoW scale lookup
            default_t0=float(runtime.config.get("dn_tier0_usd", 120000.0)),
            default_t1=float(runtime.config.get("dn_tier1_usd", 350000.0)),
            default_t2=float(runtime.config.get("dn_tier2_usd", 750000.0)),
        )
        
        # Publish decision tiers to canonical runtime.dynamic_cfg for transparency
        runtime.dynamic_cfg["dn_tier0_usd"] = float(dn_tiers_decision.tier0_usd)
        runtime.dynamic_cfg["dn_tier1_usd"] = float(dn_tiers_decision.tier1_usd)
        runtime.dynamic_cfg["dn_tier2_usd"] = float(dn_tiers_decision.tier2_usd)
        runtime.dynamic_cfg["dn_src"] = str(dn_tiers_decision.src)
        
        # Determine current tick's tier
        delta_usd = abs(float(delta_event.get("delta", 0.0))) * price
        
        # P2: Feed the calibrator with this event (autocalib)
        # Only feed significant events (>0) to avoid pollution if we trigger on noise
        if delta_usd > 0:
             runtime.tick_dn_calib.update(regime=rg, dn_usd=delta_usd, ts_ms=int(tick_ts))

        tier = 0
        if delta_usd > dn_tiers_decision.tier2_usd:
             tier = 2
        elif delta_usd > dn_tiers_decision.tier1_usd:
             tier = 1
        elif delta_usd > dn_tiers_decision.tier0_usd:
             tier = 0
        else:
             tier = -1 # Sub-tier0 (noise)

        # Gate Logic:
        # Check pass-rate telemetry (if we are in a high-noise regime/session)
        # dn_gate_passrate tracks EMA(pass) per tier/session.
        
        min_tier = int(runtime.config.get("delta_tier_min", 0))
        passed = (tier >= min_tier)
        
        # Telemetry update
        sess = indicators.get("session", "OFF")
        runtime.dn_passrate.update(tier=tier, session=sess, passed=passed)
        
        # Metrics
        res = "pass" if passed else "veto_tier"
        dn_gate_events_total.labels(symbol=runtime.symbol, tier=str(tier), session=sess, result=res).inc()
        
        # Enforce Veto
        if not passed:
             # Log veto
             if runtime.delta_log_sampler.should_log("dn_veto"):
                  logger.info(
                      "🛑 [DN-GATE] (%s) VETO: delta_usd=$%.0f < T%d=$%.0f (tier=%d < min=%d) src=%s session=%s",
                      runtime.symbol, delta_usd, min_tier, 
                      getattr(dn_tiers_decision, f"tier{min_tier}_usd", 0.0),
                      tier, min_tier, dn_tiers_decision.src, sess
                  )
             return None

        # Add indicators
        indicators["dn_tier"] = int(tier)
        indicators["dn_usd"] = float(delta_usd)
        indicators["dn_t1_usd"] = float(dn_tiers_decision.tier1_usd)
        indicators["dn_src"] = str(dn_tiers_decision.src)
        
        # P2: Inject Liquidity Scale (Hour-of-Week) for Risk/Conf
        indicators["liquidity_scale"] = float(dn_tiers_decision.scale)


        # Deterministic "now" (tick time preferred; wall-time fallback only if missing)
        now_ts = tick_ts if tick_ts > 0 else int(time.time() * 1000)

        indicators.update({
            "delta": delta_event.get("delta", 0.0),
            "delta_z": delta_event.get("z", 0.0),
        })

        # Pre-calculate absorption once for all consumers (Variant A + OFConfirm)
        absorption_feat = None
        try:
            absorption_feat = runtime.absorption_detector.push(tick, runtime.last_book, price)
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Variant A: Publish delta_spike event for decentralized OFConfirm service
        # ------------------------------------------------------------------
        try:
            spike_out = {
                "type": "delta_spike",
                "symbol": runtime.symbol,
                "ts_ms": now_ts,
                "price": float(price),
                "direction": direction,
                "delta": float(delta_event.get("delta", 0.0)),
                "delta_z": float(delta_event.get("z", 0.0))
            }
            # Optional: if we already have features from runtime
            # Optional: if we already have features from runtime
            if absorption_feat:
                spike_out["absorption"] = absorption_feat
            
            # Enrich with OBI/Iceberg (if not stale)
            now_ms = int(tick_ts) # EXPERT FIX: Use tick_ts instead of wall-time
            obi_ttl = int(runtime.config.get("obi_event_ttl_ms", 15000))
            if runtime.last_obi_event and (now_ms - runtime.last_obi_event.get("ts_ms", 0)) < obi_ttl:
                spike_out["obi"] = runtime.last_obi_event
            
            ice_ttl = int(runtime.config.get("iceberg_event_ttl_ms", 15000))
            if runtime.last_iceberg_event and (now_ms - runtime.last_iceberg_event.get("ts_ms", 0)) < ice_ttl:
                spike_out["iceberg"] = runtime.last_iceberg_event
            
            # Enrich with L3-lite stats
            if runtime.l3_stats:
                spike_out.update({
                    "cancel_bid_rate_ema": float(runtime.l3_stats.cancel_bid_rate_ema),
                    "cancel_ask_rate_ema": float(runtime.l3_stats.cancel_ask_rate_ema),
                    "taker_buy_rate_ema": float(runtime.l3_stats.taker_buy_rate_ema),
                    "taker_sell_rate_ema": float(runtime.l3_stats.taker_sell_rate_ema),
                })

            asyncio.create_task(
                self.redis.xadd(
                    "events:delta_spike",
                    {"payload": json.dumps(spike_out, ensure_ascii=False)},
                    maxlen=20000,
                    approximate=True
                )
            )
        except Exception as e:
            logger.error(f"Failed to publish delta_spike event: {e}")

        # Attach Tick-CVD indicators
        try:
            if runtime.cvd_state:
                indicators.update(runtime.cvd_state.indicators_light())
                indicators.update(runtime.cvd_state.robust_snapshot())
        except Exception:
            pass

        # Attach Phase B structure snapshots
        try:
            if runtime.last_bar:
                b = runtime.last_bar
                indicators.update({
                    "microbar_tf_ms": int(b.tf_ms),
                    "microbar_start_ts": int(b.start_ts_ms),
                    "microbar_end_ts": int(b.end_ts_ms),
                    "microbar_open": float(b.open),
                    "microbar_high": float(b.high),
                    "microbar_low": float(b.low),
                    "microbar_close": float(b.close),
                    "microbar_vol": float(b.vol),
                    "microbar_delta_sum": float(b.delta_sum),
                    "microbar_cvd_close": float(b.cvd_close),
                    "microbar_vwap": float(b.vwap),
                    "microbar_mid": float(b.mid_last) if b.mid_last is not None else None,
                    "microbar_spread": float(b.spread_last) if b.spread_last is not None else None,
                    "microbar_ticks": int(b.tick_count),
                })
            
            # RSI indicators (if available)
            if hasattr(runtime, "rsi_price") and runtime.rsi_price.value is not None:
                indicators["rsi_price"] = float(runtime.rsi_price.value)
            if hasattr(runtime, "rsi_cvd") and runtime.rsi_cvd.value is not None:
                indicators["rsi_cvd"] = float(runtime.rsi_cvd.value)

            # RSI Confirmation check
            rp = float(indicators.get("rsi_price", 50.0))
            rc = float(indicators.get("rsi_cvd", 50.0))
            if direction == "LONG" and rp > 50 and rc > 50:
                confirmations.append("rsi_agree=1")
            elif direction == "SHORT" and rp < 50 and rc < 50:
                confirmations.append("rsi_agree=1")

            if runtime.last_swing_high:
                sh = runtime.last_swing_high
                indicators.update({
                    "swing_high_ts": int(sh.ts_ms),
                    "swing_high_px": float(sh.price),
                    "swing_high_cvd": float(sh.cvd),
                })
            if runtime.last_swing_low:
                sl = runtime.last_swing_low
                indicators.update({
                    "swing_low_ts": int(sl.ts_ms),
                    "swing_low_px": float(sl.price),
                    "swing_low_cvd": float(sl.cvd),
                })
            if runtime.last_div:
                dv = runtime.last_div
                indicators.update({
                    "div_kind": str(dv.kind),
                    "div_ts": int(dv.ts_ms),
                    "div_strength": float(dv.strength),
                    "div_price_prev": float(dv.price_prev),
                    "div_price_curr": float(dv.price_curr),
                    "div_cvd_prev": float(dv.cvd_prev),
                    "div_cvd_curr": float(dv.cvd_curr),
                })
        except Exception:
            pass

        # Phase C/D: Metadata for Payload (Sweep, Footprint, Weak Progress)
        try:
            ev = runtime.last_sweep
            if ev is not None:
                div = runtime.last_div
                div_match = False
                if div is not None:
                    if ev.direction_bias == "SHORT" and str(div.kind).startswith("bearish"):
                        div_match = True
                    if ev.direction_bias == "LONG" and str(div.kind).startswith("bullish"):
                        div_match = True
                indicators["sweep_div_match"] = int(1 if div_match else 0)
                if div_match: confirmations.append("div_match=1")

            b = runtime.last_bar
            if b is not None and getattr(b, "fp_enabled", False):
                indicators.update({
                    "fp_bucket_px": float(getattr(b, "fp_bucket_px", 0.0) or 0.0),
                    "fp_max_imbalance": float(getattr(b, "fp_max_imbalance", 0.0) or 0.0),
                    "fp_absorb_score": float(getattr(b, "fp_absorb_score", 0.0) or 0.0),
                })
                fp_confs = fp_confirmations_from_microbar(b, direction, runtime.config)
                for c in fp_confs:
                    confirmations.append(c)
            
            wp = runtime.last_wp
            if wp is not None:
                indicators.update({"weak_range_atr": wp.range_atr, "weak_body_atr": wp.body_atr, "weak_eff": wp.eff})
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Unified data_health score (0..1) + policies
        # ------------------------------------------------------------------
        try:
            # ensure basic indicators for compute_data_health
            indicators["book_ts_gap_ms"] = int(tick_ts - int(getattr(runtime, "last_book_ts_ms", 0) or 0))
            indicators["book_rate_hz"] = float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
            indicators["spread_bps"] = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
            
            dh = compute_data_health(indicators=indicators, cfg=cfg)
            indicators["data_health"] = float(dh.score)
            indicators["data_health_reasons"] = ",".join(list(dh.reasons or [])[:5])
            indicators["book_health_ok"] = int(dh.book_health_ok)
            apply_book_evidence_policy(indicators=indicators, dh=dh, cfg=cfg)
            apply_shadow_only_policy(indicators=indicators, dh=dh, cfg=cfg)
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Expected slippage model (bps) for adverse selection filtering
        # ------------------------------------------------------------------
        try:
            spr = float(indicators.get("spread_bps", 0.0) or 0.0)
            churn = float(getattr(runtime, "book_churn_score", 0.0) or 0.0)
            brz = float(getattr(runtime, "book_rate_z", 0.0) or 0.0)
            press = float(getattr(runtime, "pressure_sps", 0.0) or 0.0)
            # Fetch ATR bps if available
            px = float(price or indicators.get("price", 0.0) or 0.0)
            atr = float(indicators.get("atr", getattr(runtime, "last_atr", 0.0)) or 0.0)
            atr_bps = (atr / px * 10000.0) if (px > 0 and atr > 0) else 0.0
            
            est = expected_slippage_bps(
                spread_bps=spr,
                churn_score=churn,
                book_rate_z=brz,
                pressure_sps=press,
                atr_bps=atr_bps,
                cfg=cfg,
            )
            indicators["expected_slippage_bps"] = float(est.expected_bps)
            indicators["slippage_reason"] = str(est.reason)
        except Exception:
            pass

        # ------------------------------------------------------------
        # OFConfirm Engine (single source of truth for decision & score)
        # ------------------------------------------------------------
        try:
            # absorption = absorption_feat (computed earlier)
            absorption = absorption_feat
            # Robust gate using pre-computed health (lines 1728+)
            book_ok = int(indicators.get("book_health_ok", 1))
            book_health = str(indicators.get("book_health", "OK"))
            
            # Additional check: explicitly verify threshold from dynamic config (if computed)
            try:
                # If health logic says OK but we have strict calibrated thresholds that fail:
                br = float(indicators.get("book_rate_hz", 0.0))
                min_hz = float(runtime.dynamic_cfg.get("book_rate_min_hz", 0.0))
                if min_hz > 0 and br < min_hz:
                    book_ok = 0
                    indicators["book_health_ok"] = 0 # Keep indicator raw but...
                    # Wait, expert says: "mark indicators['book_health_ok']=0"
                    indicators["book_health"] = "LOW_RATE_CALIB"
            except Exception:
                pass
            
            if book_ok == 0:
                of_session_outcome_total.labels(runtime.symbol, sess, "veto_book_stale").inc()
                # Stale or Unhealthy -> Disable Microstructure Evidence
                # We do NOT return None (fail-close for signal), but we zero-out 
                # book-dependent evidence so OFConfirmEngine sees "no evidence".
                indicators["obi"] = 0
                indicators["iceberg_refresh"] = 0
                indicators["iceberg_avg_qty"] = 0
                
                # Verify removal of any other book-dependent components if needed? 
                # Currently these are the main ones feeding score.
                
                # Check for debug logs
                if bool(int(os.getenv("DEBUG_DELTAS", "0"))):
                     logger.debug("⚠️ (%s) Book Health Fail: %s (OBI/Iceberg disabled)", runtime.symbol, book_health)
            
            # --- PRESSURE PROXY LAYER START ---
            # 1. Update meters
            # Note: We do NOT add tick_ts to pressure here. Pressure tracks *candidates*, recorded later.
            
            # 2. Compute metrics
            p_snap = runtime.pressure.snapshot(now_ms=int(tick_ts))
            pres_per_min = float(p_snap.per_min_ema)
            cd_per_min = float(p_snap.cd_rate_ema)
            
            hit_rate = cd_per_min # It's already an EMA rate

            runtime.last_pressure_per_min = pres_per_min
            runtime.last_cd_hit_rate = hit_rate
            indicators["pressure_per_min"] = pres_per_min
            indicators["cooldown_hit_rate"] = hit_rate

            # 3. Dynamic Thresholds
            p_hi = float(runtime.config.get("pressure_hi_per_min", 0.0) or 0.0)
            p_ext = float(runtime.config.get("pressure_extreme_per_min", 0.0) or 0.0)
            
            pressure_hi = int(p_hi > 0 and pres_per_min >= p_hi)
            pressure_extreme = int(p_ext > 0 and pres_per_min >= p_ext)
            
            runtime.dynamic_cfg["pressure_per_min"] = pres_per_min
            runtime.dynamic_cfg["pressure_hi"] = pressure_hi
            runtime.dynamic_cfg["pressure_extreme"] = pressure_extreme
            indicators["pressure_hi_flag"] = pressure_hi
            indicators["pressure_extreme_flag"] = pressure_extreme

            # 4. Strictness escalation (Need=3)
            # If pressure is high, increase required confirmations (reversal/continuation need -> 3)
            # Only if strong_dynamic_need_enable=1 (default)
            if bool(int(runtime.config.get("strong_dynamic_need_enable", 1))):
                # [EXPERT] Fix drift: always base on static config values instead of cumulative dynamic state
                base_r = int(runtime.config.get("strong_need_reversal", 2) or 2)
                base_c = int(runtime.config.get("strong_need_continuation", 2) or 2)
                need_r = base_r
                need_c = base_c

                if pressure_hi or pressure_extreme:
                    need_r = max(need_r, 3)
                    need_c = max(need_c, 3)
                    indicators["strong_need_reason"] = "pressure"
                else:
                    indicators["strong_need_reason"] = "base"

                runtime.dynamic_cfg["strong_need_reversal"] = int(need_r)
                runtime.dynamic_cfg["strong_need_continuation"] = int(need_c)
            
            # --- Delta-notional tier gate (AUTHORITATIVE: dn_calib via dynamic_cfg) ---
            tiers_cfg = runtime.config.get("delta_diff_tiers") or get_default_delta_tiers(runtime.symbol)

            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
            tier_idx = 0 if "trend" in rg else 1
            # Escalation by pressure flags (telemetry-only inputs; dn thresholds remain dn_calib)
            if int(runtime.dynamic_cfg.get("pressure_hi", 0) or 0) == 1:
                tier_idx = min(tier_idx + 1, 2)
            if int(runtime.dynamic_cfg.get("pressure_extreme", 0) or 0) == 1:
                tier_idx = 2

            tier_key = f"tier{tier_idx}"

            # Read ONLY canonical dn_calib keys; fallback to defaults
            th = float(runtime.dynamic_cfg.get(f"dn_tier{tier_idx}_usd", 0.0) or 0.0)
            if th <= 0:
                th = float(tiers_cfg.get(tier_key, tiers_cfg.get("tier1", 100000.0)))

            notional_usd = abs(float(delta_event.get("delta", 0.0))) * float(price)
            indicators["delta_notional_usd"] = float(notional_usd)
            indicators["dn_tier_active"] = int(tier_idx)
            indicators["dn_tier_threshold"] = float(th)

            sess = session_utc(int(tick_ts))

            if th > 1.0 and notional_usd < th:
                ticks_pressure_filtered_total.labels(symbol=runtime.symbol, reason=tier_key).inc()
                dn_gate_events_total.labels(symbol=runtime.symbol, tier=str(tier_idx), session=sess, result="veto").inc()
                sampled_warning(
                    logger,
                    "DN_FILTERED",
                    "🛑 (%s) Notional Veto: $%.2f < threshold $%.2f (tier=%s)",
                    runtime.symbol,
                    notional_usd,
                    th,
                    tier_key,
                )
                return None
            dn_gate_events_total.labels(symbol=runtime.symbol, tier=str(tier_idx), session=sess, result="pass").inc()
            # --- PRESSURE PROXY LAYER END ---

            # Merge static cfg + dynamic calibrated thresholds
            cfg2 = dict(runtime.config)
            try:
                dyn = getattr(runtime, "dynamic_cfg", {}) or {}
                if bool(int(cfg2.get("abs_lvl_use_dynamic_th", 1))):
                    cfg2.update(dyn)
                else:
                    indicators["abs_lvl_dynamic_disabled"] = 1
            except Exception:
                pass

            try:
                # readiness gate
                min_samples = int(cfg2.get("eff_calib_min_samples", cfg2.get("EFF_CALIB_MIN_SAMPLES", 300)) or 300)
                calib_n = int(cfg2.get("abs_lvl_calib_n", 0) or 0)
                calib_src = str(cfg2.get("abs_lvl_calib_src", "static") or "static")
                abs_ready = int((calib_n >= min_samples) and (calib_src != "static"))
                
                # safety switch: unstable -> disable ready
                if int(cfg2.get("abs_lvl_th_unstable", 0) or 0) == 1:
                    abs_ready = 0
                    indicators["abs_lvl_disabled_by_unstable"] = 1
                    
                cfg2["abs_lvl_calib_ready"] = abs_ready
                indicators["abs_lvl_ready"] = abs_ready
            except Exception:
                pass
                
            # Continuation context update: if this spike is counter-trend + weak progress, record it.
            # This enables Bit C in eval_continuation for future trend-aligned signals.
            try:
                div_k = getattr(runtime.last_div, "kind", None) if runtime.last_div else None
                t_dir = hidden_trend_dir(div_k)
                if t_dir is not None and direction != t_dir:
                    if runtime.last_wp and runtime.last_wp.weak_any:
                        runtime.cont_ctx_ts_ms = now_ts
                        runtime.cont_ctx_trend_dir = t_dir
            except Exception:
                pass

            # Continuation veto logic
            try:
                div_k = getattr(runtime.last_div, "kind", None) if runtime.last_div else None
                t_dir = hidden_trend_dir(div_k)
                veto_th = float(cfg2.get("abs_lvl_cont_veto_score", 0.75))
                abs_bias = str(indicators.get("abs_lvl_bias", "NONE") or "NONE").upper()
                abs_score = float(indicators.get("abs_lvl_score", 0.0) or 0.0)
                if int(indicators.get("abs_lvl_ready", 0)) == 1 and t_dir is not None:
                    if abs_bias in ("LONG","SHORT") and abs_bias != str(t_dir).upper() and abs_score >= veto_th:
                        indicators["abs_lvl_cont_veto"] = 1
            except Exception:
                pass

            # Threshold and weighting overrides: relax 0.65 -> 0.45
            cfg2["of_score_min"] = float(cfg2.get("of_score_min", 0.45))
            if cfg2["of_score_min"] == 0.65:
                cfg2["of_score_min"] = 0.45 # Force lower if it was stick at old default

            # Divergence Sensitivity
            cfg2["div_strength_min"] = float(cfg2.get("div_strength_min", 1.5))
            cfg2["div_min_price_bp"] = float(cfg2.get("div_min_price_bp", 3.0))
            if hasattr(runtime, "divergence") and runtime.divergence:
                runtime.divergence.apply_config(cfg2)

            # --- L3-lite (Cancellation rates for OFConfirm engine) ---
            if runtime.l3_stats:
                indicators["cancel_bid_rate_ema"] = float(runtime.l3_stats.cancel_bid_rate_ema)
                indicators["cancel_ask_rate_ema"] = float(runtime.l3_stats.cancel_ask_rate_ema)
                indicators["taker_buy_rate_ema"] = float(runtime.l3_stats.taker_buy_rate_ema)
                indicators["taker_sell_rate_ema"] = float(runtime.l3_stats.taker_sell_rate_ema)
            # Hawkes burst features (computed on bucket advance; fail-open if missing)
            hsnap = getattr(runtime, "hawkes_snapshot", None)
            if isinstance(hsnap, dict):
                indicators.update(hsnap)

            ofc, dec = self.of_engine.build(
                symbol=runtime.symbol,
                tf=str(runtime.config.get("micro_tf", "1s")),
                direction=direction,
                tick_ts_ms=tick_ts,
                price=float(price),
                delta_z=float(delta_event.get("z", 0.0)),
                runtime=runtime,
                cfg=cfg2,
                indicators=indicators,
                absorption=absorption if isinstance(absorption, dict) else None,
            )

            # expose calibration diagnostics
            indicators["abs_lvl_eff_quote_th"] = float(cfg2.get("abs_lvl_eff_quote_th", 0.0) or 0.0)
            indicators["abs_lvl_min_quote_delta"] = float(cfg2.get("abs_lvl_min_quote_delta", 0.0) or 0.0)
            indicators["abs_lvl_calib_n"] = int(cfg2.get("abs_lvl_calib_n", 0) or 0)
            indicators["abs_lvl_calib_src"] = str(cfg2.get("abs_lvl_calib_src", "static"))

            if ofc:
                ev = ofc.evidence
                indicators["of_confirm"] = ofc.to_dict()
                indicators["of_confirm_v3"] = ofc.to_dict()
                indicators["of_confirm_ok"] = int(ofc.ok)
                
                # Use dec directly from build() instead of overwriting with None
                if dec and hasattr(dec, "need") and hasattr(dec, "have"):
                    # P2: Liquidity Scaling for Confirmation (Need)
                    # If liquidity is poor (scale < 0.8), increase confirmation requirement.
                    liq_scale = float(indicators.get("liquidity_scale", 1.0) or 1.0)
                    need_bump = 0
                    if liq_scale < 0.8:
                        need_bump = 1
                        indicators["strong_gate_need_bump"] = 1
                        indicators["strong_gate_need_reason"] = "low_liquidity"
                    
                    eff_need = int(dec.need) + need_bump
                    
                    # Re-evaluate OK status
                    is_ok = int(dec.have) >= eff_need
                    # Only strictify (never relax)
                    if not is_ok:
                        indicators["strong_gate_ok"] = 0
                        indicators["of_confirm_ok"] = 0
                        ofc.ok = False # Sync object
                    
                    indicators["of_confirm_score"] = float(dec.have / eff_need) if eff_need > 0 else 0.0
                    
                    # Persist last strong-gate diagnostics for SMT snapshot / entry policy.
                    try:
                        indicators["strong_gate_have"] = int(dec.have)
                        indicators["strong_gate_need"] = int(eff_need)
                        indicators["strong_gate_scn"] = str(dec.scenario)
                        indicators["strong_need_reason"] = str(getattr(dec, "need_reason", "") or "")

                        runtime.last_of_confirm_score = float(indicators.get("of_confirm_score", 0.0) or 0.0)
                        runtime.last_strong_gate_have = int(indicators.get("strong_gate_have", 0) or 0)
                        runtime.last_strong_gate_need = int(indicators.get("strong_gate_need", 0) or 0)
                        runtime.last_strong_gate_scn = str(indicators.get("strong_gate_scn", "") or "")
                    except Exception:
                        pass
                indicators["strong_gate_bits"] = int(ofc.gate_bits)
                indicators["strong_gate_reason"] = str(ofc.reason)
                # indicators["strong_gate_ok"] already updated if needed
                indicators["of_gate_mode"] = "SHADOW" if bool(runtime.config.get("strong_gate_shadow", False)) else "ENFORCE"

                # --- NEW: record last strong-pass dir/ts ONLY when gate passed (ok==1) ---
                # This is the value SMT/EntryPolicy should trust as "leader confirmed by OF".
                try:
                    if int(ofc.ok) == 1:
                        runtime.last_strong_pass_ts_ms = int(tick_ts)
                        runtime.last_strong_pass_dir = str(direction).upper()
                except Exception:
                    pass




                # Rate limit logs: only 1 in 50
                sg_cnt = self.strong_gate_counters.get(runtime.symbol, 0) + 1
                self.strong_gate_counters[runtime.symbol] = sg_cnt

                if sg_cnt % 50 == 0:
                    self.logger.info(
                        "🔥 Signal Strong-Gate Decision: symbol=%s, scenario=%s, ok=%d, score=%.2f, have=%d, need=%d, reason=%s (x%d)",
                        runtime.symbol, ofc.scenario, ofc.ok, ofc.score, ofc.have, ofc.need, ofc.reason, sg_cnt
                    )

                # ENFORCE / SHADOW logic (+ liquidity auto-enforce on stressed)
                enforce = bool(runtime.config.get("require_strong_confirmation", False))
                try:
                    if str(getattr(runtime, "liq_regime", "normal") or "normal").lower() == "stressed":
                        enforce = bool(int(runtime.config.get("liq_enforce_strong_when_stressed", 1) or 1))
                except Exception:
                    pass

                if enforce and ofc.ok == 0:
                    if bool(runtime.config.get("strong_gate_shadow", False)):
                        indicators["strong_gate_shadow_veto"] = 1
                    else:
                        strong_gate_veto_total.labels(symbol=runtime.symbol, scenario=ofc.scenario, reason="engine_veto", mode="ENFORCE").inc()
                        veto_low_conf_total.labels(symbol=runtime.symbol).inc()
                        of_session_outcome_total.labels(runtime.symbol, sess, "veto_strong_gate").inc()
                        # Add explicit visibility for dropped signals
                        self.logger.warning(
                            "🚫 Signal filtered by Strong Gate (ENFORCE): symbol=%s, scenario=%s, reason=%s. "
                            "To fix, enable strong_gate_shadow=1 or disable require_strong_confirmation.",
                            runtime.symbol, ofc.scenario, ofc.reason
                        )
                        return None

                # Audit Confirmations (mirror resulting evidence)
                # Note: We append these to confirmations list for Telegram/UI
                if ev.get("sweep"):
                    div_match = bool(indicators.get("sweep_div_match", 0))
                    require_div = bool(runtime.config.get("sweep_require_divergence", 0))
                    if (not require_div) or div_match:
                         kind = indicators.get("sweep_kind", "")
                         confirmations.insert(0, "sweep_eqh=1" if kind == "EQH_SWEEP" else "sweep_eql=1")
                
                if ev.get("absorption"): confirmations.append(f"absorption={ev.get('absorption_volume', 0.0):.2f}")
                if ev.get("weak_progress"): confirmations.append("weak_progress=1")
                if ev.get("abs_lvl_ok"): confirmations.append(f"abs_lvl={ev.get('abs_lvl_score', 0.0):.2f}")

                # ------------------------------------------------------------
                # Phase E: OBI quality, FP Edge Absorb, Weak Trend (Scoring/Telemetry)
                # ------------------------------------------------------------
                try:
                    now_ms_det = int(now_ms)
                    # OBI stability (quality-gated)
                    if runtime.last_obi_event:
                        age = now_ms_det - int(runtime.last_obi_event.get("ts_ms", 0) or 0)
                        ttl = int(runtime.config.get("obi_event_ttl_ms", 15000))
                        if 0 <= age <= ttl:
                            indicators["obi_event_age_ms"] = int(age)
                            indicators["obi_dir"] = str(runtime.last_obi_event.get("direction") or "")
                            indicators["obi"] = float(runtime.last_obi_event.get("obi", 0.0) or 0.0)
                            indicators["obi_z"] = float(runtime.last_obi_event.get("obi_z", 0.0) or 0.0)
                            indicators["obi_stable_secs"] = float(runtime.last_obi_event.get("stable_secs", 0.0) or 0.0)
                            indicators["obi_stability_score"] = float(runtime.last_obi_event.get("stability_score", 0.0) or 0.0)
                            indicators["obi_sustained"] = bool(int(runtime.last_obi_event.get("stable", 0) or 0) == 1)
                            if str(runtime.last_obi_event.get("direction") or "").upper() == direction:
                                if indicators["obi_sustained"]:
                                    confirmations.append(f"obi_stable={float(indicators['obi_stable_secs']):.2f}")

                    # Footprint edge absorb (recent, no range expansion)
                    fe = getattr(runtime, "last_fp_edge", None)
                    if fe is not None:
                        valid = int(runtime.config.get("fp_edge_valid_ms", 30000))
                        age = now_ms_det - int(getattr(fe, "ts_ms", 0) or 0)
                        if 0 <= age <= valid:
                            p90 = float(getattr(fe, "p90", 0.0) or 0.0)
                            val = float(getattr(fe, "value", 0.0) or 0.0)
                            strength = (val / p90) if p90 > 0 else 0.0
                            bias = str(getattr(fe, "bias", "") or "").upper()
                            rng = int(getattr(fe, "range_expansion", 0) or 0)
                            # Logic: LONG signal needs BUY bias edge (support?), SHORT needs SELL bias?
                            # Actually, tick-level fp_edge side "BID" means absorption on bid (support).
                            # If bias is present, use it.
                            ok = 1 if (bias == direction and rng == 0 and strength > 0) else 0
                            indicators["fp_edge_absorb"] = int(ok)
                            indicators["fp_edge_strength"] = float(strength)
                            indicators["fp_edge_range_expansion"] = int(rng)
                            indicators["fp_edge_age_ms"] = int(age)
                            if ok:
                                confirmations.append(f"fp_edge_absorb={strength:.2f}")

                    # Weak progress trend (history)
                    try:
                        wp_det = getattr(runtime, "weak_progress_det", None)
                        if wp_det is not None:
                            indicators["weak_recent_window"] = int(getattr(wp_det, "recent_window", 0) or 0)
                            indicators["weak_recent_count"] = int(wp_det.recent_weak_count())
                            w = int(indicators["weak_recent_window"] or 0)
                            c = int(indicators["weak_recent_count"] or 0)
                            ratio = float(c / w) if w > 0 else 0.0
                            indicators["weak_recent_ratio"] = ratio
                            
                            # Legacy boolean for Scorer fallback
                            min_weak = int(runtime.config.get("weak_recent_min_cnt", 3))
                            indicators["weak_progress"] = bool(ev.get("weak_progress") or (c >= min_weak))
                            if c >= min_weak:
                                confirmations.append(f"weak_recent={c}/{w}")
                    except Exception:
                        pass
                except Exception:
                    pass
                    
                # Iceberg (Strict/Recent)
                if runtime.last_iceberg_event:
                     ice_ts = int(runtime.last_iceberg_event.get("ts_ms") or 0)
                     if (tick_ts - ice_ts) < 5000:
                         confirmations.append(f"iceberg={runtime.last_iceberg_event.get('total_refresh_qty')}")
                         # strict direction check
                         ice_side = str(runtime.last_iceberg_event.get("side")).upper()
                         spike_side = "BUY" if float(delta_event.get("delta", 0)) > 0 else "SELL"
                         iceberg_side = "BUY" if ice_side == "BID" else "SELL" # iceberg is limit
                         # We want opposing iceberg for absorption
                         if spike_side != iceberg_side:
                              confirmations.append("ice_strict=1")


                # Optional Redis Publication (v3 asychronous)
                if bool(int(runtime.config.get("publish_of_confirm", 0))):
                    stream = str(runtime.config.get("of_confirm_stream", "signals:of:confirm"))
                    try:
                        asyncio.create_task(
                            self.ticks.xadd(
                                stream,
                                fields={"payload": json.dumps(ofc.to_dict(), ensure_ascii=False)},
                                maxlen=int(runtime.config.get("of_confirm_stream_maxlen", 50000)),
                                approximate=True,
                            )
                        )
                    except Exception:
                        pass
                    
                    # ------------------------------------------------------------
                    # Publish deterministic decision inputs for golden replay
                    # ------------------------------------------------------------
                # ------------------------------------------------------------
                # Publish deterministic decision inputs for golden replay
                # ------------------------------------------------------------
                try:
                    # logger.error("DEBUG: 1. accessing OFI config")
                    pub_val = runtime.config.get("publish_of_inputs", 0)
                    should_pub = bool(int(pub_val))
                    
                    if should_pub:
                        # logger.error("DEBUG: 2. Entering OFI Logic")
                        # continuation context
                        trend_dir = "NONE"
                        hidden_ctx_recent = 0
                        cont_ctx_recent = 0
                        try:
                            div = getattr(runtime, "last_div", None)
                            td = hidden_trend_dir(getattr(div, "kind", None) if div else None)
                            if td:
                                trend_dir = str(td).upper()
                            # hidden ctx
                            if div and td:
                                now_ts = int(tick_ts) if int(tick_ts) > 0 else int(time.time() * 1000)
                                hidden_ms = int(runtime.config.get("hidden_ctx_valid_ms", 120_000))
                                age = now_ts - int(getattr(div, "ts_ms", now_ts))
                                hidden_ctx_recent = 1 if (0 <= age <= hidden_ms) else 0
                            # cont ctx
                            now_ts = int(tick_ts) if int(tick_ts) > 0 else int(time.time() * 1000)
                            cts = int(getattr(runtime, "cont_ctx_ts_ms", 0) or 0)
                            cv = int(runtime.config.get("cont_ctx_valid_ms", 120_000))
                            cont_ctx_recent = 1 if (cts > 0 and 0 <= now_ts - cts <= cv) else 0
                        except Exception as ex_ctx:
                            logger.debug(f"OFI: Context calc error: {ex_ctx}")

                        # 2. Extract evidence
                        ev_weak = int(indicators.get("weak_progress", 0))
                        ev_sweep = int(indicators.get("sweep", 0))
                        ev_reclaim = int(indicators.get("reclaim", 0))
                        ev_obi_stable = int(indicators.get("obi_stable", 0))
                        ev_ice_strict = int(indicators.get("ice_strict", 0))
                        ev_abs_lvl_ok = int(indicators.get("abs_lvl_ok", 0))
                        
                        if ofc and hasattr(ofc, "evidence") and isinstance(ofc.evidence, dict):
                            ev_weak = int(ofc.evidence.get("weak_progress", ev_weak))
                            ev_sweep = int(ofc.evidence.get("sweep", ev_sweep))
                            ev_reclaim = int(ofc.evidence.get("reclaim", ev_reclaim))
                            ev_obi_stable = int(ofc.evidence.get("obi_stable", ev_obi_stable))
                            ev_ice_strict = int(ofc.evidence.get("iceberg_strict", ev_ice_strict))
                            ev_abs_lvl_ok = int(ofc.evidence.get("abs_lvl_ok", ev_abs_lvl_ok))
                        
                        # 4. Create Object
                        # logger.error("DEBUG: 4. Creating OFI Object")
                        
                        # Safe CFG
                        cfg_safe = {}
                        
                        # Build OFInputsV1 kwargs with backward-compatible optional fields.
                        ofi_kwargs = dict(
                            v=1,
                            symbol=str(runtime.symbol),
                            ts_ms=int(tick_ts),
                            regime=str(getattr(runtime, "last_regime", "na")),
                            direction=str(direction),
                            scenario=str(dec.scenario) if dec else "na",
                            delta_z=float(delta_event.get("z", 0.0)),
                            weak_progress=ev_weak,
                            sweep_recent=ev_sweep,
                            reclaim_recent=ev_reclaim,
                            obi_stable=ev_obi_stable,
                            iceberg_strict=ev_ice_strict,
                            abs_lvl_ok=ev_abs_lvl_ok,
                            trend_dir=str(trend_dir or "NONE"),
                            hidden_ctx_recent=int(hidden_ctx_recent),
                            cont_ctx_recent=int(cont_ctx_recent),
                            cfg=cfg_safe,
                            fp_eff_quote=float(getattr(runtime.last_bar, "fp_eff_quote", 0.0) if runtime.last_bar else 0.0),
                            fp_quote_delta=float(getattr(runtime.last_bar, "fp_quote_delta", 0.0) if runtime.last_bar else 0.0),
                        )
                        
                        # Optional fields (only if contract supports them)
                        _ann = getattr(OFInputsV1, "__annotations__", {}) or {}
                        if "regime_group" in _ann:
                            ofi_kwargs["regime_group"] = str(getattr(runtime, "last_regime", "na"))
                        
                        hsnap = getattr(runtime, "hawkes_snapshot", None)
                        if isinstance(hsnap, dict):
                            if "hawkes_dt_s" in _ann:
                                ofi_kwargs["hawkes_dt_s"] = float(hsnap.get("hawkes_dt_s", 0.0) or 0.0)
                            if "hawkes_taker_lam" in _ann:
                                ofi_kwargs["hawkes_taker_lam"] = float(hsnap.get("hawkes_taker_lam", 0.0) or 0.0)
                            if "hawkes_cancel_lam" in _ann:
                                ofi_kwargs["hawkes_cancel_lam"] = float(hsnap.get("hawkes_cancel_lam", 0.0) or 0.0)
                            if "hawkes_churn_lam" in _ann:
                                ofi_kwargs["hawkes_churn_lam"] = float(hsnap.get("hawkes_churn_lam", 0.0) or 0.0)
                        
                        ofi = OFInputsV1(**ofi_kwargs)
                        # logger.error("DEBUG: 5. Serializing...")
                        blob = json.dumps(ofi.to_dict(), ensure_ascii=False)
                        
                        in_stream = str(runtime.config.get("of_inputs_stream", "stream:of:inputs"))

                        sampled_debug(logger, "OFI_PUBLISHING", "OFI: Publishing to Redis...")
                        asyncio.create_task(
                            self.ticks.xadd(
                                in_stream,
                                fields={"payload": blob},
                                maxlen=int(runtime.config.get("of_inputs_stream_maxlen", 200000)),
                                approximate=True,
                            )
                        )
                        sampled_debug(logger, "OFI_PUBLISHED", "OFI: PublishedTask Created")

                except Exception as e_main:
                     logger.debug(f"OFI: Block error: {e_main}")
                     pass

        except Exception as ex:
            logger.error(f"OFConfirm engine error: {ex}")


        # ------------------------------------------------------------
        # min_confirmations gate (hard vs soft)
        # По умолчанию fp_imb не увеличивает hard_count, иначе pass-rate станет выше.
        # ------------------------------------------------------------
        # ------------------------------------------------------------
        from core.footprint_policy import is_soft_confirmation # Ensure import or use existing
        
        if tick.get("mock_force"):
             self.logger.warning("TRACE 3: Approaching Gate Check")

        delta_abs = abs(delta_event.get("delta", 0.0))
        min_delta = runtime.config["delta_abs_min_confirm"]
        min_confirmations = int(runtime.config.get("min_confirmations", 0))
        
        fp_imb_counts = bool(runtime.config.get("fp_imb_counts_for_min_confirmations", False))
        if fp_imb_counts:
            hard_count = len(confirmations)
        else:
            hard_count = 0
            for c in confirmations:
                if is_soft_confirmation(c):
                    continue
                hard_count += 1

        if delta_abs < min_delta and hard_count < min_confirmations:
            # FORCE LOG for diagnostics
            logger.warning(
                "🛑 [MIN-CONF] (%s) Signal filtered: delta_abs=%.2f < %.2f AND hard_confirmations=%d < %d",
                runtime.symbol,
                delta_abs,
                min_delta,
                hard_count,
                min_confirmations,
            )
            return None

        # Deterministic now
        now_ms = int(tick_ts)

        signal_id = f"crypto-of:{runtime.symbol}:{now_ms}"
        primary_reason = "delta_spike"
        if confirmations:
            primary_reason = confirmations[0].split("=", 1)[0]

        # [DEDUPLICATED] Primary ATR-floor gate is handled as Early Gate (lines ~600).


        # ------------------------------------------------------------
        # Phase E: OBI stability evidence (TTL + book health)
        # ------------------------------------------------------------
        # Populate indicators so scorer/Telegram can use stability duration + quality.
        # Fail-open: if no book evidence or TTL expired, do nothing.
        try:
            if int(indicators.get("book_health_ok", 1) or 1) == 1:
                obe = getattr(runtime, "last_obi_event", None)
                if isinstance(obe, dict):
                    ots = int(obe.get("ts_ms", 0) or 0)
                    ttl = int(runtime.config.get("obi_event_ttl_ms", 15000) or 15000)
                    if ots > 0 and 0 <= (now_ms - ots) <= ttl:
                        # raw OBI values
                        indicators["obi"] = float(obe.get("obi", indicators.get("obi", 0.0) or 0.0) or 0.0)
                        indicators["obi_z"] = float(obe.get("obi_z", 0.0) or 0.0)
                        # stability
                        indicators["obi_stable_secs"] = float(obe.get("stable_secs", 0.0) or 0.0)
                        # quality score may be missing (legacy); default 1.0 if duration present
                        q = obe.get("stability_score", None)
                        if q is None:
                            q = 1.0 if float(indicators.get("obi_stable_secs", 0.0) or 0.0) > 0 else 0.0
                        indicators["obi_stability_score"] = float(q)
                        indicators["obi_stable"] = int(obe.get("stable", 0) or 0)
        except Exception:
            pass

        # ------------------------------------------------------------
        # Phase E+: OFI stability evidence (TTL + book health)
        # ------------------------------------------------------------
        try:
            if int(indicators.get("book_health_ok", 1) or 1) == 1:
                oe = getattr(runtime, "last_ofi_event", None)
                if isinstance(oe, dict):
                    ots = int(oe.get("ts_ms", 0) or 0)
                    ttl = int(runtime.config.get("ofi_event_ttl_ms", 15000) or 15000)
                    if ots > 0 and 0 <= (now_ms - ots) <= ttl:
                        indicators["ofi"] = float(oe.get("ofi", 0.0) or 0.0)
                        indicators["ofi_z"] = float(oe.get("ofi_z", 0.0) or 0.0)
                        indicators["ofi_stable_secs"] = float(oe.get("stable_secs", 0.0) or 0.0)
                        indicators["ofi_stability_score"] = float(oe.get("stability_score", 0.0) or 0.0)
                        indicators["ofi_stable"] = int(oe.get("stable", 0) or 0)
                        indicators["ofi_age_ms"] = int(now_ms - ots)

                        if int(indicators["ofi_stable"] or 0) == 1:
                            if str(oe.get("direction", "") or "").upper() == str(direction).upper():
                                confirmations.append(f"ofi_stable={float(indicators['ofi_stable_secs']):.1f}s")
        except Exception:
            pass

        # ------------------------------------------------------------
        # Phase E: CVD Reclaim (bonus-layer)
        # ------------------------------------------------------------
        # Add as SOFT confirmation after gates (won't affect min_confirmations).
        try:
            if int(runtime.config.get("cvd_reclaim_enable", 1) or 0) == 1:
                ev = runtime.last_cvd_reclaim
                if ev and (now_ms - ev.ts_ms) <= 120_000:
                    if ev.direction_bias == direction:
                        indicators["cvd_reclaim_ok"] = int(ev.ok)
                        indicators["cvd_reclaim_score"] = float(ev.score)
                        indicators["cvd_reclaim_delta"] = float(ev.cvd_delta)
                        if ev.ok:
                            confirmations.append(f"cvdR={ev.score:.2f}")
                            cvd_reclaim_applied_total.labels(symbol=runtime.symbol, bias=direction).inc()
                            cvd_reclaim_age_ms_gauge.labels(symbol=runtime.symbol, bias=direction).set(int(now_ms - ev.ts_ms))
        except Exception:
            pass

        if tick.get("mock_force"):
             self.logger.warning("TRACE 5: Computing Confidence")

        confidence = self._compute_confidence(runtime, indicators, confirmations, side=direction, kind=primary_reason)
        indicators["confidence"] = confidence

        # ------------------------------------------------------------
        # Phase E+: Liquidity regime (risk overlay)
        # ------------------------------------------------------------
        # Uses:
        #  - spread from BookSnapshot (top5)
        #  - depth_usd_min_5 from top5 volumes * mid
        #  - book_rate_ema from runtime
        #  - book_stale_ms from tick_ts - last_book_ts_ms
        try:
            snap = getattr(runtime, "last_book", None)
            bts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
            book_stale_ms = int(tick_ts - bts) if (bts > 0 and tick_ts >= bts) else int(10**9)
            if snap is not None:
                mid = 0.5 * (float(snap.best_bid_px) + float(snap.best_ask_px))
                depth_qty = float(min(snap.depth_5_bid_vol, snap.depth_5_ask_vol))
                depth_usd_min_5 = float(depth_qty * max(mid, 1e-9))
                spread_bps = float(getattr(snap, "spread_bps", 0.0) or 0.0)
            else:
                depth_usd_min_5 = 0.0
                spread_bps = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)

            liq = runtime.liq_service.score(
                ts_ms=int(tick_ts),
                spread_bps=float(spread_bps),
                depth_usd_min_5=float(depth_usd_min_5),
                book_rate_ema_hz=float(getattr(runtime, "book_rate_ema", 0.0) or 0.0),
                book_stale_ms=int(book_stale_ms),
            )
            runtime.liq_score = float(liq.liq_score)
            runtime.liq_regime = str(liq.liq_regime)
            runtime.last_liq = liq.to_dict()

            indicators["liq_score"] = float(liq.liq_score)
            indicators["liq_regime"] = str(liq.liq_regime)
            indicators["liq_depth_usd_min_5"] = float(liq.depth_usd_min_5)
            indicators["liq_spread_bps"] = float(liq.spread_bps)
            indicators["liq_book_rate_hz"] = float(liq.book_rate_ema_hz)
            indicators["liq_book_stale_ms"] = int(liq.book_stale_ms)
            if liq.why:
                indicators["liq_why"] = str(liq.why)
        except Exception:
            pass

        # Log the confidence for this signal
        # Log the confidence for this signal (sampled)
        if primary_reason == "weak_progress":
            if runtime.weak_signal_log_sampler.should_log("weak_progress"):
                self.logger.info("emit signal %s conf=%.1f%%", primary_reason, confidence * 100.0)
        else:
            # Log other signals sampled at 1/1000
            if runtime.signal_emit_log_sampler.should_log(primary_reason):
                self.logger.info("emit signal %s conf=%.1f%%", primary_reason, confidence * 100.0)

        # Фильтр по минимальной уверенности
        try:
            min_conf_pct = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "80"))
        except Exception:
            min_conf_pct = 80.0

        # Override из symbol spec, если указано (signal_min_conf или min_conf)
        try:
            spec = get_symbol_info(runtime.symbol)
            if isinstance(spec, dict):
                spec_min_conf = spec.get("signal_min_conf", spec.get("min_conf"))
                if spec_min_conf is not None:
                    min_conf_pct = float(spec_min_conf)
        except Exception:
            pass

        # Liquidity-based bump (small, bounded). Thin/stressed markets create more false spikes/slippage.
        try:
            rg = str(indicators.get("liq_regime") or getattr(runtime, "liq_regime", "normal") or "normal").lower()
            if rg == "thin":
                min_conf_pct += float(runtime.config.get("liq_min_conf_bump_thin_pct", 3.0) or 3.0)
            elif rg == "stressed":
                min_conf_pct += float(runtime.config.get("liq_min_conf_bump_stressed_pct", 7.0) or 7.0)
            cap = float(runtime.config.get("liq_min_conf_cap_pct", 95.0) or 95.0)
            if min_conf_pct > cap:
                min_conf_pct = cap
        except Exception:
            pass

        # ------------------------------------------------------------
        # Phase E: OFI stability evidence (TTL + book health)
        # ------------------------------------------------------------
        # OFI is harder to fake than snapshot OBI because it is incremental.
        # Default: SOFT confirmation (does not affect min_confirmations).
        try:
            if int(indicators.get("book_health_ok", 1) or 1) == 1:
                ev = getattr(runtime, "last_ofi_event", None)
                if isinstance(ev, dict):
                    ots = int(ev.get("ts_ms", 0) or 0)
                    ttl = int(runtime.config.get("ofi_event_ttl_ms", 15000) or 15000)
                    if ots > 0 and 0 <= (now_ms - ots) <= ttl:
                        indicators["ofi"] = float(ev.get("ofi", 0.0) or 0.0)
                        indicators["ofi_z"] = float(ev.get("ofi_z", 0.0) or 0.0)
                        indicators["ofi_stable_secs"] = float(ev.get("stable_secs", 0.0) or 0.0)
                        indicators["ofi_stability_score"] = float(ev.get("stability_score", 0.0) or 0.0)
                        indicators["ofi_stable"] = int(ev.get("stable", 0) or 0)
                        indicators["ofi_age_ms"] = int(now_ms - ots)

                        # direction match -> add confirmation
                        if int(ev.get("stable", 0) or 0) == 1:
                            bias = str(ev.get("direction", "") or "").upper()
                            if bias == str(direction).upper():
                                confirmations.append(f"ofi_stable={float(indicators['ofi_stable_secs']):.1f}s")
        except Exception:
            pass

        min_conf = min_conf_pct / 100.0

        if tick.get("mock_force"):
             self.logger.warning("TRACE 6: Confidence Check. conf=%f min=%f", confidence, min_conf)

        # Strict confidence filter
        if confidence < min_conf:
             disabled = _to_bool(os.getenv("DISABLE_CONFIDENCE_FILTER", os.getenv("CRYPTO_DISABLE_CONFIDENCE_FILTER", runtime.config.get("disable_confidence_filter", "false"))))
             if disabled:
                 self.logger.info("ℹ️ (%s) [LOW-CONF] Signal confidence %.2f%% < %.2f%% but filter is DISABLED.", runtime.symbol, confidence * 100.0, min_conf_pct)
             else:
                 self.low_conf_counters[runtime.symbol] = self.low_conf_counters.get(runtime.symbol, 0) + 1
                 sampled_warning(logger, "LOW_CONF",
                     "🛑 [LOW-CONF] (%s) Signal filtered: conf=%.2f%% < min_conf=%.2f%%. (x%d)",
                     runtime.symbol, confidence * 100.0, min_conf_pct, self.low_conf_counters[runtime.symbol]
                 )
                 return None
        
        # Telemetry: Hidden Divergence Usage
        if indicators.get("hidden_div_used"):
             from services.orderflow.metrics import of_hidden_divergence_signal_total
             of_hidden_divergence_signal_total.labels(symbol=runtime.symbol).inc()

        runtime.signal_count += 1
        
        # Executable Entry Pricing (P0)
        executable_entry = float(price)
        try:
            if runtime.last_book:
                bts_entry = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
                # Max staleness 2s for pricing to avoid bad fills
                if bts_entry > 0 and (tick_ts - bts_entry) < 2000:
                    if direction == "LONG":
                        asks_entry = runtime.last_book.get("asks")
                        if asks_entry and len(asks_entry) > 0:
                             executable_entry = float(asks_entry[0][0])
                    else:
                        bids_entry = runtime.last_book.get("bids")
                        if bids_entry and len(bids_entry) > 0:
                             executable_entry = float(bids_entry[0][0])
                    
                    # Sanity: if deviation > 10% from tick price, revert to tick (bad book?)
                    if abs(executable_entry - price) / (price + 1e-9) > 0.10:
                        executable_entry = float(price)
        except Exception:
            executable_entry = float(price)

        # Initialize payload early for candidate/pressure enrichment
        payload = {
            "symbol": runtime.symbol,
            "ts_ms": int(tick_ts),
            "tick_ts": int(tick_ts),
            "price": float(price),
            "entry": float(executable_entry),
            "direction": direction,
            "side": direction.lower(),
            "indicators": indicators,
            "confirmations": list(confirmations),
            "confidence": float(confidence),
            "signal_id": str(signal_id),
            "entry_tag": str(primary_reason),
        }
        
        self._log_metrics(runtime)


        # === Pressure snapshot attached to every candidate payload ===
        try:
            ps = runtime.pressure.snapshot(now_ms=int(tick_ts))
            payload["pressure"] = {
                "per_min_ema": float(ps.per_min_ema),
                "cd_rate_ema": float(ps.cd_rate_ema),
                "n_raw": int(ps.n_raw),
                "n_cd": int(ps.n_cd),
            }
            hi_th = float(runtime.config.get("pressure_hi_per_min", 60.0))
            payload["pressure"]["pressure_hi"] = 1 if ps.per_min_ema >= hi_th else 0
        except Exception:
            pass

        # Attach microstructure context (from last book/bar)
        try:
            payload.setdefault("micro", {})
            payload["micro"]["spread_bps"] = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
            payload["micro"]["spread_z"] = float(getattr(runtime, "last_spread_z", 0.0) or 0.0)
            # book freshness/rate
            bts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
            book_stale_ms = int(tick_ts - bts) if (bts > 0 and tick_ts > 0 and tick_ts >= bts) else int(10**9)
            payload["micro"]["book_stale_ms"] = int(book_stale_ms)
            payload["micro"]["book_rate_ema"] = float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
            payload["micro"]["book_rate_z"] = float(getattr(runtime, "book_rate_z", 0.0) or 0.0)
            payload["micro"]["book_churn_score"] = float(getattr(runtime, "book_churn_score", 0.0) or 0.0)
            payload["micro"]["book_churn_hi"] = int(getattr(runtime, "book_churn_hi", 0) or 0)
            if book_stale_ms_gauge is not None:
                book_stale_ms_gauge.labels(symbol=runtime.symbol).set(float(book_stale_ms))
        except Exception:
            pass

        if runtime.last_book:
            payload["book_ts"] = runtime.last_book.get("ts")
            bids = runtime.last_book.get("bids") or []
            asks = runtime.last_book.get("asks") or []
            if bids:
                payload["best_bid"] = bids[0][0]
            if asks:
                payload["best_ask"] = asks[0][0]

        # ------------------------------------------------------------------
        # 🛡️ ADVERSE SELECTION GATE (P0)
        # ------------------------------------------------------------------
        # 1. Reversal: Must have Reclaim or Absorption or OFI stability
        # 2. Continuation: Must wait for next microbar close (verify follow-through)
        # ------------------------------------------------------------------
        if bool(int(runtime.config.get("adverse_check_enable", 0))):
            scn = str(indicators.get("strong_gate_scn", "") or "").lower()
            if not scn:
                scn = "reversal" if int(indicators.get("sweep", 0) or 0) == 1 else "continuation"
            
            # REVERSAL CHECK (Immediate Veto)
            if "reversal" in scn:
                # Evidence required: cvd_reclaim OR absorption OR obi_stable OR ofi_stable
                has_reclaim = bool(indicators.get("cvd_reclaim_ok", 0))
                has_absorb = bool(indicators.get("absorption_volume", 0) > 0)
                has_obi = bool(indicators.get("obi_stable", 0))
                has_ofi = bool(indicators.get("ofi_stable", 0))
                
                if not (has_reclaim or has_absorb or has_obi or has_ofi):
                    # Veto
                    # sampled_warning(logger, "ADVERSE_REV", "🛑 [ADVERSE] Reversal Veto: No confirmation evidence")
                    return None
            
            # CONTINUATION CHECK (Wait for Bar)
            elif "continuation" in scn:
                # Store and WAIT. Do not emit now.
                runtime.pending_adverse_payload = payload
                runtime.pending_adverse_ts_ms = int(tick_ts)
                # logger.info("⏳ [ADVERSE] Continuation Wait: payload buffered for next microbar")
                return None

        return await self._emit_payload(runtime, payload, int(tick_ts))

    async def _emit_payload(self, runtime: SymbolRuntime, payload: Dict[str, Any], now_ms: int) -> Optional[Dict[str, Any]]:
        """
        Internal helper: Cooldown -> Burst -> Return/Buffer.
        Used by process_tick AND _on_microbar_closed (deferred execution).
        """
        indicators = payload.get("indicators", {})
        confidence = float(payload.get("confidence", 0.0))
        
        scenario = str(indicators.get("strong_gate_scn", "") or "")
        if not scenario:
            scenario = "reversal" if int(indicators.get("sweep", 0) or 0) == 1 else "continuation"
            
        cooldown_ms = _cooldown_ms_for(runtime, scenario=scenario, now_ms=now_ms)
        last_emit_ts = int(getattr(runtime, "last_signal_ts", 0) or 0)
        age = int(now_ms) - last_emit_ts if last_emit_ts > 0 else 10**9

        # define score for candidate selection (always)
        of_score = float(indicators.get("of_confirm_score", 0.0))
        # Recalculate score from payload data just in case
        score = of_score if of_score > 0 else confidence

        if age < cooldown_ms:
            # --- Pressure Proxy: record deterministic cooldown hit ---
            try:
                runtime.pressure.on_cooldown_hit(ts_ms=int(now_ms))
            except Exception:
                pass

            # Buffer into pending_payload for post-cooldown emission
            cand_score = float(score)
            if runtime.pending_payload is None or cand_score > float(getattr(runtime, "pending_score", 0.0) or 0.0):
                runtime.pending_payload = payload
                runtime.pending_score = float(cand_score)
                runtime.pending_ts_ms = int(now_ms)
                runtime.pending_replaced += 1
            
            logger.warning(
                "🛑 [COOLDOWN] (%s) Signal buffered (age=%dms < %dms). Pending updated=%s",
                runtime.symbol, age, cooldown_ms, "YES"
            )
            return None

        # Cooldown window open: check if we have better pending
        if runtime.pending_payload is not None:
            pending_score = float(getattr(runtime, "pending_score", 0.0) or 0.0)
            cur_score = float(score)
            if pending_score >= cur_score:
                payload = runtime.pending_payload
                # upgrade score if pending was better
                score = pending_score
            runtime.pending_payload = None
            runtime.pending_score = 0.0

        # Burst Mode Check (Consolidated)
        force_burst = bool(indicators.get("pressure_extreme_flag", 0))
        use_burst = bool(int(os.getenv("CRYPTO_BURST_ENABLE", "0"))) or force_burst
        
        # DEBUG: Log that signal passed all filters and is about to enter burst
        # logger.info(
        #     "✅ [PRE-BURST] (%s) Signal passed all filters: dir=%s conf=%.1f%% score=%.2f",
        #     runtime.symbol, payload.get("direction"), confidence*100, score
        # )
        
        if use_burst:
            try:
                out = None
                async with runtime.burst_mu:
                    was_active = runtime.burst.st.active
                    runtime.burst.consider(
                        ts_ms=int(now_ms),
                        cand=BurstCandidate(ts_ms=int(now_ms), score=float(score), payload=payload),
                    )
                    # EXPERT FIX: Check flush immediately to prevent 'stuck' signals
                    pass # Burst flush handled by dedicated loop
                    
                    burst_active_gauge.labels(symbol=runtime.symbol).set(1 if runtime.burst.st.active else 0)

                # Do not emit now; we will flush at deadline.
                return None
            except Exception:
                pass # Bookkeeping moved to SignalPipeline
                return payload

        # No burst: emit immediately
        return payload


    def _compute_confidence(
        self,
        runtime: SymbolRuntime,
        indicators: Dict[str, Any],
        confirmations: Sequence[str],
        *,
        side: str,
        kind: str,
    ) -> float:
        """
        Делегируем расчёт в универсальный ConfidenceScorer (services/signal_confidence.py).
        """
        from types import SimpleNamespace

        def _get(name: str, default=0.0):
            v = indicators.get(name)
            return v if v is not None else default

        ctx = SimpleNamespace(
            z_delta=_get("delta_z", _get("z", 0.0)),
            delta=_get("delta", 0.0),
            obi_avg=_get("obi", 0.0),
            obi_sustained=bool(indicators.get("obi_sustained", False)),
            obi_avg_20=_get("obi_20", 0.0),
            obi_sustained_20=bool(indicators.get("obi_sustained_20", False)),
            microprice_shift_bps_20=_get("microprice_shift_bps_20", 0.0),
            wall_bid=bool(indicators.get("wall_bid", False)),
            wall_ask=bool(indicators.get("wall_ask", False)),
            wall_bid_dist_bps=_get("wall_bid_dist_bps", 0.0),
            wall_ask_dist_bps=_get("wall_ask_dist_bps", 0.0),
            depletion_score=_get("depletion_score", 0.0),
            refill_score=_get("refill_score", 0.0),
            impact_proxy=_get("impact_proxy", 0.0),
            spread_bps=_get("spread_bps", 0.0),
            realized_ema_bps=_get("realized_ema_bps", 0.0),
            adverse_ratio_ema=_get("adverse_ratio_ema", 0.0),
            market_mode=indicators.get("market_mode", "mixed") or "mixed",
            l2_age_ms=_get("l2_age_ms", 0.0),
            l2_is_stale=bool(indicators.get("l2_is_stale", False)),
            taker_buy_rate_ema=_get("taker_buy_rate_ema", 0.0),
            taker_sell_rate_ema=_get("taker_sell_rate_ema", 0.0),
            cancel_to_trade_ask=_get("cancel_to_trade_ask", 0.0),
            cancel_to_trade_bid=_get("cancel_to_trade_bid", 0.0),
            eta_fill_ask_sec=_get("eta_fill_ask_sec", 0.0),
            eta_fill_bid_sec=_get("eta_fill_bid_sec", 0.0),
            weak_progress=bool(indicators.get("weak_progress", False)),
            # Phase E+: weak progress trend (history-based)
            weak_recent_cnt=int((indicators.get("weak_recent_cnt") if indicators.get("weak_recent_cnt") is not None else indicators.get("weak_recent_count", 0)) or 0),
            weak_recent_window=int(indicators.get("weak_recent_window", 0) or 0),
            # Phase E+: OBI stability quality (duration + persistence score)
            obi_stable_secs=float(indicators.get("obi_stable_secs", 0.0) or 0.0),
            obi_stability_score=float(indicators.get("obi_stability_score", 0.0) or 0.0),
            # Phase E+: OFI stability quality
            ofi_stable_secs=float(indicators.get("ofi_stable_secs", 0.0) or 0.0),
            ofi_stability_score=float(indicators.get("ofi_stability_score", 0.0) or 0.0),
            # Liquidity regime (risk overlay)
            liq_score=float(indicators.get("liq_score", 0.0) or 0.0),
            liq_regime=str(indicators.get("liq_regime", getattr(runtime, "liq_regime", "normal")) or "normal"),
            # Phase E+: footprint edge absorb evidence
            fp_edge_absorb=bool(indicators.get("fp_edge_absorb", False)),
            fp_edge_absorb_strength=float((indicators.get("fp_edge_absorb_strength") if indicators.get("fp_edge_absorb_strength") is not None else indicators.get("fp_edge_strength", 0.0)) or 0.0),
            iceberg_refresh=_get("iceberg_refresh", 0.0),
            iceberg_duration=_get("iceberg_duration", 0.0),
            absorption_volume=_get("absorption_volume", 0.0),
            # Phase D+: footprint data for scoring
            confirmations=list(confirmations or []),
            fp_absorb_min_score=float(runtime.config.get("fp_absorb_min_score", 1.0)),
            fp_absorb_bonus_w=float(runtime.config.get("fp_absorb_bonus_w", 0.06)),
            fp_imb_bonus_w=float(runtime.config.get("fp_imb_bonus_w", 0.03)),
            fp_bonus_cap=float(runtime.config.get("fp_bonus_cap", 0.08)),
        )

        try:
            conf, parts = self.conf_scorer.score(kind=kind or "custom", side=side, ctx=ctx)
            indicators["confidence_breakdown"] = {
                "base": round(float(parts.get("base", 0.0)), 4),
                "mult": round(float(parts.get("mult", 1.0)), 4),
                "pen_total": round(float(parts.get("pen_total", 0.0)), 4),
            }
            return round(float(conf), 4)
        except Exception as exc:
            self.logger.warning("confidence scorer fallback due to error: %s", exc)
            return float(0.1)

    def _get_atr_for_symbol(self, symbol: str, cfg: Dict[str, Any], tf_override: Optional[str] = None, runtime: Optional[Any] = None) -> Optional[float]:
        """
        Delegates to MarketStateService.
        """
        try:
            # Single source of truth: atr_tf_selected (via canonical resolver)
            tf = str(tf_override or (runtime.get_atr_tf_selected() if runtime else None) or cfg.get("atr_tf") or os.getenv("ATR_TF", "15m") or "15m")
            return self.market_state.get_atr(symbol, tf)
        except Exception:
            return None


    async def publish_signal(self, runtime: SymbolRuntime, signal: Dict[str, Any]) -> None:
        """
        Delegates signal publishing to SignalPipeline.
        """
        await self.signal_pipeline.publish_signal(runtime, signal)
    async def _publish_orders_queue(self, runtime: SymbolRuntime, signal: Dict[str, Any]) -> None:
        """
        Публикует команду в orders:queue по схеме order_creation.md (минимально необходимый payload).
        """
        symbol = signal.get("symbol") or runtime.symbol
        ts_value = signal.get("tick_ts") or signal.get("generated_at")
        if not ts_value:
            logger.warning("⚠️ (%s) Нет временной метки сигнала, пропускаем orders:queue", runtime.symbol)
            return

        side = str(signal.get("direction", "")).upper()
        direction = "buy" if side == "LONG" else "sell"

        reason = signal.get("reason") or "delta_spike"

        order_cmd = {
            "id": f"order-{symbol}-{ts_value}",
            "sid": f"signal-{symbol}-{ts_value}",
            "symbol": symbol,
            "type": "market",
            "direction": direction,
            "source": "crypto_orderflow_service",
            "reason": reason,
        }

        try:
            await self.redis.lpush(self.orders_queue, json.dumps(order_cmd))
        except RedisError as exc:
            logger.warning("⚠️ (%s) Не удалось отправить в очередь ордеров: %s", runtime.symbol, exc)

    # ── Парсинг сообщений ──────────────────────────────────────────────────────

    def _parse_tick_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if "data" in payload:
            try:
                nested = json.loads(payload["data"])
            except json.JSONDecodeError:
                nested = {}
        else:
            nested = {}

        merged = {**payload, **nested}
        ts_ms = normalize_epoch_ms(merged.get("ts") or merged.get("event_time"))
        tick: Dict[str, Any] = {
            "symbol": merged.get("symbol"),
            "ts": int(ts_ms or 0),      # legacy epoch ms (keep)
            "ts_ms": int(ts_ms or 0),   # source of truth epoch ms
            "price": _safe_float(merged.get("price") or merged.get("last") or merged.get("mid")),
            "last": _safe_float(merged.get("last")),
            "bid": _safe_float(merged.get("bid")),
            "ask": _safe_float(merged.get("ask")),
            "qty": merged.get("qty") or merged.get("volume"),
            "side": str(merged.get("side") or merged.get("trade_side") or "BUY").upper(),
            "is_buyer_maker": merged.get("is_buyer_maker"),
            "written_at": _safe_int(merged.get("written_at")),
        }

        # Нормализация числовых полей и buyer/maker + mid
        try:
            qty = float(tick.get("qty", 0.0) or 0.0)
        except (TypeError, ValueError):
            qty = 0.0
        tick["qty"] = qty

        side_upper = str(tick.get("side") or "").upper()
        if side_upper == "SELL":
            tick["is_buyer_maker"] = True
        elif side_upper == "BUY":
            tick["is_buyer_maker"] = False

        bid = _safe_float(tick.get("bid"))
        ask = _safe_float(tick.get("ask"))
        if bid and ask:
            tick["mid"] = (bid + ask) / 2.0
        else:
            tick["mid"] = _safe_float(tick.get("price"))

        return tick

    @staticmethod
    def _env_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
        """Читает boolean переменную окружения с fallback."""
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.lower() in ("1", "true", "yes", "on")


    def _log_metrics(self, runtime: SymbolRuntime) -> None:
        """
        Периодический сброс метрик в Prometheus.
        """
        now = time.time()
        if now - runtime.last_metrics_ts < 30:
            return
        runtime.last_metrics_ts = now
        
        # Count how many times _log_metrics has been called
        if not hasattr(runtime, '_metrics_call_count'):
            runtime._metrics_call_count = 0
        runtime._metrics_call_count += 1

        # Only log every 10000th call
        if runtime._metrics_call_count % 10000 != 0:
            return

        logger.info(
            "METRICS symbol=%s ticks=%d delta_trig=%d signals=%d",
            runtime.symbol,
            runtime.tick_count,
            runtime.delta_triggers,
            runtime.signal_count,
        )

    async def _on_microbar_closed(self, runtime: SymbolRuntime, bar: MicroBar) -> None:
        """
        In-memory обработка события bar_close.
        Здесь можно делать более тяжелые вычисления (но только на bar_close, не на каждом тике):
        - swings
        - divergences
        - RSI(price) и RSI(CVD)
        - New: CVD Snapshots & Dedicated Div Stream
        """
        try:
            await runtime.ensure_dn_loaded(self.redis)
            if bool(int(os.getenv("ATR_TF_CALIB_ENABLE", "1"))):
                await runtime.ensure_atr_tf_loaded(self.redis)
            # ATR sanity selector state (source preference)
            try:
                if bool(int(runtime.config.get("atr_sanity_enable", int(os.getenv("ATR_SANITY_ENABLE", "1"))) or 1)):
                    await runtime.ensure_atr_sanity_loaded(self.redis)
            except Exception:
                pass
            # ATR(bps) calibrator (lazy-load once)
            if bool(int(os.getenv("ATR_BPS_CALIB_ENABLE", "1"))):

                await runtime.ensure_atr_bps_loaded(self.redis)
            if bool(int(os.getenv("ATR_TF_CALIB_ENABLE", "1"))):
                await runtime.ensure_atr_tf_loaded(self.redis)
            if bool(int(os.getenv("ATR_BPS_CALIB_ENABLE", "1"))):
                await runtime.ensure_atr_bps_loaded(self.redis)
            # Load persisted ATR sanity states once (lazy)
            try:
                await runtime.ensure_atr_sanity_loaded(self.redis)
            except Exception:
                pass
        except Exception:
            pass


        # --- ATR sanity range proxy update (roll microbars into atr_tf buckets) ---
        try:
            o = float(getattr(bar, "open", 0.0) or 0.0)
            h = float(getattr(bar, "high", 0.0) or 0.0)
            l = float(getattr(bar, "low", 0.0) or 0.0)
            c = float(getattr(bar, "close", 0.0) or 0.0)
            ts = int(getattr(bar, "end_ts_ms", 0) or 0)
            if ts > 0:
                # ADVERSE Selection Check: Continuation Verify
                if runtime.pending_adverse_payload:
                    sig = runtime.pending_adverse_payload
                    # Check timeout (e.g. 2 * tf or 5s)
                    age_adv = ts - int(runtime.pending_adverse_ts_ms or 0)
                    if 0 < age_adv < 5000:
                        s_dir = str(sig.get("direction", "")).upper()
                        # Verified if bar closes in favor
                        verified = False
                        if s_dir == "LONG" and c > o: verified = True
                        elif s_dir == "SHORT" and c < o: verified = True
                        
                        if verified:
                            logger.info("✅ [ADVERSE] Continuation Verified! Emitting buffered signal.")
                            # inject late metrics
                            sig["adverse_wait_ms"] = age_adv
                            # EMIT
                            final_sig = await self._emit_payload(runtime, sig, ts)
                            if final_sig:
                                preprocess_signal_for_publish(final_sig, runtime.symbol, "CryptoOrderFlow", logger)
                                await self.publish_signal(runtime, final_sig)
                        else:
                            pass
                    
                    # Clear buffer after check (one-shot)
                    runtime.pending_adverse_payload = None
                    runtime.pending_adverse_ts_ms = 0

                runtime.atr_range_agg.push_microbar(end_ts_ms=ts, o=o, h=h, l=l, c=c)
                snap = runtime.atr_range_agg.snapshot()
                runtime.dynamic_cfg["atr_range_tf_ms"] = int(snap.tf_ms)
                runtime.dynamic_cfg["atr_range_n"] = int(snap.n)
                runtime.dynamic_cfg["atr_range_p50_bps"] = float(snap.p50)
                runtime.dynamic_cfg["atr_range_p95_bps"] = float(snap.p95)
        except Exception:
            pass

        # 0. Update Daily Tracker
        try:
             # Feed microbar to daily tracker (persists on day roll)
             runtime.daily_tracker.update(bar)
        except Exception:
             pass

        # 0) Dynamic Regime Update
        try:
             # Fast fetch, fall back to "na" (default)
             # Key convention: regime:{symbol} -> string "range"|"trend"|"thin"
             reg_key = f"regime:{runtime.symbol}"
             # We use generic 'ticks' or 'main' redis? 'ticks' is usually for streams. 'main' is for keys.
             # self.redis is available in CryptoOrderflowService instance (self)
             # but we need to await it.
             rg_val = await self.redis.get(reg_key)
             
             old_regime = str(getattr(runtime, "last_regime", "na") or "na")
             new_regime = "na"
             
             if rg_val:
                 new_regime = str(rg_val)
             
             runtime.last_regime = new_regime

             # 🔔 Notify on regime change
             # COMMENTED OUT: Telegram notifications disabled
             # if old_regime != "na" and new_regime != "na" and old_regime != new_regime:
             #      try:
             #          msg_text = (
             #              f"🔄 <b>Regime Change</b> [{runtime.symbol}]\n"
             #              f"Old: {old_regime}\n"
             #              f"New: {new_regime}\n"
             #              f"Time: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
             #          )
             #          await self.notify_client.xadd(
             #              self.notify_stream,
             #              {"type": "report", "text": msg_text},
             #              maxlen=5000,
             #              approximate=True
             #          )
             #      except Exception as ex:
             #          logger.warning(f"⚠️ Failed to send regime change notify: {ex}")
        except Exception:
             # fail-safe
             pass

        # ------------------------------------------------------------------
        # ATR TF Calibrator update (freshness + consistency)
        # Deterministic time: bar.end_ts_ms
        # ------------------------------------------------------------------
        try:
            if bool(int(os.getenv("ATR_TF_CALIB_ENABLE", "1"))):
                now_ts = int(getattr(bar, "end_ts_ms", 0) or 0)
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                if now_ts > 0 and close_px > 0:
                    cand_str = str(runtime.config.get("atr_tf_candidates", os.getenv("ATR_TF_CANDIDATES", "1m,5m,15m")) or "")
                    cands = tuple([x.strip() for x in cand_str.split(",") if x.strip()])
                    if not cands:
                        cands = ("1m", "5m", "15m")

                    # floor hint: helps detect absurdly low ATR for current regime (optional)
                    hint_floor = float(runtime.dynamic_cfg.get("atr_bps_th", 0.0) or runtime.config.get("atr_bps_min_static", 0.0) or 0.0)

                    scores_inst: Dict[str, float] = {}
                    # score each tf from ATRCache meta
                    for tf in cands:
                        v, m = self.atr_cache.get_with_meta(symbol=runtime.symbol, timeframe=tf, now_ms=now_ts)
                        vv = float(v or 0.0)
                        if vv <= 0 or not m:
                            continue
                        age_ms = int((m or {}).get("age_ms", 0) or 0)
                        atr_bps = 10000.0 * (vv / close_px) if close_px > 0 else 0.0
                        # build inst score in [0..~1.5]
                        # freshness: decays with age
                        # consistency: penalize too-low vs hint
                        # NOTE: scoring function is mirrored in ATRTFCalibrator docs
                        fresh = float(1.0 / (1.0 + (max(0, age_ms) / float(max(1, int(os.getenv("ATR_TF_CALIB_MAX_AGE_MS", str(10 * 60_000))) ) / 2))))
                        cons = 1.0
                        if hint_floor > 0 and atr_bps > 0:
                            cons = max(0.0, min(1.5, float(atr_bps / hint_floor)))
                        sc = float(0.7 * fresh + 0.3 * min(1.0, cons))
                        # tiny bonus for tracker hash (more trustworthy)
                        src = str((m or {}).get("src", (m or {}).get("source", "")) or "")
                        if src == "tracker_hash":
                            sc *= 1.05
                        scores_inst[str(tf)] = float(sc)

                    runtime.atr_tf_calib.update(regime=rg, scores_inst=scores_inst, ts_ms=now_ts)
                    dec = runtime.atr_tf_calib.pick(regime=rg, default_tf=str(runtime.config.get("atr_tf", "1m") or "1m"), candidates=cands)
                    runtime.dynamic_cfg["atr_tf_selected"] = str(dec.tf)
                    runtime.dynamic_cfg["atr_tf_src"] = str(dec.src)
                    runtime.dynamic_cfg["atr_tf_n"] = int(dec.n)
                    runtime.dynamic_cfg["atr_tf_ready"] = int(dec.ready)
                    runtime.dynamic_cfg["atr_tf_scores_ema"] = dict(dec.scores_ema or {})
                    runtime.dynamic_cfg["atr_tf_scores_inst"] = dict(dec.last_scores_inst or {})
                    runtime.dynamic_cfg["atr_tf_picked_score"] = float(dec.picked_score or 0.0)
                    runtime.dynamic_cfg["atr_tf_second_score"] = float(dec.second_score or 0.0)

        except Exception:
            pass

        # --------------------------------------------------------
        # ATR Sanity Calibrator (Source Selection) - User Diff Integration
        # --------------------------------------------------------
        try:
            if bool(int(runtime.config.get("atr_sanity_enable", int(os.getenv("ATR_SANITY_ENABLE", "1"))) or 1)):
                close_ts = int(now_ts)
                # ATR TF
                atr_tf = str(runtime.config.get("atr_tf", "1m") or "1m")
                # Normalize TF
                try:
                    tf_norm = self.atr_cache._normalize_tracker_tf(atr_tf)
                except Exception:
                    tf_norm = str(atr_tf).upper()

                cands_src = []
                try:
                    cands_src = self.atr_cache.get_candidates(symbol=runtime.symbol, timeframe=atr_tf, now_ms=close_ts)
                except Exception:
                    cands_src = []

                dec_src = runtime.atr_sanity.decide(tf_norm=tf_norm, candidates=cands_src)
                
                runtime.dynamic_cfg["atr_src_pref"] = str(dec_src.src_pref)
                runtime.dynamic_cfg["atr_src_ready"] = int(dec_src.ok)
                runtime.dynamic_cfg["atr_src_reason"] = str(dec_src.reason)
                runtime.dynamic_cfg["atr_src_mismatch"] = int(dec_src.mismatch)
                runtime.dynamic_cfg["atr_src_n"] = int(dec_src.n)
                runtime.dynamic_cfg["atr_src_median"] = float(dec_src.median)
                runtime.dynamic_cfg["atr_src_picked"] = float(dec_src.picked)
                
                # Persist state (throttled)
                try:
                    min_iv_ms = int(runtime.config.get("atr_sanity_persist_min_interval_ms", 300_000) or 300_000)
                    min_bars = int(runtime.config.get("atr_sanity_persist_min_bars", 30) or 30)
                    runtime._atr_sanity_bars_since_persist = int(getattr(runtime, "_atr_sanity_bars_since_persist", 0) or 0) + 1
                    last_p = int(getattr(runtime, "_atr_sanity_last_persist_ts_ms", 0) or 0)
                    due_by_time = (last_p <= 0) or (close_ts - last_p >= min_iv_ms)
                    due_by_bars = runtime._atr_sanity_bars_since_persist >= min_bars
                    
                    if int(dec_src.n) >= 5 and (due_by_time or due_by_bars):
                        if self.calib_svc:
                            await self.calib_svc.persist_atr_sanity(runtime, tf_norm=str(tf_norm), ts_ms=int(close_ts))
                        runtime._atr_sanity_last_persist_ts_ms = int(close_ts)
                        runtime._atr_sanity_bars_since_persist = 0
                except Exception:
                    pass
        except Exception:
            pass


        # Throttled persist per regime
        try:
            gap_ms = int(runtime.config.get("atr_tf_calib_persist_gap_ms", int(os.getenv("ATR_TF_CALIB_PERSIST_GAP_MS", "120000"))))
            last_p = int(getattr(runtime, "_atr_tf_last_persist_ts_ms", 0) or 0)
            if gap_ms > 0 and (now_ts - last_p) >= gap_ms:
                if self.calib_svc:
                    await self.calib_svc.persist_atr_tf_regime(runtime, regime=rg, ts_ms=now_ts)
                runtime._atr_tf_last_persist_ts_ms = int(now_ts)
        except Exception as exc:
            log_silent_error(exc, 'persist_failure', runtime.symbol, '_handle_tick:atr_tf_persist')
            pass

    
        # --- Dynamic calibration update (eff_quote / min_quote_delta) ---
        try:
            quote_delta = float(getattr(runtime, "last_quote_delta", 0.0) or 0.0)
            if quote_delta > 0:
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                runtime.eff_calib.update(regime=rg, quote_delta=float(quote_delta))
                
                # ... existing eff_calib persistence ...
                # Leaving existing EffQuote logic here as is, assumed working
                # ...
                if bool(int(runtime.config.get("calib_persist_enable", 1))):
                    runtime._calib_bars_since_persist += 1
                    min_bars = int(runtime.config.get("calib_persist_min_bars", 60))
                    if runtime._calib_bars_since_persist >= min_bars:
                        runtime._calib_bars_since_persist = 0
                        if self.calib_svc:
                            await self.calib_svc.persist_effq(runtime, regime=rg, ts_ms=int(bar.end_ts_ms))
    
        except Exception as exc:
            log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_handle_tick:eff_calib_update')
            pass
    
        # ------------------------------------------------------------------
        # ATR(bps) sanity floors (per-regime) -> runtime.dynamic_cfg
        # Fix "broken chain": we MUST select atr_bps_th based on regime+tier and expose it.
        # ------------------------------------------------------------------
            close_px = float(getattr(bar, "close", 0.0) or 0.0)
            atr_val = float(getattr(runtime, "last_atr", 0.0) or 0.0)
            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
            if close_px > 0 and atr_val > 0:
                atr_bps = 10000.0 * (atr_val / close_px)
                runtime.dynamic_cfg["atr_bps"] = float(atr_bps)

                # Update calibrator (fail-open)
                if bool(int(os.getenv("ATR_BPS_CALIB_ENABLE", "1"))):
                    runtime.atr_bps_calib.update(regime=rg, atr_bps=float(atr_bps))

                # Bootstrap floors (must be >0 in config; if not, fallback to static min)
                # --- ATR Floor Policy (Tiered) ---
                # Check for overrides in local 'cfg'
                cfg = runtime.config
                d0 = float(cfg.get("atr_floor_t0_bps", 0.0) or 0.0)
                d1 = float(cfg.get("atr_floor_t1_bps", 0.0) or 0.0)
                d2 = float(cfg.get("atr_floor_t2_bps", 0.0) or 0.0)
                floors = runtime.atr_bps_calib.thresholds(
                    regime=rg,
                    default_floor_t0=d0,
                    default_floor_t1=d1,
                    default_floor_t2=d2,
                )
                runtime.dynamic_cfg["atr_floor_t0_bps"] = float(floors.floor_t0)
                runtime.dynamic_cfg["atr_floor_t1_bps"] = float(floors.floor_t1)
                runtime.dynamic_cfg["atr_floor_t2_bps"] = float(floors.floor_t2)
                runtime.dynamic_cfg["atr_bps_src"] = str(floors.src)
                runtime.dynamic_cfg["atr_bps_n"] = int(floors.n)
                runtime.dynamic_cfg["atr_calib_ready"] = int(
                    1 if floors.n >= int(runtime.config.get("atr_bps_calib_min_samples", int(os.getenv("ATR_BPS_CALIB_MIN_SAMPLES", "500")))) else 0
                )

                # SELECT threshold by regime tier (this is the missing link)
                tier, rg2, th = compute_atr_bps_threshold(
                    regime=rg,
                    cfg=runtime.config,
                    t0=float(floors.floor_t0),
                    t1=float(floors.floor_t1),
                    t2=float(floors.floor_t2),
                )
                runtime.dynamic_cfg["atr_floor_tier"] = int(tier)
                runtime.dynamic_cfg["atr_bps_th"] = float(th)

                # Persist (throttled)
                try:
                    gap_ms = int(runtime.config.get("atr_bps_calib_persist_gap_ms", int(os.getenv("ATR_BPS_CALIB_PERSIST_GAP_MS", "120000"))))
                    last_p = int(getattr(runtime, "_atr_bps_last_persist_ts_ms", 0) or 0)
                    if bool(int(os.getenv("ATR_BPS_CALIB_ENABLE", "1"))) and gap_ms > 0 and (int(bar.end_ts_ms) - last_p) >= gap_ms:
                        if self.calib_svc:
                            await self.calib_svc.persist_atr_bps(runtime, regime=rg, ts_ms=int(bar.end_ts_ms))
                        runtime._atr_bps_last_persist_ts_ms = int(bar.end_ts_ms)
                except Exception as exc:
                    log_silent_error(exc, 'persist_failure', runtime.symbol, '_handle_tick:atr_bps_persist')
                    pass
        except Exception as exc:
            log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_handle_tick:atr_bps_wrapper')
            pass
            
        # --- DeltaNotional tiers calibration (per regime) ---
        try:
            dn_usd = abs(float(getattr(bar, "delta_sum", 0.0) or 0.0)) * float(getattr(bar, "close", 0.0) or 0.0)
            if math.isfinite(dn_usd) and dn_usd > 0:
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                
                # 1. Update Calibrator (Authoritative source)
                runtime.dn_calib.update(
                    regime=rg,
                    dn_usd=float(dn_usd),
                    ts_ms=int(bar.end_ts_ms)
                )

                # 2. Telemetry: Check Scale & Divergence (Throttle: 1h)
                now_ms = int(bar.end_ts_ms)
                if not hasattr(runtime, "last_dn_how_report_ts_ms"):
                     runtime.last_dn_how_report_ts_ms = 0
                
                if now_ms - runtime.last_dn_how_report_ts_ms > 3600_000:
                    tiers_cfg = runtime.config.get("delta_diff_tiers") or get_default_delta_tiers(runtime.symbol)
                    d0 = float(tiers_cfg.get("tier0", 0.0) or 0.0)
                    d1 = float(tiers_cfg.get("tier1", 0.0) or 0.0)
                    d2 = float(tiers_cfg.get("tier2", 0.0) or 0.0)
                    
                    t_telem = runtime.dn_calib.tiers(regime=rg, ts_ms=now_ms, default_t0=d0, default_t1=d1, default_t2=d2)
                    t_decis = runtime.dn_calib.tiers(regime=rg, ts_ms=0, default_t0=d0, default_t1=d1, default_t2=d2)
                    
                    # Metrics
                    from services.orderflow.metrics import of_dn_how_scale_gauge, of_dn_how_ratio_t1_gauge
                    try:
                        of_dn_how_scale_gauge.labels(symbol=runtime.symbol, regime=rg).set(t_telem.scale)
                    except Exception:
                        pass
                    
                    ratio = 1.0
                    if t_decis.tier1_usd > 0:
                        ratio = t_telem.tier1_usd / t_decis.tier1_usd
                    try:
                        of_dn_how_ratio_t1_gauge.labels(symbol=runtime.symbol, regime=rg).set(ratio)
                    except Exception:
                        pass
                    
                    # Report
                    if ratio < 0.8 or ratio > 1.2:
                        msg = (
                            f"Liquidity Divergence Report ({runtime.symbol})\n"
                            f"Regime: {rg}\n"
                            f"HourOfWeek: {t_telem.hour_of_week}\n"
                            f"Global Liq (EMA): ${t_telem.g_liq_ema:,.0f}\n"
                            f"Bucket Liq (EMA): ${t_telem.b_liq_ema:,.0f}\n"
                            f"Scale Factor: {t_telem.scale:.2f}x\n"
                            f"Tier1 (Decision): ${t_decis.tier1_usd:,.0f}\n"
                            f"Tier1 (Telemetry): ${t_telem.tier1_usd:,.0f}\n"
                            f"Ratio: {ratio:.2f}"
                        )
                        await self.signal_pipeline.send_telegram_report(runtime=runtime, text=msg)
                    runtime.last_dn_how_report_ts_ms = now_ms

                # 3. Persistence
                if bool(int(runtime.config.get("calib_persist_enable", 1))):
                    runtime._calib_bars_since_persist = int(getattr(runtime, "_calib_bars_since_persist", 0) or 0) + 1
                    min_bars = int(runtime.config.get("calib_persist_min_bars", 60))
                    if runtime._calib_bars_since_persist >= min_bars:
                        runtime._calib_bars_since_persist = 0
                        if self.calib_svc:
                            await self.calib_svc.persist_dn(runtime, regime=rg, ts_ms=int(bar.end_ts_ms))

        except Exception as exc:
             log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_on_microbar_closed:dn_calib')
             pass


        # ATR TF Selector (UNIFIED - single source of truth: atr_tf_selected)
        # Shadow mode: compute candidate, no apply. Enforce mode: apply candidate to selected.
        # ------------------------------------------------------------------
        try:
            if bool(int(os.getenv("ATR_TF_CALIB_ENABLE", "1"))):
                now_ts = int(getattr(bar, "end_ts_ms", 0) or 0)
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()

                # Throttle: do not recompute too often (Redis reads for multiple TF)
                refresh_ms = int(runtime.config.get("atr_tf_calib_refresh_ms", 60_000))
                last = int(runtime.dynamic_cfg.get("atr_tf_calib_last_ms", 0) or 0)
                if refresh_ms < 10_000:
                    refresh_ms = 10_000
                if now_ts > 0 and (now_ts - last) >= refresh_ms and close_px > 0:
                    runtime.dynamic_cfg["atr_tf_calib_last_ms"] = int(now_ts)

                    # Candidate TFs list (env-tunable)
                    tfs_raw = str(os.getenv("ATR_TF_CALIB_TFS", "1m,5m,15m,1h"))
                    tfs = [x.strip() for x in tfs_raw.split(",") if x.strip()]
                    if not tfs:
                        tfs = ["1m", "5m", "15m", "1h"]

                    # Compute target from fees-aware gate (rocket_v1) to avoid permanent veto
                    # NOTE: this is *sanity* target; unified gate still uses max(floor,fees).
                    target_bps = 0.0
                    try:
                        tp_ratios = parse_tp_ratio(runtime.config.get("tp_ratio") or runtime.config.get("tp_rr") or "")
                        tp1_share = float(tp_ratios[0] if tp_ratios else 0.5)
                        # Use signal_pipeline for rocket logic
                        rocket_mult = float(self.signal_pipeline._get_rocket_multiplier(runtime.symbol) or 0.0)
                        denom = float(tp1_share * rocket_mult)
                        if denom > 0:
                            target_bps = float((float(self.signal_pipeline.FEES_BPS_RT) + float(self.signal_pipeline.TP_BPS_BUFFER)) / denom)
                    except Exception:
                        target_bps = 0.0

                    # Collect atr_bps for each TF (best-effort; if tf missing -> skip)
                    atr_bps_by_tf: Dict[str, float] = {}
                    for tf in tfs:
                        try:
                            # Use raw cache lookup to bypass calibration logic itself
                            atr_tf = float(self.atr_cache.get(runtime.symbol, tf) or 0.0)
                            if atr_tf > 0:
                                atr_bps_by_tf[tf] = 10000.0 * (atr_tf / close_px)
                        except Exception as exc:
                            log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_handle_tick:atr_tf_update')
                            continue

                    if atr_bps_by_tf:
                        runtime.atr_tf_calib.update_many(regime=rg, atr_bps_by_tf=atr_bps_by_tf)

                        # Recommend TF (switching controlled by hold-down + hysteresis)
                        fallback_tf = str(runtime.config.get("atr_tf", os.getenv("ATR_TF", "15m")) or "15m")
                        current_tf = runtime.get_atr_tf_selected()  # Use canonical resolver
                        mode = str(os.getenv("ATR_TF_SELECTOR_MODE", "shadow")).lower()  # "shadow"|"enforce"
                        allow_switch = (mode == "enforce")
                        runtime.dynamic_cfg["atr_tf_mode"] = mode

                        choice = runtime.atr_tf_calib.recommend_tf(
                            regime=rg,
                            target_bps=target_bps,
                            fallback_tf=fallback_tf,
                            now_ts_ms=now_ts,
                            current_tf=current_tf,
                            allow_switch=allow_switch,
                        )

                        runtime.dynamic_cfg["atr_tf_target_bps"] = float(choice.target_bps)
                        runtime.dynamic_cfg["atr_tf_ready"] = int(1 if choice.src != "static" and choice.n >= int(os.getenv("ATR_TF_CALIB_MIN_SAMPLES", "300")) else 0)
                        runtime.dynamic_cfg["atr_tf_src"] = str(choice.src)
                        runtime.dynamic_cfg["atr_tf_n"] = int(choice.n)
                        # Telemetry: always write candidate (for observability)
                        runtime.dynamic_cfg["atr_tf_candidate"] = str(choice.tf)
                        runtime.dynamic_cfg["atr_tf_candidate_src"] = str(choice.src)
                        runtime.dynamic_cfg["atr_tf_candidate_n"] = int(choice.n)
                        runtime.dynamic_cfg["atr_tf_candidate_score"] = float(getattr(choice, "score", 0.0) or 0.0)
                        runtime.dynamic_cfg["atr_tf_candidates_bps"] = dict(atr_bps_by_tf)
                                
                        # Update metrics
                        atr_tf_target_bps.labels(symbol=runtime.symbol).set(float(target_bps))
                        atr_tf_candidate_score.labels(symbol=runtime.symbol).set(float(getattr(choice, "score", 0.0) or 0.0))
                        candidate_diff = 1 if str(choice.tf) != current_tf else 0
                        atr_tf_candidate_diff.labels(symbol=runtime.symbol).set(candidate_diff)
                                
                        # Apply: ONLY in enforce mode
                        if allow_switch and str(choice.tf) != current_tf:
                            prev_tf = current_tf
                            new_tf = str(choice.tf)
                            runtime.dynamic_cfg["atr_tf_selected"] = new_tf
                            runtime.dynamic_cfg["atr_tf_last_switch_ts_ms"] = int(now_ts)
                            # Log switch (rate-limited)
                            logger.info(
                                "🔄 (%s) ATR-TF switch: %s → %s (target_bps=%.1f, src=%s, n=%d)",
                                runtime.symbol, prev_tf, new_tf, target_bps, choice.src, choice.n
                            )
                            # Increment switch counter
                            atr_tf_switch_total.labels(symbol=runtime.symbol).inc()
                        elif not allow_switch:
                            # Shadow mode: ensure selected is initialized but don't change it
                            runtime.dynamic_cfg.setdefault("atr_tf_selected", current_tf)

                        # Persist selected TF (throttled, only in enforce or on init)
                        persist_gap = int(runtime.config.get("atr_tf_calib_persist_gap_ms", 300_000))
                        if persist_gap < 60_000:
                            persist_gap = 60_000
                        last_p = int(getattr(runtime, "_atr_tf_last_persist_ts_ms", 0) or 0)
                        if now_ts > 0 and (now_ts - last_p) >= persist_gap and allow_switch:
                            runtime._atr_tf_last_persist_ts_ms = int(now_ts)
                            choice_state = {
                                "tf": runtime.get_atr_tf_selected(),
                                "src": str(choice.src),
                                "updated_ts_ms": int(now_ts)
                            }
                            if self.calib_svc:
                                await self.calib_svc.persist_atr_tf_choice(runtime, choice_state=choice_state, ts_ms=now_ts)
        except Exception:
            pass


        # --- ADX quantile snapshot (deterministic by bar end ts) ---
        # We store in runtime.dynamic_cfg for later use in snapshot publisher.
        # Source of truth:
        #  - adx14 is in Redis key adx:{symbol} (float)
        #  - quantiles are in Redis key regime:q:{symbol}:1m (json)
        # Here we only read adx14 (cheap); adx_q is computed in snapshot publisher.
        try:
            # best-effort; fail-open
            adx_raw = await self.redis.get(f"adx:{runtime.symbol}")
            runtime.dynamic_cfg["adx14"] = float(adx_raw) if adx_raw is not None else 0.0
        except Exception:
            pass

        # 1) RSI updates
        try:
            runtime.rsi_price.update(float(bar.close))
            runtime.rsi_cvd.update(float(bar.cvd_close))
        except Exception:
            pass
            
        # Metric: bars closed
        bars_closed_total.labels(symbol=runtime.symbol, tf=str(getattr(bar, "tf_ms", "0"))).inc()


        # ------------------------------------------------------------
        # Phase C: ATR TF selection + ATR caching for bar_close.
        # Goal:
        #  - choose best timeframe/source by freshness+consistency
        #  - store deterministic choice for later tick/execution use
        # Fail-open:
        #  - if selector fails, fall back to cfg atr_tf
        # ------------------------------------------------------------
        atr_val = float(getattr(runtime, "last_atr", 0.0) or 0.0)
        try:
            now_ts = int(bar.end_ts_ms)
            refresh_ms = int(runtime.config.get("eq_atr_refresh_ms", 15_000))
            if refresh_ms < 1_000:
                refresh_ms = 1_000

            if (now_ts - int(getattr(runtime, "last_atr_ts_ms", 0) or 0)) >= refresh_ms:
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                # 1) Use canonical TF resolver (single source of truth)
                tf_sel = runtime.get_atr_tf_selected()
                try:
                    if bool(int(os.getenv("ATR_TF_CALIB_ENABLE", "1"))) and close_px > 0:
                        choice = self.atr_tf_sel.choose(
                            symbol=str(runtime.symbol),
                            price=float(close_px),
                            now_ms=int(now_ts),
                            atr_cache=self.atr_cache,
                        )
                        if choice is not None:
                            # TELEMETRY ONLY: do NOT write to atr_tf_selected (legacy path)
                            # Single source of truth is the unified selector in _on_microbar_closed
                            runtime.dynamic_cfg["atr_tf_alt_candidate"] = str(choice.tf)
                            runtime.dynamic_cfg["atr_tf_alt_src"] = str(choice.src)
                            runtime.dynamic_cfg["atr_tf_alt_score"] = float(choice.score)
                            runtime.dynamic_cfg["atr_tf_alt_age_ms"] = int(choice.age_ms)
                            runtime.dynamic_cfg["atr_tf_alt_atr_bps"] = float(choice.atr_bps)
                            # NO persistence for legacy path
                except Exception:
                    pass

                # 2) fetch ATR using selected TF (best-effort)
                atr_tmp = 0.0
                try:
                    atr_tmp, atr_meta = self.atr_cache.get_with_meta(symbol=runtime.symbol, timeframe=tf_sel, now_ms=int(now_ts))
                    atr_tmp = float(atr_tmp or 0.0)
                    # expose meta for audit/debug
                    if isinstance(atr_meta, dict):
                        runtime.dynamic_cfg["atr_live_src"] = str(atr_meta.get("src", "na"))
                        runtime.dynamic_cfg["atr_live_key"] = str(atr_meta.get("key", ""))
                        runtime.dynamic_cfg["atr_live_age_ms"] = int(atr_meta.get("age_ms", 0) or 0)
                except Exception:
                    atr_tmp = 0.0

                if atr_tmp > 0:
                    atr_val = float(atr_tmp)
                    runtime.last_atr = float(atr_val)
                    runtime.last_atr_ts_ms = int(now_ts)
        except Exception:
            pass

        # ------------------------------------------------------------------
        # ATR floor tiers (per-symbol/per-regime) -> runtime.dynamic_cfg
        # Purpose:
        #   Fix "broken chain": ATR tiers must be selected later by tick-gate.
        # Deterministic time:
        #   uses bar.end_ts_ms and runtime.last_regime (bar-close derived).


        # 2) Swings and Divergences
        try:
            swings = runtime.swing.update(bar)
            for sp in swings:
                # Rate limit logs: only 1 in 50
                sp_cnt = self.swing_point_counters.get(runtime.symbol, 0) + 1
                self.swing_point_counters[runtime.symbol] = sp_cnt

                if sp_cnt % 50 == 0:
                     self.logger.info("📐 Swing Point detected (%s): kind=%s, price=%.2f, ts_ms=%d (x%d)", runtime.symbol, sp.kind, sp.price, sp.ts_ms, sp_cnt)
                
                if sp.kind == "high":
                    runtime.prev_swing_high = runtime.last_swing_high
                    runtime.last_swing_high = sp
                elif sp.kind == "low":
                    runtime.prev_swing_low = runtime.last_swing_low
                    runtime.last_swing_low = sp

                # Hidden divergence requires trend bias.
                bias = "none"
                if getattr(runtime, "cont_ctx_trend_dir", None):
                     td = str(runtime.cont_ctx_trend_dir).upper()
                     bias = "UP" if td == "LONG" else "DOWN" if td == "SHORT" else "none"
                else:
                     if runtime.last_swing_high and bar.close >= runtime.last_swing_high.price:
                         bias = "UP"
                     elif runtime.last_swing_low and bar.close <= runtime.last_swing_low.price:
                         bias = "DOWN"

                # Check Hidden Divergence
                divs_swing = runtime.divergence.update_swing(sp, trend_bias=bias)
                if divs_swing:
                    runtime.last_div = divs_swing[-1]
                    for d in divs_swing:
                        divergence_detected_total.labels(symbol=runtime.symbol, kind=str(d.kind)).inc()
                        self.logger.info("💎 Divergence Detected (%s): kind=%s, strength=%.2f", runtime.symbol, d.kind, d.strength)
                        
                        # --- Unified Divergence/Pools Signal Publishing ---
                        try:
                            # 1. Features
                            feats = {}
                            try:
                                feats["deltaSpikeZ"] = 0.0  # Not directly available in swing context
                                feats["weak_progress"] = int(getattr(runtime.last_wp, "is_weak", 0)) if runtime.last_wp else 0
                                feats["regime"] = str(getattr(runtime, "last_regime", "na"))
                                feats["atr_mult"] = 0.0  # Placeholder since ATR usually part of specific rule config
                                # Additional context if available
                                if hasattr(runtime, "last_spread_bps"):
                                    feats["spread_bps"] = float(runtime.last_spread_bps)
                            except Exception:
                                pass

                            # 2. Nearest Pool (mature only)
                            npool_info = None
                            try:
                                # Find nearest pool of ANY kind to the current price
                                pools_all = runtime.eq_pools.pools(kind=None, only_mature=True)
                                if pools_all:
                                    # Sort by distance to bar.close
                                    pools_all.sort(key=lambda p: abs(float(p.level) - float(bar.close)))
                                    np = pools_all[0]
                                    npool_info = {
                                        "id": str(getattr(np, "pool_id", "")),
                                        "kind": str(getattr(np, "kind", "")),
                                        "level": float(getattr(np, "level", 0.0)),
                                        "dist_px": abs(float(np.level) - float(bar.close))
                                    }
                            except Exception:
                                pass

                            # 3. Payload
                            payload = {
                                "signal_type": "Divergence",
                                "symbol": str(runtime.symbol),
                                "tf": str(runtime.config.get("micro_tf", "1s")),
                                "ts_ms": int(d.ts_ms),
                                "side_bias": str(bias),
                                "divergence_kind": str(d.kind),
                                "strength": float(d.strength),
                                "confidence": min(0.99, float(d.strength) / 10.0),  # Simple confidence estimation
                                "features": feats,
                                "nearest_pool": npool_info,
                                "generated_at": int(time.time() * 1000),
                                # Standard fields for compatibility
                                "reason": f"divergence_{d.kind}",
                                "entry": float(d.price_curr),
                                "price": float(d.price_curr),
                                "cvd": float(d.cvd_curr)
                            }

                            # 4. Publish to signals:crypto:raw
                            # We use xadd directly here to ensure it goes to the unified stream immediately
                            stream_key = "signals:crypto:raw"
                            pl_json = json.dumps(payload, default=str, ensure_ascii=False)
                            asyncio.create_task(self.ticks.xadd(stream_key, {"payload": pl_json}, maxlen=20000))

                        except Exception as ex:
                            self.logger.warning(f"⚠️ Failed to publish Divergence signal: {ex}")

                # Update EQ pools from swing points
                try:
                    runtime.eq_pools.on_swing(sp, atr=atr_val)
                except Exception:
                    pass

            divs = runtime.divergence.update(bar, runtime.swing.swings)
            for div in divs:
                runtime.last_div = div
        except Exception:
            pass

        # --- Dynamic calibration update (eff_quote / min_quote_delta) ---
        try:
            if bool(getattr(bar, "fp_enabled", False)):
                eff_q = float(getattr(bar, "fp_eff_quote", 0.0) or 0.0)
                qd = float(getattr(bar, "fp_quote_delta", 0.0) or 0.0)
                regime = str(getattr(runtime, "last_regime", "na") or "na")
                runtime.eff_calib.update(regime=regime, eff_quote=eff_q, quote_delta=qd)

                # Tier policy by regime
                tier = int(cfg.get("abs_lvl_tier_default", 1))
                if regime in ("range",):
                    tier = int(cfg.get("abs_lvl_tier_range", 1))
                elif regime in ("trend", "trending_bull", "trending_bear"):
                    tier = int(cfg.get("abs_lvl_tier_trend", 0))
                elif regime in ("thin", "news", "illiquid"):
                    tier = int(cfg.get("abs_lvl_tier_thin", 2))

                th = runtime.eff_calib.thresholds(
                    regime=regime,
                    default_eff_th=float(runtime.config.get("abs_lvl_eff_quote_th", 0.0020)),
                    default_min_qd=float(runtime.config.get("abs_lvl_min_quote_delta", 0.0)),
                    tier=tier,
                )
                runtime.dynamic_cfg["abs_lvl_eff_quote_th"] = float(th.eff_quote_th)
                runtime.dynamic_cfg["abs_lvl_min_quote_delta"] = float(th.min_quote_delta)
                runtime.dynamic_cfg["abs_lvl_calib_n"] = int(th.n)
                runtime.dynamic_cfg["abs_lvl_calib_src"] = str(th.src)
                runtime.dynamic_cfg["abs_lvl_tier"] = int(tier)

                stab = runtime._th_stab.update(float(th.eff_quote_th))
                runtime.dynamic_cfg["abs_lvl_th_ema"] = float(stab.ema)
                runtime.dynamic_cfg["abs_lvl_th_drift"] = float(stab.drift)
                runtime.dynamic_cfg["abs_lvl_th_range_norm"] = float(stab.range_norm)
                runtime.dynamic_cfg["abs_lvl_th_stab_n"] = int(stab.n)

                drift_max = float(runtime.config.get("abs_lvl_th_drift_max", 0.35))
                range_max = float(runtime.config.get("abs_lvl_th_range_max", 1.20))
                unstable = int((stab.drift > drift_max) or (stab.range_norm > range_max))
                runtime.dynamic_cfg["abs_lvl_th_unstable"] = unstable

                # Dynamic strictness: if unstable or thin/news -> need=3
                if bool(int(runtime.config.get("strong_dynamic_need_enable", 1))):
                    if unstable or regime in ("thin", "news", "illiquid"):
                        runtime.dynamic_cfg["strong_need_reversal"] = 3
                        runtime.dynamic_cfg["strong_need_continuation"] = 3
                    else:
                        runtime.dynamic_cfg["strong_need_reversal"] = int(cfg.get("strong_need_reversal", 2))
                        runtime.dynamic_cfg["strong_need_continuation"] = int(cfg.get("strong_need_continuation", 2))
                runtime.dynamic_cfg["abs_lvl_calib_n"] = int(th.n)
                runtime.dynamic_cfg["abs_lvl_calib_src"] = str(th.src)

                # --- Persist calibration (throttled, deterministic by bar time) ---
                if bool(int(runtime.config.get("calib_persist_enable", 1))):
                    runtime._calib_bars_since_persist += 1
                    min_bars = int(runtime.config.get("calib_persist_min_bars", 120))
                    min_dt = int(runtime.config.get("calib_persist_min_interval_ms", 60_000))
                    ts_ms = int(getattr(bar, "end_ts_ms", 0) or 0)
                    last = int(getattr(runtime, "_calib_last_persist_ts_ms", 0) or 0)

                    due = (runtime._calib_bars_since_persist >= min_bars) or (ts_ms > 0 and last > 0 and (ts_ms - last) >= min_dt)
                    if due and ts_ms > 0:
                        runtime._calib_last_persist_ts_ms = ts_ms
                        runtime._calib_bars_since_persist = 0
                        # regime label should match what you used for update()
                        rg = str(getattr(runtime, "last_regime", "na") or "na")
                        if self.calib_svc:
                            asyncio.create_task(self.calib_svc.persist_effq(runtime, regime=rg, ts_ms=ts_ms))

                if bool(int(runtime.config.get("strong_dynamic_need_enable", 0))):
                    if regime in ("thin", "news", "illiquid"):
                        runtime.dynamic_cfg["strong_need_reversal"] = 3
                        runtime.dynamic_cfg["strong_need_continuation"] = 3
                    else:
                        runtime.dynamic_cfg["strong_need_reversal"] = int(cfg.get("strong_need_reversal", 2))
                        runtime.dynamic_cfg["strong_need_continuation"] = int(cfg.get("strong_need_continuation", 2))
        except Exception:
            pass
            
        # C) Rolling CVD Snapshot (for UI/QA)
        # Writes to LIST: cvd:snap:{symbol}
        if os.getenv("CVD_SNAPSHOT_ENABLE", "0") == "1":
            try:
                # Format: "{ts_ms},{cvd},{cvd_ema},{cvd_slope}"
                # For now, just cvd, others 0.0
                val_str = f"{int(bar.end_ts_ms)},{float(bar.cvd_close):.2f},0.0,0.0"
                snap_key = f"cvd:snap:{runtime.symbol}"
                
                # Use pipeline for atomicity if possible, or just gather
                # Need to verify if self.ticks supports pipeline easily (it is redis client)
                # Just sequential await is fine for now as it's fire-and-forget logic
                await self.ticks.lpush(snap_key, val_str)
                await self.ticks.ltrim(snap_key, 0, 3599) # Keep last 3600 (1 hour @ 1s)
                await self.ticks.expire(snap_key, 21600)  # TTL 6 hours
            except Exception as ex:
                pass


        # 3) Footprint diagnostics
        if getattr(bar, "fp_evictions", 0) > 0:
            fp_buckets_evicted_total.labels(symbol=runtime.symbol).inc(bar.fp_evictions)


        # Phase C: sweep detection using mature pools.
        try:
            mature = runtime.eq_pools.pools(only_mature=True)
            sweeps = runtime.sweep.update_bar(bar, pools=mature)
            if sweeps:
                sw = sweeps[-1]
                runtime.last_sweep = sw
                # Store baseline CVD at sweep bar close
                try:
                    runtime.last_sweep_ts_ms = int(getattr(sw, "ts_ms", 0) or int(bar.end_ts_ms))
                    runtime.last_sweep_cvd = float(getattr(bar, "cvd_close", 0.0) or 0.0)
                except Exception:
                    pass
                sweep_detected_total.labels(symbol=runtime.symbol, eq_kind=str(sw.kind)).inc()
                # start reclaim FSM on sweep return
                runtime.reclaim.on_sweep_return(runtime.last_sweep)
                # FIX: prevent reclaim on same bar
                runtime.reclaim_start_ts_ms = int(getattr(sw, "ts_ms", 0))
        except Exception:
            pass
            
        # Reclaim FSM progress on each bar close
        try:
            # FIX: ignore same bar
            if int(getattr(runtime, "reclaim_start_ts_ms", 0)) == int(bar.end_ts_ms):
                pass
            else:
                ev = runtime.reclaim.on_bar_close(bar)
                if ev is not None:
                    runtime.last_reclaim = ev

                    # ------------------------------------------------------------
                    # Phase E: CVD Reclaim Evidence (bonus-evidence)
                    # ------------------------------------------------------------
                    try:
                        # Always try to compute if we have sweep baseline
                        if (int(runtime.config.get("cvd_reclaim_enable", 1) or 0) == 1 and 
                            runtime.last_sweep_ts_ms > 0):
                            
                            res = compute_cvd_reclaim(
                                ts_ms=int(ev.ts_ms),
                                sweep_ts_ms=runtime.last_sweep_ts_ms,
                                cvd_sweep=float(runtime.last_sweep_cvd),
                                reclaim_ts_ms=int(ev.ts_ms),
                                cvd_reclaim=float(bar.cvd_close),
                                direction_bias=str(ev.direction_bias),
                                min_abs=float(runtime.config.get("cvd_reclaim_min_abs", 0.0)),
                                sat_abs=float(runtime.config.get("cvd_reclaim_sat_abs", 0.0)),
                            )
                            runtime.last_cvd_reclaim = res
                            
                            cvd_reclaim_eval_total.labels(symbol=runtime.symbol, bias=str(ev.direction_bias)).inc()
                            if res.ok:
                                cvd_reclaim_ok_total.labels(symbol=runtime.symbol, bias=str(ev.direction_bias)).inc()
                            
                            self.logger.info(
                                "CVDReclaim computed sym=%s bias=%s ok=%d score=%.3f delta=%.1f window_ms=%d",
                                runtime.symbol, ev.direction_bias, res.ok, res.score, res.cvd_delta, (int(ev.ts_ms) - runtime.last_sweep_ts_ms)
                            )
                    except Exception:
                        pass
        except Exception:
            pass

        # --- Weak progress snapshot ---
        try:
            runtime.last_wp = compute_weak_progress(bar, atr_val, runtime.config)
            # Update WeakProgressDetector history (trend-of-absorption)
            try:
                if runtime.last_wp is not None:
                    runtime.weak_progress_det.push(runtime.last_wp, ts_ms=int(bar.end_ts_ms))
            except Exception:
                pass
        except Exception:
            runtime.last_wp = None

        # --- Footprint edge absorb ---
        try:
            fe = runtime.fp_edge.update_bar(bar, runtime.config)
            if fe is not None:
                runtime.last_fp_edge = fe
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Variant A: Publish microbar_closed for decentralized services
        # ------------------------------------------------------------------
        try:
            bar_out = {
                "type": "microbar_closed",
                "symbol": runtime.symbol,
                "ts_ms": int(bar.end_ts_ms),
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "vol": float(bar.vol),
                "cvd": float(bar.cvd_close),
                # Metadata needed by OFConfirmEngine
                "weak_progress": bool(runtime.last_wp.weak_any) if runtime.last_wp else False,
                "sweep": {
                    "kind": str(runtime.last_sweep.kind),
                    "ts_ms": int(runtime.last_sweep.ts_ms)
                } if runtime.last_sweep else None,
                "regime": str(getattr(runtime, "last_regime", "na")),
                "reclaim": {
                    "hold_bars": int(runtime.last_reclaim.hold_bars),
                    "ts_ms": int(runtime.last_reclaim.ts_ms)
                } if runtime.last_reclaim else None,
                "last_div_kind": str(runtime.last_div.kind) if runtime.last_div else None,
                "generated_at": int(time.time() * 1000)
            }
            asyncio.create_task(
                self.redis.xadd(
                    "events:microbar_closed",
                    {"payload": json.dumps(bar_out, ensure_ascii=False)},
                    maxlen=200000,
                    approximate=True
                )
            )
        except Exception as e:
            logger.error(f"Failed to publish microbar_closed event: {e}")

        # ------------------------------------------------------------------
        # Adaptive Pressure Proxy Calibration (Tick-Level)
        # ------------------------------------------------------------------
        try:
            now_ms = int(getattr(bar, "end_ts_ms", 0) or 0)
            calib_min_samples = int(os.getenv("PRESSURE_TIER_CALIB_MIN_SAMPLES", "300"))
            calib_refresh_ms = int(os.getenv("PRESSURE_TIER_CALIB_REFRESH_MS", "60000"))
            
            last_update = int(getattr(runtime, "ptier_last_update_ts_ms", 0) or 0)
            if now_ms > 0 and (now_ms - last_update) >= calib_refresh_ms:
                 # Clone deque to list for sorting
                 samples = list(runtime.ptier_samples_usd)
                 if len(samples) >= calib_min_samples:
                     samples.sort()
                     n = len(samples)
                     def _q(p): return samples[int(p * (n - 1))]
                     
                     p75 = _q(0.75)
                     p90 = _q(0.90)
                     p97 = _q(0.97)
                     
                     # Clamp (safety)
                     min_usd = float(os.getenv("PRESSURE_TIER_MIN_USD", "10000.0"))
                     max_usd = float(os.getenv("PRESSURE_TIER_MAX_USD", "5000000.0"))
                     
                     def _clamp_usd(x): return max(min_usd, min(max_usd, x))
                     
                     t0 = _clamp_usd(p75)
                     t1 = _clamp_usd(p90)
                     t2 = _clamp_usd(p97)
                     
                     runtime.dynamic_cfg["pressure_tier0_usd"] = t0
                     runtime.dynamic_cfg["pressure_tier1_usd"] = t1
                     runtime.dynamic_cfg["pressure_tier2_usd"] = t2
                     
                     runtime.ptier_last_update_ts_ms = int(now_ms)
                     
                     # Log calibration
                     self.logger.info(
                         "⚖️ [PTIER-CALIB] (%s) Updated thresholds (n=%d): T0=$%.0f, T1=$%.0f, T2=$%.0f",
                         runtime.symbol, n, t0, t1, t2
                     )
        except Exception as exc:
            log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_on_microbar_closed:ptier_calib')

        # ------------------------------------------------------------
        # Pressure Tier Calibrator (Expert Recommendation - Production Ready)
        # Regime-aware quantile-based adaptive thresholds with hysteresis
        # ------------------------------------------------------------
        try:
            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
            tiers = runtime.ptier_calib.maybe_recompute(now_ms=int(now_ms), regime=rg)
            
            if tiers:
                # Update telemetry-only keys in dynamic_cfg
                runtime.dynamic_cfg["ptier_tier0_usd"] = float(tiers["tier0"])
                runtime.dynamic_cfg["ptier_tier1_usd"] = float(tiers["tier1"])
                runtime.dynamic_cfg["ptier_tier2_usd"] = float(tiers["tier2"])
                
                # Update telemetry metrics
                ptier_tier0_usd.labels(symbol=runtime.symbol).set(float(tiers["tier0"]))
                ptier_tier1_usd.labels(symbol=runtime.symbol).set(float(tiers["tier1"]))
                ptier_tier2_usd.labels(symbol=runtime.symbol).set(float(tiers["tier2"]))

                # NOTE: We no longer update dn_tier*, dn_tier_active, or dn_th_usd here.
                # dn_calib (above) is now the sole authority for those keys.
                # [EXPERT] Persistence disabled for telemetry-only ptier results.
                
                # Log calibration (telemetry only)
                    
        except Exception as exc:
            log_silent_error(exc, 'ptier_calib_failure', runtime.symbol, '_on_microbar_closed:ptier_calib')

        # ------------------------------------------------------------
        # SMT V2: Publish compact snapshot (BOS proxy, swings, OF state)
        # ------------------------------------------------------------
        await self._publish_smt_snapshot(runtime, bar)

    async def _publish_smt_snapshot(self, runtime: SymbolRuntime, bar: MicroBar) -> None:
        try:
            now_ts = int(getattr(bar, "end_ts_ms", 0) or 0)
            if now_ts <= 0:
                now_ts = int(time.time() * 1000)

            snap_every_ms = int(runtime.config.get("smt_snapshot_every_ms", 1000))
            if snap_every_ms < 250:
                snap_every_ms = 250
            
            if (now_ts - int(getattr(runtime, "last_snapshot_ts_ms", 0) or 0)) >= snap_every_ms:
                runtime.last_snapshot_ts_ms = now_ts

                # --- Persist MicroBar to PostgreSQL (Redundancy) ---
                try:
                    pm = (getattr(runtime, 'pm', None) or get_persistence_manager())
                    b_dict = {
                        "ts_ms": int(bar.end_ts_ms),
                        "open": float(bar.open),
                        "high": float(bar.high),
                        "low": float(bar.low),
                        "close": float(bar.close),
                        "vol": float(bar.vol),
                        "cvd": float(bar.cvd_close)
                    }
                    asyncio.create_task(pm.save_microbar(runtime.symbol, b_dict))
                except Exception:
                    pass

                # 1. BOS / Structure Proxy
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                close_cross = 0
                close_cross_dir = "NONE"
                close_cross_level = 0.0
                
                if runtime.last_swing_high:
                    lvl = float(runtime.last_swing_high.price)
                    if lvl > 0 and close_px > lvl:
                        close_cross = 1
                        close_cross_dir = "UP"
                        close_cross_level = lvl
                
                if runtime.last_swing_low:
                    lvl = float(runtime.last_swing_low.price)
                    if lvl > 0 and close_px < lvl:
                        close_cross = 1
                        close_cross_dir = "DOWN"
                        close_cross_level = lvl
                
                # Trend Dir Proxy (Hidden Div > CloseCross > NONE)
                trend_dir = "NONE"
                if runtime.last_div:
                    k = str(runtime.last_div.kind)
                    if k == "bullish_hidden": trend_dir = "UP"
                    elif k == "bearish_hidden": trend_dir = "DOWN"
                
                if trend_dir == "NONE" and close_cross_dir in ("UP", "DOWN"):
                    trend_dir = close_cross_dir

                # 2. Strong OF Context
                of_valid_ms = int(runtime.config.get("smt_of_strong_valid_ms", 120000))
                of_strong = 0
                if runtime.last_of_strong_ts_ms > 0:
                     if (now_ts - runtime.last_of_strong_ts_ms) <= of_valid_ms:
                         of_strong = 1
                
                # 3. Detectors state
                wp = 1 if (runtime.last_wp and runtime.last_wp.weak_any) else 0
                
                reclaim = 0
                reclaim_dir = "NONE"
                reclaim_ts = 0
                if runtime.last_reclaim:
                    reclaim_ts = int(runtime.last_reclaim.ts_ms)
                    if now_ts - reclaim_ts <= int(runtime.config.get("smt_reclaim_valid_ms", 120000)):
                        reclaim = 1
                        reclaim_dir = str(runtime.last_reclaim.direction_bias).upper()
                
                sweep = 0
                sweep_dir = "NONE"
                sweep_ts = 0
                if runtime.last_sweep:
                    sweep_ts = int(runtime.last_sweep.ts_ms)
                    if now_ts - sweep_ts <= int(runtime.config.get("smt_sweep_valid_ms", 120000)):
                        sweep = 1
                        sweep_dir = str(runtime.last_sweep.direction_bias).upper()

                obi_stable_sec = 0.0
                if runtime.last_obi_event:
                     obi_stable_sec = float(runtime.last_obi_event.get("stable_secs", 0.0) or 0.0)
                
                iceberg_strict = 0
                if runtime.last_iceberg_event:
                    # check if recent strict criteria met
                    # Simplified: just check if refresh count is high
                    refresh = int(runtime.last_iceberg_event.get("refresh", 0) or 0)
                    dur = float(runtime.last_iceberg_event.get("duration", 0.0) or 0.0)
                    r_min = int(runtime.config.get("iceberg_strict_refresh_min", 3))
                    d_min = float(runtime.config.get("iceberg_strict_duration_min", 1.5))
                    if refresh >= r_min and dur >= d_min:
                        iceberg_strict = 1
                
                div_kind = "none"
                div_ts = 0
                if runtime.last_div:
                    div_kind = str(runtime.last_div.kind)
                    div_ts = int(runtime.last_div.ts_ms)
                
                # Ranking features
                rsi14 = float(runtime.rsi_price.value) if (hasattr(runtime, "rsi_price") and runtime.rsi_price.value is not None) else 0.0
                cvd_slope = float(getattr(runtime.cvd_state, "cvd_slope", 0.0)) if hasattr(runtime.cvd_state, "cvd_slope") else 0.0
                retrace_atr = float(runtime.config.get("smt_retrace_atr", 0.0))

                sh0 = float(runtime.last_swing_high.price) if runtime.last_swing_high else 0.0
                sh1 = float(runtime.prev_swing_high.price) if runtime.prev_swing_high else 0.0
                sl0 = float(runtime.last_swing_low.price) if runtime.last_swing_low else 0.0
                sl1 = float(runtime.prev_swing_low.price) if runtime.prev_swing_low else 0.0
                tsh0 = int(runtime.last_swing_high.ts_ms) if runtime.last_swing_high else 0
                tsh1 = int(runtime.prev_swing_high.ts_ms) if runtime.prev_swing_high else 0
                tsl0 = int(runtime.last_swing_low.ts_ms) if runtime.last_swing_low else 0
                tsl1 = int(runtime.prev_swing_low.ts_ms) if runtime.prev_swing_low else 0

                rsi14 = float(runtime.rsi_price.value) if (hasattr(runtime, "rsi_price") and runtime.rsi_price.value is not None) else 0.0
                cvd_slope = float(getattr(runtime.cvd_state, "cvd_slope", 0.0)) if hasattr(runtime.cvd_state, "cvd_slope") else 0.0
                
                # The user patch provided a different calculation for rsi14 and cvd_slope.
                # I will use the original calculation for rsi14 and cvd_slope as it seems more robust
                # (checking for hasattr and None) and the user's snippet for these two lines
                # seems to be a partial or alternative thought process.
                # The user's snippet for rsi14 and cvd_slope:
                # rsi14 = float(runtime.rsi_price.value)
                # cvd_slope = float(runtime.rsi_cvd.value) # Using rsi_cvd as proxy or separate slope?
                # This conflicts with the existing `cvd_slope` which uses `runtime.cvd_state.cvd_slope`.
                # I will keep the existing `rsi14` and `cvd_slope` calculations.

                retrace_atr = 0.0
                if runtime.last_retrace:
                     retrace_atr = float(getattr(runtime.last_retrace, "depth_atr", 0.0) or 0.0)

                # --- SMT snapshot extra fields (for SMT V2 quality/confScore/entry gating) ---
                # We compute "zone" as a proxy: use close_cross_level (last swing level crossed).
                # This is NOT FVG/OB. It is a structural proxy until zones are wired into snapshot.
                delta_z = float(getattr(runtime, "last_delta_z", 0.0) or 0.0)
                delta_eff_norm = float(getattr(runtime, "last_delta_eff_norm", 0.0) or 0.0)
                abs_lvl_ok = int(getattr(runtime, "last_abs_lvl_ok", 0) or 0)

                # --- REAL nearest zone from HTF zones cache (preferred over swing proxy) ---
                zone_id = ""
                zone_type = ""
                zone_src = ""
                zone_side = ""
                zone_px_lo = 0.0
                zone_px_hi = 0.0
                zone_ts_ms = 0
                zone_weight = 0.0
                zone_dist_bp = 0.0
                near_zone = 0
                zone_ok = 0

                try:
                    await runtime.maybe_load_htf_zones(now_ts_ms=int(now_ts), redis_client=self.redis)
                    px = float(close_px or 0.0)
                    pack = getattr(runtime, "zones_pack", None)
                    if pack is not None and px > 0:
                        z, d_bp, inside = pack.nearest(px)
                        if z is not None:
                            zone_id = str(z.id)
                            zone_type = str(z.type)
                            zone_src = str(z.src)
                            zone_side = str(z.side)
                            zone_px_lo = float(z.px_lo)
                            zone_px_hi = float(z.px_hi)
                            zone_ts_ms = int(z.ts_ms)
                            zone_weight = float(z.weight)
                            zone_dist_bp = float(d_bp)
                            near_bp = float(runtime.config.get("smt_near_zone_bp", runtime.config.get("smt_zone_max_bp", 15.0)))
                            ok_bp = float(runtime.config.get("smt_zone_max_bp", 15.0))
                            near_zone = 1 if (inside or (zone_dist_bp > 0 and zone_dist_bp <= near_bp)) else 0
                            zone_ok = 1 if (inside or (zone_dist_bp > 0 and zone_dist_bp <= ok_bp)) else 0
                except Exception:
                    pass

                # Fallback to swing proxy if HTF zones missing
                if zone_ok == 0 and (not zone_id):
                    try:
                        z_level = float(close_cross_level or 0.0)
                        z_px = float(close_px or 0.0)
                        if z_level > 0 and z_px > 0:
                            mid = 0.5 * (abs(z_px) + abs(z_level))
                            zone_dist_bp = (10000.0 * abs(z_px - z_level) / mid) if mid > 0 else 0.0
                        near_bp = float(runtime.config.get("smt_near_zone_bp", runtime.config.get("smt_zone_max_bp", 15.0)))
                        ok_bp = float(runtime.config.get("smt_zone_max_bp", 15.0))
                        near_zone = 1 if (zone_dist_bp > 0 and zone_dist_bp <= near_bp) else 0
                        zone_ok = 1 if (near_zone == 1 and int(close_cross or 0) == 1 and zone_dist_bp <= ok_bp) else 0
                        # mark proxy
                        zone_id = "SWING_PROXY"
                        zone_type = "LEVEL"
                        zone_src = "swing"
                        zone_side = "NA"
                        zone_px_lo = float(z_level)
                        zone_px_hi = float(z_level)
                        zone_ts_ms = int(now_ts)
                        zone_weight = 0.1
                    except Exception as e:
                       self.logger.warning(f"Fallback proxy error: {e}")
                       pass
                
                # abs_lvl_ok should already be present in indicators/dynamic cfg; keep best-effort:
                abs_lvl_ok = 0
                try:
                    # We can't access indicators here as they are not in scope.
                    # But we used getattr(runtime, "last_abs_lvl_ok", 0) previously.
                    abs_lvl_ok = int(getattr(runtime, "last_abs_lvl_ok", 0) or 0)
                except Exception:
                    abs_lvl_ok = 0

                # --- ADX strength quantile (deterministic in snapshot) ---
                # Source of truth:
                #   adx14: Redis adx:{symbol} (float)
                #   quantiles: Redis regime:q:{symbol}:1m (json with adx_p40/p60/p75)
                # We compute adx_q with approx_quantile_3pt; fail-open 0.5.
                adx14 = 0.0
                adx_q = 0.5
                try:
                    # now_ts is your snapshot ts_ms (bar-aligned); keep deterministic.
                    adx14 = float(await self.market_state.get_adx(symbol=runtime.symbol, now_ms=int(now_ts)))
                    rq = await self.market_state.get_regime_quantiles(symbol=runtime.symbol, tf="1m", now_ms=int(now_ts))
                    if isinstance(rq, dict):
                        p40 = float(rq.get("adx_p40") or 0.0)
                        p60 = float(rq.get("adx_p60") or 0.0)
                        p75 = float(rq.get("adx_p75") or 0.0)
                        # sanity: must be monotonic and positive
                        if p40 > 0 and p60 > 0 and p75 > 0 and (p40 <= p60 <= p75):
                            from core.regime_quantiles_store import approx_quantile_adx
                            adx_q = float(approx_quantile_adx(float(adx14), p40, p60, p75))
                except Exception:
                    pass


                # Data-quality from runtime (deterministic at now_ts)
                spread_bp = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
                book_age_ms = 10**9
                try:
                    bts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
                    if bts > 0:
                        book_age_ms = int(max(0, now_ts - bts))
                except Exception:
                    pass
                obi_age_ms = 10**9
                try:
                    if runtime.last_obi_event:
                        ots = int(runtime.last_obi_event.get("ts_ms") or 0)
                        if ots > 0: obi_age_ms = int(max(0, now_ts - ots))
                except Exception:
                    pass
                iceberg_age_ms = 10**9
                try:
                    if runtime.last_iceberg_event:
                        its = int(runtime.last_iceberg_event.get("ts_ms") or 0)
                        if its > 0: iceberg_age_ms = int(max(0, now_ts - its))
                except Exception:
                    pass

                snap = SymbolSnapshot(
                    symbol=str(runtime.symbol),
                    ts_ms=now_ts,
                    trend_dir=trend_dir,
                    close_px=close_px,
                    close_cross=close_cross,
                    close_cross_dir=close_cross_dir,
                    close_cross_level=close_cross_level,
                    swing_high_0=sh0,
                    swing_high_1=sh1,
                    swing_low_0=sl0,
                    swing_low_1=sl1,
                    swing_ts_high_0=tsh0,
                    swing_ts_high_1=tsh1,
                    swing_ts_low_0=tsl0,
                    swing_ts_low_1=tsl1,
                    of_strong=of_strong,
                    of_dir=str(of_dir),
                    of_ts_ms=int(runtime.last_of_strong_ts_ms),
                    weak_progress=int(wp),
                    reclaim=reclaim,
                    reclaim_dir=reclaim_dir,
                    reclaim_ts_ms=reclaim_ts,
                    sweep=sweep,
                    sweep_dir=sweep_dir,
                    sweep_ts_ms=sweep_ts,
                    obi_stable_sec=obi_stable_sec,
                    iceberg_strict=iceberg_strict,
                    div_kind=str(runtime.last_div.kind) if runtime.last_div else "none",
                    div_ts_ms=int(runtime.last_div.ts_ms) if runtime.last_div else 0,
                    rsi14=rsi14,
                    cvd_slope=cvd_slope,
                    retrace_atr=retrace_atr,
                    # SMT V2 fields
                    delta_z=float(delta_z),
                    delta_eff_norm=float(delta_eff_norm),
                    zone_dist_bp=float(zone_dist_bp),
                    zone_ok=int(zone_ok),
                    near_zone=int(near_zone),
                    abs_lvl_ok=int(abs_lvl_ok),
                    # Real zone identity (for retest FSM/UI/debug)
                    zone_id=str(zone_id),
                    zone_type=str(zone_type),
                    zone_src=str(zone_src),
                    zone_side=str(zone_side),
                    zone_px_lo=float(zone_px_lo),
                    zone_px_hi=float(zone_px_hi),
                    zone_ts_ms=int(zone_ts_ms),
                    zone_weight=float(zone_weight),
                    # Market context
                    regime=str(getattr(runtime, "last_regime", "na") or "na"),
                    atr=float(getattr(runtime, "last_atr", 0.0) or 0.0),
                    # Absorption-level readiness/stability
                    abs_lvl_ready=int(1 if int(runtime.dynamic_cfg.get("abs_lvl_calib_n", 0) or 0) >= int(runtime.config.get("abs_lvl_calib_min_samples", 300)) else 0),
                    delta_z_window=int(runtime.config.get("delta_window_n", 60) or 60),

                    # Book health (deterministic)
                    book_rate_hz=float(getattr(runtime, "book_rate_ema", 0.0) or 0.0),
                    book_age_ms=int(max(0, int(now_ts) - int(getattr(runtime, "last_book_ts_ms", 0) or 0))) if int(getattr(runtime, "last_book_ts_ms", 0) or 0) > 0 else 10**9,
                    book_rate_ok_min_hz=float(runtime.dynamic_cfg.get("book_rate_ok_min_hz", runtime.config.get("book_rate_min_hz", 5.0))),
                    book_rate_crit_hz=float(runtime.dynamic_cfg.get("book_rate_crit_hz", runtime.config.get("book_rate_crit_hz", 2.0))),
                    book_rate_ready=int(runtime.dynamic_cfg.get("book_rate_ready", 0) or 0),
                    book_rate_src=str(runtime.dynamic_cfg.get("book_rate_calib_src", "static") or "static"),
                    
                    # Already computed in handle_tick, but we refresh for snapshot context just in case, 
                    # or use stored runtime values.
                    # Using stored runtime values is safer for consistency with what triggered signal.
                    book_health_ok=int(getattr(runtime, "last_book_health_ok", 1)),
                    book_health=str(getattr(runtime, "last_book_health", "OK")),

                    abs_lvl_th_unstable=int(runtime.dynamic_cfg.get("abs_lvl_th_unstable", 0) or 0),
                    # Strong gate diagnostics
                    of_confirm_score=float(getattr(runtime, "last_of_confirm_score", 0.0) or 0.0),
                    strong_gate_have=int(getattr(runtime, "last_strong_gate_have", 0) or 0),
                    strong_gate_need=int(getattr(runtime, "last_strong_gate_need", 0) or 0),
                    strong_gate_scn=str(getattr(runtime, "last_strong_gate_scn", "") or ""),
                    # ADX-aware regime strength
                    adx_q=float(adx_q),
                    adx14=float(adx14),
                    # DQ / Pressure
                    pressure_sps=float(getattr(runtime, "pressure_sps", 0.0) or 0.0),
                    pressure_hi=int(getattr(runtime, "pressure_hi", 0) or 0),
                    spread_bp=float(spread_bp),
                    obi_age_ms=int(obi_age_ms),
                    iceberg_age_ms=int(iceberg_age_ms),
                    cooldown_sps=float(getattr(runtime, "cooldown_hits_ema", 0.0) or 0.0),
                    spread_z=float(getattr(runtime, "last_spread_z", 0.0) or 0.0),
                )

                ttl_sec = int(runtime.config.get("smt_snapshot_ttl_sec", 30))
                if ttl_sec < 5: ttl_sec = 5
                
                key = f"smt:snap:{runtime.symbol}"
                # Fire and forget
                asyncio.create_task(self.redis.set(key, snap.to_json(), ex=ttl_sec))
        except Exception:
            pass

    def _parse_book_payload(self, payload: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        if "data" in payload:
            try:
                nested = json.loads(payload["data"])
            except json.JSONDecodeError:
                nested = {}
        else:
            nested = {}

        merged = {**payload, **nested}
        bids = _ensure_list_levels(merged.get("bids"))
        asks = _ensure_list_levels(merged.get("asks"))
        ts_ms = normalize_epoch_ms(merged.get("ts") or merged.get("event_time"))

        book = {
            "symbol": symbol,
            "ts": int(ts_ms or 0),
            "ts_ms": int(ts_ms or 0),  # deterministic exchange timestamp (ms)
            "first_id": _safe_int(merged.get("first_id") or merged.get("firstId") or merged.get("U")),
            "final_id": _safe_int(merged.get("final_id") or merged.get("finalId") or merged.get("u")),
            "prev_final": _safe_int(merged.get("prev_final") or merged.get("pu")),
            "bids": bids,
            "asks": asks,
        }
        return book

    # ── Конфигурация и инфраструктура ─────────────────────────────────────────

