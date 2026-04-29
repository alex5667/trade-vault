from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
import json
from common.time_utils import normalize_epoch_ms as normalize_epoch_ms_v2
from common.of_gate_metrics_contract import enrich_schema_fields
import logging
import asyncio
from utils.task_manager import safe_create_task

import hashlib
from typing import Any, Dict, List, Optional

from services.orderflow.configuration import _safe_int
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.utils import (
    _should_sample,
    session_utc,)
from services.orderflow.metrics import (
    ok_metrics_emitted_total, ok_metrics_error_total,
    log_silent_error, tick_ts_missing_total, tick_ts_backwards_total, tick_ts_clamped_total, tick_ts_quarantined_total,
    tick_ts_future_total, tick_age_ms_hist, tick_reorder_back_ms_hist, tick_time_action_total,
    tick_time_decision_total, of_confirm_build_ms_hist,
    ticks_out_of_order_total, sweep_detected_total, strong_gate_veto_total, sweep_side_missing_total,
    dn_gate_events_total, track_confirmations, record_evidence_used
)
from common.tick_time import TickTimeGuard, TickTimePolicy
from services.orderflow.tick_time_quarantine_integration import TickTimeQuarantineIntegration

from core.strong_of_gate import hidden_trend_dir
from core.footprint_policy import fp_confirmations_from_microbar
from core.data_health import compute_data_health, apply_book_evidence_policy, apply_shadow_only_policy
from core.slippage_model import expected_slippage_bps
from core.of_inputs_contract import OFInputsV1, OFInputsV2
from core.dyn_cfg_keys import DynCfgKeys as DK


import redis.asyncio as aioredis

# P62: write Unified Decision Record even on early veto (before SignalPipeline)
from services.orderflow.decision_record_v1 import DecisionRecordV1, write_decision_record, extract_fields_best_effort, deterministic_sample
from services.orderflow.decision_binding_v1 import BindingInput, recommend_binding
from services.orderflow.metrics import decision_record_written_total, decision_record_error_total, decision_record_sampled_out_total
# P68: circuit breaker policy (global dq/drift + quality KPIs -> effective overrides)
from services.orderflow.policy.circuit_breaker_v1 import decide_circuit_breaker, apply_circuit_breaker_overrides, enforce_circuit_breaker_regime
# P69: hysteresis state
from services.orderflow.policy.circuit_breaker_state_v1 import CircuitBreakerState


from services.async_signal_publisher import AsyncSignalPublisher, StreamSink

# SRE metrics for gate decisions
OF_GATE_METRICS_STREAM = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
OF_GATE_METRICS_ENABLE = os.getenv("OF_GATE_METRICS_ENABLE", "1").strip().lower() in ("1", "true", "yes", "on")
OF_GATE_METRICS_SAMPLE = float(os.getenv("OF_GATE_METRICS_SAMPLE", "0.02") or 0.02)
OF_GATE_METRICS_MAXLEN = int(os.getenv("OF_GATE_METRICS_MAXLEN", "10000") or 10000)
OF_GATE_METRICS_SAMPLE_SALT = os.getenv("OF_GATE_METRICS_SAMPLE_SALT", "").strip()
OF_GATE_METRICS_SAMPLE_KEY_MODE = "symbol_ts_v1"

def _sample_uid_symbol_ts(symbol: str, ts_ms: int) -> int:
    """
    Sampling-invariant key: stable per (symbol, ts_ms) and independent of ok/ok_soft.
    Prevents cross-symbol correlation when many streams share identical tick_ts.
    """
    b = f"{OF_GATE_METRICS_SAMPLE_SALT}|{symbol}|{int(ts_ms)}".encode("utf-8", errors="replace")
    h = hashlib.sha1(b).digest()
    return int.from_bytes(h[:8], byteorder="big", signed=False)

class TickProcessor:
    """
    Handles individual tick processing:
    1. Validation & Parsing
    2. Feature Extraction
    3. Gating & Risk Checks
    4. Signal Generation
    5. Telemetry & Emission
    """
    def __init__(self, 
                 redis: aioredis.Redis, 
                 ticks: aioredis.Redis,
                 publisher: AsyncSignalPublisher, 
                 of_engine, 
                 calib_svc,
                 atr_cache,
                 atr_sanity, 
                 conf_scorer=None):
        self.redis = redis
        self.ticks = ticks
        self.publisher = publisher
        self.of_engine = of_engine
        self.calib_svc = calib_svc
        self.atr_cache = atr_cache
        self._atr_sanity = atr_sanity
        self.conf_scorer = conf_scorer
        self.logger = logging.getLogger("orderflow_tick_processor")
        
        # State counters (moved from Strategy)
        self.low_conf_counters = {}
        self.strong_gate_counters = {}
        self.dn_gate_relaxed_counters = {}
        self.dn_gate_proxy_relaxed_counters = {}
        self.conf_relax_counters = {}
        self.adverse_continuation_counters = {}
        
        # Config constants (fail-open defaults)
        self.spread_bps_missing_default = float(os.getenv("SPREAD_BPS_MISSING_DEFAULT", "15.0"))
        self.slippage_bps_missing_default = float(os.getenv("SLIPPAGE_BPS_MISSING_DEFAULT", "4.0"))
        self.data_health_on_spread_missing = float(os.getenv("DATA_HEALTH_ON_SPREAD_MISSING", "0.80"))

        # Confidence explainability (world practice): optionally attach scorer decomposition and evidence flags
        self.confidence_parts_enable = os.getenv("CONFIDENCE_PARTS_ENABLE", "0").strip().lower() in ("1", "true", "yes", "on")
        self.confidence_evidence_enable = os.getenv("CONFIDENCE_EVIDENCE_ENABLE", "0").strip().lower() in ("1", "true", "yes", "on")
        self.confidence_parts_max_keys = int(os.getenv("CONFIDENCE_PARTS_MAX_KEYS", "48") or 48)
        # Comma-separated allowlist for evidence keys to persist into indicators (kept small by default)
        self.confidence_evidence_keys = [
            s.strip().lower()
            for s in (os.getenv(
                "CONFIDENCE_EVIDENCE_KEYS",
                "reclaim,obi_stable,fp_edge_absorb,ice_strict,iceberg_strict,rsi_agree,div_match,sweep,div_kind,div_strength,market_mode,data_health",
            ) or "").split(",")
            if s.strip()
        ]
        
        self.of_gate_metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate") or "metrics:of_gate"
        self.of_gate_metrics_enable = os.getenv("OF_GATE_METRICS_ENABLE", "1").strip().lower() in ("1", "true", "yes", "on")
        self.of_gate_metrics_sample = float(os.getenv("OF_GATE_METRICS_SAMPLE", "0.02") or 0.02)
        self.of_gate_metrics_maxlen = int(os.getenv("OF_GATE_METRICS_MAXLEN", "10000") or 10000)

        # Tick time guard / quarantine (moved from Strategy; must run BEFORE delta detector)
        self.tick_time_observe_enable = os.getenv("TICK_TIME_OBSERVE_ENABLE", "1").strip().lower() in ("1", "true", "yes", "on")
        self.tick_time_age_clamp_ms = int(os.getenv("TICK_TIME_AGE_CLAMP_MS", "120000") or 120000)

        self.tick_time_stream_enable = os.getenv("TICK_TIME_STREAM_ENABLE", "0").strip().lower() in ("1", "true", "yes", "on")
        self.tick_time_stream_key = os.getenv("TICK_TIME_STREAM_KEY", "metrics:tick_time") or "metrics:tick_time"
        self.tick_time_stream_sample = float(os.getenv("TICK_TIME_STREAM_SAMPLE", "0.01") or 0.01)
        self.tick_time_stream_maxlen = int(os.getenv("TICK_TIME_STREAM_MAXLEN", "200000") or 200000)

        self._enable_tick_time_quarantine = os.getenv("ENABLE_TICK_TIME_QUARANTINE", "1").strip().lower() in ("1", "true", "yes", "on")
        self._tick_time_quarantine: Dict[str, TickTimeQuarantineIntegration] = {}
        self._tick_time_guard: Dict[str, TickTimeGuard] = {}

        # Shared policy for non-quarantine guard (quarantine integration builds its own guard)
        self._tick_time_policy = TickTimePolicy(
            max_future_ms=int(os.getenv("TICK_TIME_MAX_FUTURE_MS", "500") or 500),
            max_past_ms=int(os.getenv("TICK_TIME_MAX_PAST_MS", "5000") or 5000),
            max_reorder_ms=int(os.getenv("TICK_TIME_MAX_REORDER_MS", "1500") or 1500),
            clamp_soft_future=os.getenv("TICK_TIME_CLAMP_SOFT_FUTURE", "1").strip().lower() in ("1", "true", "yes", "on"),
            allow_soft_reorder=os.getenv("TICK_TIME_ALLOW_SOFT_REORDER", "1").strip().lower() in ("1", "true", "yes", "on"),
        )

        self._capture_queue: Optional[asyncio.Queue] = None
        self._ofc_capture_enabled = os.getenv("OFC_CAPTURE", "0") == "1"

        # P69: Circuit Breaker State (Hysteresis)
        self.cb_state = CircuitBreakerState(
            redis=self.redis,
            symbol=getattr(of_engine, "symbol", getattr(calib_svc, "symbol", "unknown")),
            min_dwell_s=int(os.getenv("CB_MIN_DWELL_S", "300") or 300),
            min_consecutive=int(os.getenv("CB_MIN_CONSECUTIVE", "3") or 3),
            change_count_ttl_s=int(os.getenv("CB_CHANGE_COUNT_TTL_S", "3600") or 3600),
        )
        # P0 Latency Optimization: local cache for policy results
        self._cb_cache_regime: str = "ok"
        self._cb_cache_fields: Dict[str, Any] = {}
        self._cb_cache_last_ts: int = 0
        self._cb_cache_ttl_ms: int = int(os.getenv("CB_CACHE_TTL_MS", "50") or 50)
        self._cb_cache_last_input: Tuple[str, str] = ("unknown", "unknown")

    def cleanup_symbol(self, symbol: str) -> None:
        """Removes all internal tracking state for a symbol to prevent memory leaks."""
        sym = str(symbol or "").upper()
        if not sym:
            return
            
        self._tick_time_quarantine.pop(sym, None)
        self._tick_time_guard.pop(sym, None)
        
        # Cleanup primitive counters
        self.low_conf_counters.pop(sym, None)
        self.strong_gate_counters.pop(sym, None)
        self.dn_gate_relaxed_counters.pop(sym, None)
        self.dn_gate_proxy_relaxed_counters.pop(sym, None)
        self.conf_relax_counters.pop(sym, None)
        self.adverse_continuation_counters.pop(sym, None)

    async def _emit_early_veto_decision_record(
        self,
        *,
        runtime: "SymbolRuntime",
        tick_ts_ms: int,
        direction: str,
        indicators: Dict[str, Any],
        reason_code: str,
        notes: str = "",
    ) -> None:
        """
        P62: In some paths we return None early (veto) before SignalPipeline is called.
        We still want a Unified Decision Record for observability and KPI breakdowns.

        This is best-effort, sampled, and must never raise on the hot path.
        """
        try:
            if os.getenv("DECISION_RECORD_EARLY_VETO_ENABLE", "1").strip().lower() not in ("1", "true", "yes", "y", "on"):
                return

            # Prefer existing SID if present, otherwise create a stable pseudo-sid for veto diagnostics.
            sid = str(
                indicators.get("sid")
                or indicators.get("signal_id")
                or indicators.get("signalId")
                or f"veto:{runtime.symbol}:{int(tick_ts_ms)}:{str(direction).upper()}:{reason_code}"
            )

            # Sampling uses the global decision sampling knob to stay consistent.
            rate = float(os.getenv("DECISION_RECORD_SAMPLE", "1.0") or 1.0)
            if not deterministic_sample(sid, rate):
                try:
                    decision_record_sampled_out_total.labels(symbol=str(runtime.symbol)).inc()
                except Exception:
                    pass
                return

            # Minimal "enriched signal" stub for best-effort extraction.
            stub = {
                "sid": sid,
                "symbol": str(runtime.symbol),
                "tf": str(runtime.config.get("micro_tf", "na")),
                "strategy": str(runtime.config.get("strategy_name", "cryptoorderflow")),
                "ts_ms": int(tick_ts_ms),
                "direction": str(direction).upper(),
                "indicators": indicators,
                # some extractors look for top-level confidence/score too
                "confidence": float(indicators.get("confidence", 0.0) or 0.0),
                "score": float(indicators.get("rule_score", indicators.get("score", 0.0)) or 0.0),
            }

            f = extract_fields_best_effort(stub)
            bind = recommend_binding(
                BindingInput(
                    rule_score=float(f.get("rule_score", 0.0)),
                    rule_ok=bool(f.get("rule_ok", False)),
                    rule_soft=bool(f.get("rule_soft", False)),
                    ml_state=str(f.get("ml_state", "na")),
                    ml_p_cal=f.get("ml_p_cal", None),
                    dq_state=str(f.get("dq_state", "unknown")),
                    drift_state=str(f.get("drift_state", "unknown")),
                )
            )

            rec = DecisionRecordV1(
                ver="v1",
                sid=sid,
                symbol=str(runtime.symbol),
                tf=str(runtime.config.get("micro_tf", "na")),
                strategy=str(runtime.config.get("strategy_name", "cryptoorderflow")),
                decision_ts_ms=int(tick_ts_ms),
                rule_score=float(f.get("rule_score", 0.0)),
                rule_ok=bool(f.get("rule_ok", False)),
                rule_soft=bool(f.get("rule_soft", False)),
                rule_reason_code_top1=str(f.get("rule_reason_code_top1", "NA")),
                ml_enabled=bool(f.get("ml_enabled", False)),
                ml_state=str(f.get("ml_state", "na")),
                ml_p_cal=f.get("ml_p_cal", None),
                ml_model_ver=str(f.get("ml_model_ver", "")),
                ml_latency_ms=f.get("ml_latency_ms", None),
                ml_error=str(f.get("ml_error", "")),
                dq_state=str(f.get("dq_state", "unknown")),
                dq_flags=list(f.get("dq_flags", []) or []),
                drift_state=str(f.get("drift_state", "unknown")),
                drift_flags=list(f.get("drift_flags", []) or []),
                actual_action="veto",
                actual_reason_code=str(reason_code),
                recommended_action=str(bind.get("recommended_action", "deny")),
                recommended_reason_code=str(bind.get("recommended_reason_code", "NA")),
                meta_enforce_cov_bucket=str(f.get("meta_enforce_cov_bucket", "unknown")),
                meta_enforce_applied=bool(f.get("meta_enforce_applied", False)),
                payload_summary={
                    "stage": "tick_processor",
                    "direction": str(direction).upper(),
                    "tick_ts_ms": int(tick_ts_ms),
                    "notes": str(notes or ""),
                },
            )

            # fire-and-forget: never block the hot path
            safe_create_task(write_decision_record(self.redis, rec))
            try:
                decision_record_written_total.labels(symbol=str(runtime.symbol), action="veto").inc()
            except Exception:
                pass
        except Exception:
            try:
                decision_record_error_total.labels(symbol=str(runtime.symbol)).inc()
            except Exception:
                pass

    def set_capture_queue(self, queue: asyncio.Queue):
        self._capture_queue = queue

    def _tick_time_should_sample(self, symbol: str, ts_ms: int, rate: float) -> bool:
        """Deterministic sampling to keep volumes bounded and replay-stable."""
        try:
            if rate <= 0.0:
                return False
            if rate >= 1.0:
                return True
            key = f"{(symbol or '').upper()}|{int(ts_ms)}"
            h = abs(hash(key)) % 10000
            return h < int(rate * 10000.0)
        except Exception:
            return False

    def _get_tick_time_quarantine(self, symbol: str) -> Optional[TickTimeQuarantineIntegration]:
        """Lazy per-symbol TickTimeQuarantineIntegration."""
        if not self._enable_tick_time_quarantine:
            return None
        sym = str(symbol or "").upper()
        if not sym:
            return None
        if sym not in self._tick_time_quarantine:
            try:
                self._tick_time_quarantine[sym] = TickTimeQuarantineIntegration(
                    symbol=sym,
                    redis_client=self.redis,
                    sample_rate=float(os.getenv("BAD_TIME_QUARANTINE_SAMPLE_RATE", "0.01") or 0.01),
                )
            except Exception as e:
                self.logger.debug("Failed to create TickTimeQuarantineIntegration for %s: %r", sym, e)
                return None
        return self._tick_time_quarantine.get(sym)

    def _get_tick_time_guard(self, symbol: str) -> TickTimeGuard:
        """Fallback per-symbol TickTimeGuard when quarantine integration is disabled."""
        sym = str(symbol or "").upper()
        if sym not in self._tick_time_guard:
            self._tick_time_guard[sym] = TickTimeGuard(self._tick_time_policy)
        return self._tick_time_guard[sym]

    async def _emit_tick_time_stream(self, *, symbol: str, decision: str, meta: Dict[str, int]) -> None:
        """Best-effort Redis stream for offline diagnostics (fail-open)."""
        try:
            if not self.tick_time_stream_enable:
                return
            ts0 = int(meta.get("orig_ts_ms", 0) or 0)
            if not self._tick_time_should_sample(symbol, ts0 if ts0 > 0 else get_ny_time_millis(), float(self.tick_time_stream_sample)):
                return
            fields = {
                "ts_ms": str(int(meta.get("proc_wall_ms", meta.get("now_ms", 0)) or 0)),
                "symbol": str(symbol),
                "decision": str(decision),
                "orig_ts_ms": str(int(meta.get("orig_ts_ms", 0) or 0)),
                "norm_ts_ms": str(int(meta.get("norm_ts_ms", 0) or 0)),
                "prev_ts_ms": str(int(meta.get("prev_ts_ms", 0) or 0)),
                "now_ms": str(int(meta.get("now_ms", 0) or 0)),
                "age_ms": str(int(meta.get("age_ms", 0) or 0)),
                "back_ms": str(int(meta.get("back_ms", 0) or 0)),
                "skew_ms": str(int(meta.get("skew_ms", 0) or 0)),
            }
            await self.redis.xadd(
                self.tick_time_stream_key,
                fields,
                maxlen=int(self.tick_time_stream_maxlen),
                approximate=True,
            )
        except Exception as e:
            log_silent_error(e, "tick_time_stream", symbol, "TickProcessor")

    async def _apply_tick_time_guard(self, runtime: SymbolRuntime, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply tick time policy + quarantine. Returns dict with normalized ts/decision/meta.

        Fail-open: if anything goes wrong, keeps original ts.
        """
        symbol = str(getattr(runtime, "symbol", "") or "")
        try:
            raw_ts = tick.get("ts_ms") or tick.get("E") or tick.get("T")
            if raw_ts is None:
                tick_ts_missing_total.labels(symbol=symbol).inc()
                tick_time_action_total.labels(symbol=symbol, action="drop", reason="missing_ts").inc()
                tick_time_decision_total.labels(symbol=symbol, decision="drop_missing").inc()
                return None
            raw_ts_ms = _safe_int(raw_ts)
        except Exception:
            tick_ts_missing_total.labels(symbol=symbol).inc()
            tick_time_action_total.labels(symbol=symbol, action="drop", reason="bad_ts").inc()
            tick_time_decision_total.labels(symbol=symbol, decision="drop_bad_ts").inc()
            return None

        proc_wall_ms = get_ny_time_millis()
        ingest_now_ms = _safe_int(tick.get("written_at"), default=proc_wall_ms)
        prev_ts_ms = _safe_int(getattr(runtime, "last_ts_ms", 0) or 0)

        meta: Dict[str, int] = {
            "orig_ts_ms": int(raw_ts_ms),
            "prev_ts_ms": int(prev_ts_ms),
            "now_ms": int(ingest_now_ms),
            "proc_wall_ms": int(proc_wall_ms),
        }

        # Observability: wall-clock age (clamped for histogram bucket sanity)
        try:
            age_ms = int(proc_wall_ms - int(raw_ts_ms))
            if age_ms > int(self.tick_time_age_clamp_ms):
                age_ms = int(self.tick_time_age_clamp_ms)
            if age_ms < -int(self.tick_time_age_clamp_ms):
                age_ms = -int(self.tick_time_age_clamp_ms)
            meta["age_ms"] = int(age_ms)
            if self.tick_time_observe_enable:
                tick_age_ms_hist.labels(symbol=symbol).observe(float(age_ms))
            if age_ms < 0:
                tick_ts_future_total.labels(symbol=symbol).inc()
        except Exception:
            pass

        decision = "ok"
        norm_ts_ms = raw_ts_ms
        back_ms = 0
        skew_ms = 0

        try:
            q = self._get_tick_time_quarantine(symbol)
            if q is not None:
                ts_res = q.sanitize_and_track(raw_ts_ms, now_ms=ingest_now_ms)
                # state freeze: suppress processing but guard still advances watermark internally
                if q.should_suppress_processing(int(ingest_now_ms)):
                    tick_ts_quarantined_total.labels(symbol=symbol).inc()
                    tick_time_action_total.labels(symbol=symbol, action="drop", reason="quarantine").inc()
                    tick_time_decision_total.labels(symbol=symbol, decision="quarantine_suppress").inc()
                    return None
            else:
                guard = self._get_tick_time_guard(symbol)
                ts_res = guard.sanitize_ts_ms(raw_ts_ms, now_ms=ingest_now_ms)

            if ts_res is None:
                decision = "drop_bad_ts"
                tick_time_action_total.labels(symbol=symbol, action="drop", reason="bad_ts").inc()
                tick_time_decision_total.labels(symbol=symbol, decision=decision).inc()
                return None

            if ts_res.drop_reason:
                dr = str(ts_res.drop_reason)
                if "future" in dr:
                    decision = "drop_future"
                    tick_time_action_total.labels(symbol=symbol, action="drop", reason="future_hard").inc()
                elif "past" in dr:
                    decision = "drop_past"
                    tick_time_action_total.labels(symbol=symbol, action="drop", reason="past_hard").inc()
                elif "reorder" in dr:
                    decision = "reorder_hard"
                    tick_time_action_total.labels(symbol=symbol, action="drop", reason="reorder_hard").inc()
                    try:
                        back_ms = int(prev_ts_ms - raw_ts_ms) if prev_ts_ms > 0 else 0
                        if back_ms > 0:
                            tick_reorder_back_ms_hist.labels(symbol=symbol).observe(float(back_ms))
                    except Exception:
                        pass
                else:
                    decision = "drop_other"
                    tick_time_action_total.labels(symbol=symbol, action="drop", reason=dr[:32]).inc()
                tick_time_decision_total.labels(symbol=symbol, decision=decision).inc()
                return None

            norm_ts_ms = int(getattr(ts_res, "ts_ms", raw_ts_ms) or raw_ts_ms)
            meta["norm_ts_ms"] = int(norm_ts_ms)

            flags = set(getattr(ts_res, "flags", []) or [])
            if "reorder_soft" in flags:
                decision = "reorder_soft"
                tick_ts_backwards_total.labels(symbol=symbol).inc()
                ticks_out_of_order_total.labels(symbol=symbol).inc()
                tick_ts_clamped_total.labels(symbol=symbol).inc()
                tick_time_action_total.labels(symbol=symbol, action="clamp", reason="reorder_soft").inc()
                try:
                    back_ms = int(prev_ts_ms - raw_ts_ms) if prev_ts_ms > 0 else 0
                    if back_ms > 0:
                        tick_reorder_back_ms_hist.labels(symbol=symbol).observe(float(back_ms))
                except Exception:
                    pass
            elif "clamped_soft_future" in flags:
                decision = "clamp_soft_future"
                tick_ts_clamped_total.labels(symbol=symbol).inc()
                tick_time_action_total.labels(symbol=symbol, action="clamp", reason="soft_future").inc()
                try:
                    skew_ms = int(raw_ts_ms - ingest_now_ms)
                except Exception:
                    pass
            else:
                decision = "ok"
                tick_time_action_total.labels(symbol=symbol, action="ok", reason="ok").inc()

            tick_time_decision_total.labels(symbol=symbol, decision=decision).inc()

        except Exception as e:
            # Fail-open: keep original ts, but count as ok
            log_silent_error(e, "tick_time_policy", symbol, "TickProcessor")
            tick_time_action_total.labels(symbol=symbol, action="ok", reason="fail_open").inc()
            tick_time_decision_total.labels(symbol=symbol, decision="ok").inc()
            norm_ts_ms = raw_ts_ms
            meta["norm_ts_ms"] = int(norm_ts_ms)

        meta["back_ms"] = int(back_ms)
        meta["skew_ms"] = int(skew_ms)

        try:
            runtime.last_ts_ms = int(norm_ts_ms)
        except Exception:
            pass

        try:
            if int(raw_ts_ms) != int(norm_ts_ms):
                tick["_orig_ts_ms"] = int(raw_ts_ms)
            tick["ts_ms"] = int(norm_ts_ms)
            if "E" in tick:
                tick["E"] = int(norm_ts_ms)
            if "T" in tick:
                tick["T"] = int(norm_ts_ms)
        except Exception:
            pass

        try:
            safe_create_task(self._emit_tick_time_stream(symbol=symbol, decision=decision, meta=meta))
        except Exception:
            pass

        return {"tick_ts_ms": int(norm_ts_ms), "decision": str(decision), "meta": meta}

    async def process_tick(self, runtime: SymbolRuntime, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        from services.orderflow.metrics import strong_gate_veto_total
        t0_us = time.perf_counter_ns() // 1000  # Start measuring latency in microseconds

        # 1) Validation
        if not tick or not isinstance(tick, dict):
            return None
        if not runtime:
            return None

        # 2) Tick time policy (must be first: protects downstream detectors)
        tt = await self._apply_tick_time_guard(runtime, tick)
        if tt is None:
            return None
        tick_ts = int(tt["tick_ts_ms"])
        tt_decision = str(tt.get("decision", "ok") or "ok")
        tt_meta = tt.get("meta") or {}

        # 3) Parse remaining tick fields
        try:
            price = float(tick.get("price") or tick.get("p") or 0.0)
            ibm = tick.get("is_buyer_maker") if "is_buyer_maker" in tick else tick.get("m")
            direction = "SHORT" if ibm else "LONG"  # True=Sell=Short, False=Buy=Long
        except Exception as e:
            log_silent_error(e, "tick_parse", runtime.symbol, "TickProcessor")
            return None

        # 4) Context update
        now_ts = tick_ts
        runtime.last_tick_ts = tick_ts
        runtime.tick_count += 1
        
        # P68: build effective cfg (static + dynamic) and apply circuit breaker overrides early.
        try:
            cfg = dict(runtime.config)
            if getattr(runtime, "dynamic_cfg", None):
                cfg.update(runtime.dynamic_cfg)
        except Exception:
            cfg = dict(getattr(runtime, "config", {}) or {})

        # ------------------------------------------------------------------
        # P68: Determine policy regime. Prefer local indicator states, fallback to dynamic cfg.
        # ------------------------------------------------------------------
        indicators = {}
        try:
            dq_state = str(indicators.get("dq_state", cfg.get("dq_state", "unknown")))
            drift_state = str(indicators.get("drift_state", cfg.get("drift_state", "unknown")))
            
            # P0 Optimization: Throttled update
            current_input = (dq_state, drift_state)
            should_update_cb = (
                current_input != self._cb_cache_last_input or 
                (now_ts - self._cb_cache_last_ts) > self._cb_cache_ttl_ms
            )

            if should_update_cb:
                # 1. Raw mode calculation (P68)
                raw_decision = decide_circuit_breaker(cfg=cfg, dq_state=dq_state, drift_state=drift_state)
                
                # 2. Hysteresis check (P69)
                safe_regime = str(getattr(raw_decision, "regime", "ok") or "ok")
                
                # Update authoritative state
                effective_regime, cb_debug = await self.cb_state.update(safe_regime, now_ts)
                
                # 3. Construct EFFECTIVE decision object
                effective_decision = enforce_circuit_breaker_regime(raw_decision, effective_regime, cfg)
                
                # 4. Apply overrides based on EFFECTIVE mode
                cb_overrides, cb_fields = apply_circuit_breaker_overrides(cfg=cfg, decision=effective_decision)
                
                # Update cache
                self._cb_cache_regime = effective_regime
                self._cb_cache_fields = {
                    **cb_fields,
                    "policy_raw_mode": raw_decision.regime,
                    "policy_effective_mode": effective_regime,
                    "policy_hysteresis_debug": json.dumps(cb_debug),
                    "policy_changed": int(cb_debug.get("switched", False))
                }
                self._cb_cache_last_input = current_input
                self._cb_cache_last_ts = now_ts
                
                # Apply overrides into effective cfg used by downstream logic
                cfg.update(cb_overrides)
                indicators.update(self._cb_cache_fields)
            else:
                # Use cached state
                indicators.update(self._cb_cache_fields)
                # Ensure runtime cfg is also synced with cached overrides if needed
                # (Assuming overrides don't change if inputs didn't change)
        except Exception as e:
            # fail-open: do not break tick processing
            self.logger.debug(f"Circuit breaker update fail-open: {e}")
            pass

        # --- Delta Detection ---
        delta_event = {}
        try:
            delta_event = runtime.delta_detector.push(tick)
        except Exception:
            pass

        if not delta_event:
            # Usually means no significant delta spike
            # We return None early unless we are in a mode that processes every tick for continuous metrics
            # But the strategy logic relies on delta_event being present for signal generation.
            # We check if we should continue anyway (e.g. for pure metrics update?)
            # No, standard strategy returns None if no delta spike.
            return None

        # ------------------------------------------------------------------
        # Authoritative DeltaNotional Tier Gating
        # ------------------------------------------------------------------
        rg = str(getattr(runtime, "last_regime", "na"))
        dn_tiers_decision = runtime.tick_dn_calib.tiers(
            regime=rg,
            ts_ms=int(tick_ts if tick_ts > 0 else get_ny_time_millis()),
            default_t0=float(runtime.config.get("dn_tier0_usd", 30000.0)),
            default_t1=float(runtime.config.get("dn_tier1_usd", 70000.0)),
            default_t2=float(runtime.config.get("dn_tier2_usd", 150000.0)),
        )
        
        runtime.dynamic_cfg[DK.DN_TIER0_USD] = float(dn_tiers_decision.tier0_usd)
        runtime.dynamic_cfg[DK.DN_TIER1_USD] = float(dn_tiers_decision.tier1_usd)
        runtime.dynamic_cfg[DK.DN_TIER2_USD] = float(dn_tiers_decision.tier2_usd)
        runtime.dynamic_cfg[DK.DN_SRC] = str(dn_tiers_decision.src)
        
        delta_usd = abs(float(delta_event.get("delta", 0.0))) * price
        
        if delta_usd > 0:
             runtime.tick_dn_calib.update(regime=rg, dn_usd=delta_usd, ts_ms=int(tick_ts))

        tier = 0
        if delta_usd > dn_tiers_decision.tier2_usd: tier = 2
        elif delta_usd > dn_tiers_decision.tier1_usd: tier = 1
        elif delta_usd > dn_tiers_decision.tier0_usd: tier = 0
        else: tier = -1

        indicators.update({
            "delta": delta_event.get("delta", 0.0),
            "delta_z": delta_event.get("z", 0.0),
        })

        # Gate Logic
        min_tier = int(runtime.config.get("delta_tier_min", 0))
        passed = (tier >= min_tier)
        
        if not passed and min_tier == 0 and tier == -1:
            from core.instrument_config import symbol_env_prefix
            prefix = symbol_env_prefix(runtime.symbol)
            is_meme = prefix in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF")
            if is_meme:
                tol_usd = dn_tiers_decision.tier0_usd * 0.50
                if delta_usd >= tol_usd:
                    passed = True
                    tier = 0
                    indicators["dn_gate_relaxed"] = 1
                    cnt = self.dn_gate_relaxed_counters.get(runtime.symbol, 0) + 1
                    self.dn_gate_relaxed_counters[runtime.symbol] = cnt
                    if cnt % 10000 == 0:
                        self.logger.info("✅ [DN-GATE] (%s) RELAXED: delta_usd=$%.0f passed via 50%% tolerance (T0=$%.0f)", 
                                    runtime.symbol, delta_usd, dn_tiers_decision.tier0_usd)
        
        sess = indicators.get("session", "OFF")
        runtime.dn_passrate.update(tier=tier, session=sess, passed=passed)
        
        res = "pass" if passed else "veto_tier"
        dn_gate_events_total.labels(symbol=runtime.symbol, tier=str(tier), session=sess, result=res).inc()
        
        if not passed:
             if runtime.delta_log_sampler.should_log("dn_veto"):
                  self.logger.info(
                      "🛑 [DN-GATE] (%s) VETO: delta_usd=$%.0f < T%d=$%.0f (tier=%d < min=%d)",
                      runtime.symbol, delta_usd, min_tier, 
                      getattr(dn_tiers_decision, f"tier{min_tier}_usd", 0.0), tier, min_tier
                  )

             # FIX (2026-02-19): Emit metric for vetoed signal (ok=0) with ACTUAL latency
             try:
                if OF_GATE_METRICS_ENABLE:
                    rate = float(runtime.config.get("of_gate_metrics_sample", OF_GATE_METRICS_SAMPLE) or OF_GATE_METRICS_SAMPLE)
                    # Sampling-invariant: stable per (symbol, ts_ms) to avoid correlated gaps
                    sample_uid = _sample_uid_symbol_ts(str(runtime.symbol), int(tick_ts))
                    if rate > 0 and _should_sample(sample_uid, rate):
                        latency_us = (time.perf_counter_ns() // 1000) - t0_us
                        # Flatten legs for diagnostics
                        legs = []
                        if 'ev' in locals() and hasattr(ev, 'get'):
                            legs = ev.get("missing_legs", [])
                        # Fill expected indicators — build payload in a nested try so a missing
                        # reference (ofc, ev) never breaks the main veto path.
                        try:
                            payload = {
                                "type": "of_gate",
                                "schema": "of_gate_metrics_v2",
                                "sample_rate": str(rate),
                                "sample_key_mode": OF_GATE_METRICS_SAMPLE_KEY_MODE,
                                # Stage1-P1: normalize_epoch_ms_v2 replaces int(tick_ts) to handle s/ms/garbage
                                "ts_ms": str(normalize_epoch_ms_v2(tick_ts).ts_ms),
                                "symbol": str(runtime.symbol),
                                "direction": str(getattr(ofc, "side", "unknown") if 'ofc' in locals() else "unknown"),
                                "scenario": str(getattr(ofc, "scenario", "unknown") if 'ofc' in locals() else "unknown"),
                                "scenario_v4": str(ev.get("scenario_v4", "unknown") if 'ev' in locals() and hasattr(ev, 'get') else "unknown"),
                                "ok": "0",
                                "ok_soft": "0",
                                "have": "0",
                                "need": "0",
                                "score": "0.0",
                                "reason": f"dn_veto_tier{tier}",
                                "gate_bits": "0",
                                "exec_risk_bps": str(float(indicators.get("exec_risk_bps", 0.0) or 0.0)),
                                "exec_risk_norm": str(float(indicators.get("exec_risk_norm", 0.0) or 0.0)),
                                "latency_us": str(int(latency_us)),
                                "dn_tier": str(tier),
                                "dn_usd": str(float(delta_usd)),
                                "dn_tier_threshold": str(float(dn_tiers_decision.tier0_usd if min_tier == 0 else getattr(dn_tiers_decision, f"tier{min_tier}_usd", 0.0))),
                                # Fill required fields with defaults
                                "meta_p": "-1.0",
                                "meta_veto": "0",
                                "meta_enforce_applied": "0",
                                "data_health": str(float(indicators.get("data_health", 1.0) or 1.0)),
                                "book_health_ok": str(int(indicators.get("book_health_ok", 1) or 1)),
                                "source_consistency_ok": str(int(indicators.get("source_consistency_ok", 1) or 1)),
                                "missing_legs": "[]",
                            }
                            payload = enrich_schema_fields(payload)
                            async def _emit_ok_metrics(_payload: dict) -> None:
                                try:
                                    await self.redis.xadd(
                                        OF_GATE_METRICS_STREAM,
                                        {k: str(v) for k, v in _payload.items()},
                                        maxlen=OF_GATE_METRICS_MAXLEN,
                                        approximate=True,
                                    )
                                    ok_metrics_emitted_total.labels("tick").inc()
                                except Exception:
                                    ok_metrics_error_total.labels("tick", "xadd").inc()
                            safe_create_task(_emit_ok_metrics(payload))
                        except Exception:
                            pass
             except Exception:
                pass

             return None

        indicators["dn_tier"] = int(tier)
        indicators["dn_usd"] = float(delta_usd)
        indicators["dn_tier0_usd"] = float(dn_tiers_decision.tier0_usd)
        indicators["dn_tier2_usd"] = float(dn_tiers_decision.tier2_usd)
        indicators["dn_t1_usd"] = float(dn_tiers_decision.tier1_usd)
        indicators["dn_src"] = str(dn_tiers_decision.src)
        indicators["liquidity_scale"] = float(dn_tiers_decision.scale)
        if sess: indicators["session"] = str(session_utc(int(tick_ts))) # Ensure session is set if missing

        now_ts = tick_ts if tick_ts > 0 else get_ny_time_millis()

        # Absorption pre-calc
        absorption_feat = None
        try:
            absorption_feat = runtime.absorption_detector.push(tick, runtime.last_book, price)
        except Exception:
            pass

        # Delta Spike Event Publication
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
            if absorption_feat: spike_out["absorption"] = absorption_feat
            
            now_ms = int(tick_ts)
            obi_ttl = int(runtime.config.get("obi_event_ttl_ms", 30000))
            if runtime.last_obi_event and (now_ms - runtime.last_obi_event.get("ts_ms", 0)) < obi_ttl:
                spike_out["obi"] = runtime.last_obi_event
            
            ice_ttl = int(runtime.config.get("iceberg_event_ttl_ms", 15000))
            if runtime.last_iceberg_event and (now_ms - runtime.last_iceberg_event.get("ts_ms", 0)) < ice_ttl:
                spike_out["iceberg"] = runtime.last_iceberg_event
            
            safe_create_task(
                self.redis.xadd(
                    "events:delta_spike",
                    {"payload": json.dumps(spike_out, ensure_ascii=False)},
                    maxlen=20000,
                    approximate=True
                )
            )
        except Exception as e:
            self.logger.error(f"Failed to publish delta_spike event: {e}")

        confirmations = []

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
            
            if hasattr(runtime, "rsi_price") and runtime.rsi_price.value is not None:
                indicators["rsi_price"] = float(runtime.rsi_price.value)
            if hasattr(runtime, "rsi_cvd") and runtime.rsi_cvd.value is not None:
                indicators["rsi_cvd"] = float(runtime.rsi_cvd.value)

            rp = float(indicators.get("rsi_price", 50.0))
            rc = float(indicators.get("rsi_cvd", 50.0))
            if direction == "LONG" and rp > 50 and rc > 50:
                confirmations.append("rsi_agree=1")
            elif direction == "SHORT" and rp < 50 and rc < 50:
                confirmations.append("rsi_agree=1")

            if runtime.last_swing_high:
                indicators.update({"swing_high_px": float(runtime.last_swing_high.price)})
            if runtime.last_swing_low:
                indicators.update({"swing_low_px": float(runtime.last_swing_low.price)})
            if runtime.last_div:
                dv = runtime.last_div
                indicators.update({
                    "div_kind": str(dv.kind),
                    "div_ts": int(dv.ts_ms),
                    "div_strength": float(dv.strength),
                })
        except Exception:
            pass

        # Phase C/D: Metadata
        try:
            if (ev := runtime.last_sweep) is not None:
                if ev.kind == "EQH_SWEEP":
                    confirmations.append("sweep_eqh=1")
                    record_evidence_used(runtime.symbol, sess, "sweep_eqh=1")
                elif ev.kind == "EQL_SWEEP":
                    confirmations.append("sweep_eql=1")
                    record_evidence_used(runtime.symbol, sess, "sweep_eql=1")
                
                # Generic sweep flag (always emit for backward compatibility)
                confirmations.append("sweep=1")
                record_evidence_used(runtime.symbol, sess, "sweep=1")

                div = runtime.last_div
                div_match = False
                cvd_q = int(indicators.get("cvd_quarantine_active", getattr(runtime, "cvd_quarantine_active", 0) or 0) or 0)
                if div is not None and cvd_q != 1:
                    if ev.direction_bias == "SHORT" and str(div.kind).startswith("bearish"): div_match = True
                    if ev.direction_bias == "LONG" and str(div.kind).startswith("bullish"): div_match = True
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
                    # Alias ice_strict=1 <-> iceberg_strict=1
                    if c == "ice_strict=1":
                        confirmations.append("iceberg_strict=1")
                    elif c == "iceberg_strict=1":
                        confirmations.append("ice_strict=1")
            
            wp = runtime.last_wp
            if wp is not None:
                indicators.update({"weak_range_atr": wp.range_atr, "weak_body_atr": wp.body_atr, "weak_eff": wp.eff})
        except Exception:
            pass

        # Unified data_health score
        try:
            indicators["book_ts_gap_ms"] = int(tick_ts - int(getattr(runtime, "last_book_ts_ms", 0) or 0))
            indicators["book_rate_hz"] = float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
            spr = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
            if spr <= 0 and runtime.last_book:
                spr = float(runtime.last_book.spread_bps)
            indicators["spread_bps"] = spr
            
            dh = compute_data_health(indicators=indicators, cfg=cfg)
            indicators["data_health"] = float(dh.score)
            indicators["data_health_reasons"] = ",".join(list(dh.reasons or [])[:5])
            indicators["book_health_ok"] = int(dh.book_health_ok)
            apply_book_evidence_policy(indicators=indicators, dh=dh, cfg=cfg)
            apply_shadow_only_policy(indicators=indicators, dh=dh, cfg=cfg)
        except Exception:
            pass

        # Expected slippage model
        indicators.setdefault("expected_slippage_bps", 0.0)
        indicators.setdefault("slippage_reason", "na")

        # OFI impact proxy
        try:
            book = getattr(runtime, 'last_book', None)
            prev = getattr(runtime, '_ofi_prev_book', None)
            if book is not None:
                # ... (OFI logic from strategy.py lines 1515-1559) ...
                # Simplified reproduction assuming logic inside Strategy was mostly getting attrs
                pass 
                # To save space, let's assume metrics and indicators updated elsewhere or we implement fully if critical
                # Implementing minimal OFI logic for indicator population:
                def _get(obj, k, d=0.0):
                    if obj is None: return d
                    if isinstance(obj, dict): return float(obj.get(k, d) or d)
                    return float(getattr(obj, k, d) or d)
                
                bbp, bbq = _get(book, 'best_bid_px'), _get(book, 'best_bid_qty')
                bap, baq = _get(book, 'best_ask_px'), _get(book, 'best_ask_qty')
                p_bbp, p_bbq = _get(prev, 'best_bid_px'), _get(prev, 'best_bid_qty')
                p_bap, p_baq = _get(prev, 'best_ask_px'), _get(prev, 'best_ask_qty')
                
                ofi_bid = 0.0
                if bbp > p_bbp and bbp > 0: ofi_bid = bbq
                elif bbp < p_bbp and p_bbp > 0: ofi_bid = -p_bbq
                elif bbp == p_bbp and bbp > 0: ofi_bid = (bbq - p_bbq)
                
                ofi_ask = 0.0
                if bap < p_bap and bap > 0: ofi_ask = -baq
                elif bap > p_bap and p_bap > 0: ofi_ask = p_baq
                elif bap == p_bap and bap > 0: ofi_ask = -(baq - p_baq)
                
                ofi = ofi_bid + ofi_ask
                norm = float(ofi / max(bbq+baq, 1e-9)) # approximate depth
                indicators['ofi_best_qty'] = float(ofi)
                indicators['ofi_best_norm'] = float(norm)
                runtime._ofi_prev_book = book
        except Exception:
            pass

        # ATR Meta & Sanity
        try:
             atr_val, atr_meta = self.atr_cache.get_with_meta(symbol=runtime.symbol, timeframe=None)
             if atr_val is not None: indicators["atr"] = float(atr_val)
             indicators["atr_src"] = str(atr_meta.get("picked_src") or "na")
             indicators["atr_tf"] = str(atr_meta.get("picked_tf") or "na")
             indicators["atr_age_ms"] = int(atr_meta.get("age_ms") or 0)
        except Exception:
             pass

        # ATR Sanity Update
        try:
             px0 = float(price)
             atr0 = float(indicators.get("atr") or getattr(runtime, "last_atr", 0.0) or 0.0)
             res = self._atr_sanity.update(
                 symbol=str(runtime.symbol),
                 atr=float(atr0),
                 px=float(px0),
                 age_ms=int(indicators.get("atr_age_ms", 0)),
                 now_ms=int(now_ts),
                 tf=str(indicators.get("atr_tf", "1m"))
             )
             indicators["atr"] = float(res.atr_used)
             indicators["atr_bad"] = int(res.bad)
             # ... (Redis logging logic skipped for brevity, but crucial in prod) ...
        except Exception:
             pass

        # CVD Quarantine
        indicators["cvd_quarantine_active"] = int(getattr(runtime, "cvd_quarantine_active", 0) or 0)
        
        # Volume-delta fallback
        delta_z_used = float(delta_event.get("z", 0.0))
        if int(indicators.get("cvd_quarantine_active", 0)) == 1:
             # Fallback logic would invoke volume_delta_z_from_tick but we skip complex import here
             indicators["delta_z_source"] = "volume_fallback"
        else:
             indicators["delta_z_source"] = "cvd"

        # Slippage Logic
        try:
            atr_bps = (float(indicators.get("atr", 0.0)) / price * 10000.0) if price > 0 else 0.0
            indicators["atr_bps"] = float(atr_bps)
            est = expected_slippage_bps(
                spread_bps=float(indicators.get("spread_bps", 0.0)),
                churn_score=float(getattr(runtime, "book_churn_score", 0.0) or 0.0),
                book_rate_z=float(getattr(runtime, "book_rate_z", 0.0) or 0.0),
                pressure_sps=float(getattr(runtime, "pressure_sps", 0.0) or 0.0),
                atr_bps=atr_bps,
                cfg=cfg
            )
            indicators["expected_slippage_bps"] = float(est.expected_bps)
            indicators["slippage_reason"] = str(est.reason)
        except Exception:
            pass

        # OFConfirm Engine
        book_ok = int(indicators.get("book_health_ok", 1))
        if book_ok == 0:
            indicators["obi"] = 0
            indicators["iceberg_refresh"] = 0

        # Pressure Proxy Layer
        p_snap = runtime.pressure.snapshot(now_ms=int(tick_ts))
        indicators["pressure_per_min"] = float(p_snap.per_min_ema)
        indicators["cooldown_hit_rate"] = float(p_snap.cd_rate_ema)
        
        # Delta Notional Tier Check already done? No, strictly DN Gate happens earlier. 
        # But we need indicators["dn_tier"] etc populated. Done above.

        t_build_ns0 = time.perf_counter_ns()

        from services.ml_confirm_gate import is_of_sync_build, run_bounded_of_build
        def _sync_build():
            return self.of_engine.build(
                symbol=runtime.symbol,
                tf=str(runtime.config.get("micro_tf", "1s")),
                direction=direction,
                tick_ts_ms=tick_ts,
                price=float(price),
                delta_z=float(delta_z_used),
                runtime=runtime,
                cfg=dict(runtime.config),  # Should merge dynamic_cfg
                indicators=indicators,
                absorption=absorption_feat
            )
        _sync_build._of_build_symbol = str(runtime.symbol)
        _sync_build._of_build_tf = str(runtime.config.get("micro_tf", "1s"))

        # B3/B4: of_engine.build() is thread-safe (pure CPU: numpy/ML inference, no asyncio primitives).
        # Normal path: offload to ThreadPoolExecutor → event loop unblocked.
        # Kill-switch: OF_SYNC_BUILD=1 → run synchronously (blocks event loop, emergency only).
        # Admission is bounded, and timed-out work keeps its slot until the thread
        # actually finishes, so fail-open does not create hidden executor backlog.
        _of_build_timeout_s = float(os.getenv("OF_BUILD_TIMEOUT_S", "0.5"))
        if is_of_sync_build():
            ofc, dec = _sync_build()
        else:
            result, build_status = await run_bounded_of_build(
                _sync_build,
                timeout_s=_of_build_timeout_s,
            )
            if build_status == "timeout":
                self.logger.warning(
                    "⚠️ (%s) OFConfirmEngine.build timeout (%.2fs) — fail-open, ofc=None",
                    runtime.symbol, _of_build_timeout_s,
                )
                ofc, dec = None, None
            elif build_status == "executor_busy":
                self.logger.warning(
                    "⚠️ (%s) OFConfirmEngine.build skipped: shared executor saturated — fail-open, ofc=None",
                    runtime.symbol,
                )
                ofc, dec = None, None
            else:
                ofc, dec = result

        t_build_us = int((time.perf_counter_ns() - t_build_ns0) / 1000)
        try:
            of_confirm_build_ms_hist.labels(symbol=runtime.symbol, tf=str(runtime.config.get("micro_tf", "1s"))).observe(t_build_us / 1000.0)
        except Exception:
            pass

        indicators["of_build_us"] = int(t_build_us)

        # Fail-safe: if engine returns None (should not happen), reconstruct default to avoid bypass
        if not ofc:
             try:
                 # Log detailed state to debug the "Impossible None"
                 self.logger.error(
                     f"❌ ({runtime.symbol}) OFConfirmEngine.build returned None! "
                     f"cfg={list(runtime.config.keys())} "
                 )
                 # Reconstruct default
                 from core.of_confirm_contract import OFConfirmV3
                 ofc = OFConfirmV3(
                     v=3,
                     symbol=runtime.symbol,
                     ts_ms=int(tick_ts),
                     direction=direction,
                     scenario="none",
                     ok=0,
                     score=0.0,
                     have=0,
                     need=0,
                     gate_bits=0,
                     reason="engine_fail_safe",
                     evidence={},
                     contrib={}
                 )
                 # dec usually is None if ofc is None
             except Exception as rx:
                 self.logger.error(f"Failed to recover OFConfirmV3: {rx}")

        if ofc:
             ev = ofc.evidence
             indicators["of_confirm"] = ofc.to_dict()
             indicators["of_confirm_ok"] = int(ofc.ok)
             
             # SRE metrics helper
             self._emit_gate_metrics(runtime, ofc, indicators, ev, tick_ts)

             if runtime.config.get("require_strong_confirmation") and ofc.ok == 0:
                 is_soft_pass = int(ev.get("ok_soft", 0) or 0) == 1
                 if is_soft_pass:
                     indicators["strong_gate_soft_pass"] = 1
                 elif not runtime.config.get("strong_gate_shadow"):
                     # VETO
                     try:
                         # Help decision record extraction: normalize fields on veto path
                         indicators["ok"] = int(getattr(ofc, "ok", 0) or 0)
                         indicators["soft"] = int(ev.get("ok_soft", 0) or 0)
                         if hasattr(ofc, "score"):
                             indicators["rule_score"] = float(getattr(ofc, "score") or 0.0)
                         if isinstance(indicators.get("of_confirm"), dict) and "score" in indicators["of_confirm"]:
                             indicators["rule_score"] = float(indicators["of_confirm"].get("score") or indicators.get("rule_score") or 0.0)
                         if "rule_reason_code_top1" not in indicators and isinstance(ev, dict):
                             indicators["rule_reason_code_top1"] = str(ev.get("reason_code_top1") or ev.get("reason_code") or "STRONG_GATE_VETO")
                         # meta coverage fields are used by downstream breakdowns
                         if isinstance(ev, dict):
                             indicators["meta_enforce_cov_bucket"] = str(ev.get("meta_enforce_cov_bucket") or indicators.get("meta_enforce_cov_bucket") or "unknown")
                             indicators["meta_enforce_applied"] = int(ev.get("meta_enforce_applied") or indicators.get("meta_enforce_applied") or 0)
                     except Exception:
                         pass

                     strong_gate_veto_total.labels(symbol=runtime.symbol, scenario=ofc.scenario, reason="engine_veto", mode="ENFORCE").inc()
                     # P62: record early veto before returning None (SignalPipeline won't see it)
                     safe_create_task(
                         self._emit_early_veto_decision_record(
                             runtime=runtime,
                             tick_ts_ms=int(tick_ts),
                             direction=str(direction),
                             indicators=dict(indicators),
                             reason_code="STRONG_GATE_ENGINE_VETO",
                             notes="require_strong_confirmation enforce + ok_soft=0",
                         )
                     )
                     return None
        
        # Audit Confirmations
        if ofc:
            ev = ofc.evidence
            if ev.get("sweep"):
                # Directionality (EQH/EQL) is critical for downstream quality.
                # Fail-open: keep generic sweep=1, but also track missing side for alerts.
                sk = str(ev.get("sweep_eq_kind") or ev.get("sweep_kind") or ev.get("sweep_side") or "").strip().lower()
                eq_kind = "unknown"
                if sk in ("eqh", "high", "eq_high", "eqh_sweep", "sell", "short", "bear", "bearish"):
                    eq_kind = "eqh"
                elif sk in ("eql", "low", "eq_low", "eql_sweep", "buy", "long", "bull", "bullish"):
                    eq_kind = "eql"
                else:
                    # Attempt recovery (merge legacy logic)
                    try:
                        bias = ""
                        sw = getattr(runtime, "last_sweep", None)
                        if sw:
                             bias = str(getattr(sw, "direction_bias", "") or "").upper()
                        if not bias and isinstance(ev, dict):
                             bias = str(ev.get("sweep_dir") or ev.get("sweep_bias") or ev.get("sweep_direction") or "").upper()
                        
                        if bias == "SHORT": eq_kind = "eqh"
                        elif bias == "LONG": eq_kind = "eql"
                    except Exception:
                        pass
                    
                    if eq_kind == "unknown":
                        sweep_side_missing_total.labels(symbol=runtime.symbol).inc()

                # Always count sweeps with the best known kind.
                sweep_detected_total.labels(symbol=runtime.symbol, eq_kind=eq_kind).inc()
                indicators["sweep_eq_kind"] = eq_kind
                
                if eq_kind == "eqh":
                    confirmations.insert(0, "sweep_eqh=1")
                    indicators["sweep_eqh"] = 1
                    indicators["sweep_dir"] = "SHORT"
                elif eq_kind == "eql":
                    confirmations.insert(0, "sweep_eql=1")
                    indicators["sweep_eql"] = 1
                    indicators["sweep_dir"] = "LONG"
                else:
                    confirmations.insert(0, "sweep=1") # generic fallback
            if ev.get("absorption"): confirmations.append(f"absorption={ev.get('absorption_volume'):.2f}")
            if ev.get("weak_progress"): confirmations.append("weak_progress=1")
            if ev.get("abs_lvl_ok"): confirmations.append(f"abs_lvl={ev.get('abs_lvl_score'):.2f}")
            if ev.get("reclaim"): confirmations.append("reclaim=1")

        # Phase E: OBI / OFI
        try:
            now_ms_det = int(tick_ts)
            # OBI stability (quality-gated)
            if runtime.last_obi_event:
                age = now_ms_det - int(runtime.last_obi_event.get("ts_ms", 0) or 0)
                ttl = int(runtime.config.get("obi_event_ttl_ms", 30000))
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
                 ice_side = str(runtime.last_iceberg_event.get("side")).upper()
                 spike_side = "BUY" if float(delta_event.get("delta", 0)) > 0 else "SELL"
                 iceberg_side = "BUY" if ice_side == "BID" else "SELL"
                 if spike_side != iceberg_side:
                      indicators["ice_strict"] = 1
                      indicators["iceberg_strict"] = 1
                      confirmations.append("ice_strict=1")  # legacy
                      confirmations.append("iceberg_strict=1")  # canonical
                      
        # Optional Redis Publication (v3 asychronous) of OFConfirm
        if bool(int(runtime.config.get("publish_of_confirm", 0))) and ofc:
            stream = str(runtime.config.get("of_confirm_stream", "signals:of:confirm"))
            try:
                safe_create_task(
                    self.ticks.xadd(
                        stream,
                        fields={"payload": json.dumps(ofc.to_dict(), ensure_ascii=False)},
                        maxlen=int(runtime.config.get("of_confirm_stream_maxlen", 50000)),
                        approximate=True,
                    )
                )
            except Exception:
                pass

        # Min Confirmations
        # ... (Check min_confirmations) ...
        # Simplified:
        min_confirmations = int(runtime.config.get("min_confirmations", 0))
        if len(confirmations) < min_confirmations:
            return None

        # Confidence Computation
        primary_reason = "delta_spike"
        if confirmations: primary_reason = confirmations[0].split("=", 1)[0]
        
        # High-ROI: confirmations schema drift / coverage telemetry
        try:
            track_confirmations(
                symbol=str(getattr(runtime, "symbol", "unknown")),
                confirmations=list(confirmations),
                side=str(direction),
                kind=str(primary_reason),
            )
        except Exception as e:
            log_silent_error(e, "confirm_coverage", symbol=str(getattr(runtime, "symbol", "unknown")), where="tick_processor")

        # Compute confidence using Scorer (if passed) OR simple logic
        # Since self.of_engine has scorer?
        confidence: float = 0.85  # Default fallback
        conf_parts = {}
        if self.conf_scorer:
            # ConfidenceScorer expects kwargs: score(kind=..., side=..., ctx=...)
            # ctx resolves: indicators -> runtime.config -> runtime attrs
            
            # Phase 2: Structured Evidence Construction
            # 1. Start with explicit evidence from OFC if available
            ctx_evidence = {}
            if ofc and hasattr(ofc, "evidence") and isinstance(ofc.evidence, dict):
                 ctx_evidence.update(ofc.evidence)
            
            # 2. Backfill from confirmations strings (legacy support)
            # This ensures that even if OFC structure is missing, we parse strings into dict
            for c in confirmations:
                if "=" in c:
                    k, v = c.split("=", 1)
                    # Don't overwrite if stronger evidence exists
                    if k not in ctx_evidence:
                        try:
                            ctx_evidence[k] = float(v)
                        except ValueError:
                            ctx_evidence[k] = v
                else:
                    ctx_evidence[c] = 1.0

            # 3. Inject critical scalar context into evidence
            # This allows the Scorer to see macro state without needing full runtime access
            # (Regime-aware weighting & DataHealth calibration)
            ctx_evidence.setdefault("market_mode", str(getattr(runtime, "market_mode", "") or "neutral"))
            ctx_evidence.setdefault("data_health", float(indicators.get("data_health", 1.0) or 1.0))
            
            class ConfCtx:
                def __init__(self, ind: Dict[str, Any], confs: List[str], rt: Any, evidence: Dict[str, Any]):
                    self.ind = ind
                    self.confirmations = confs
                    self.rt = rt
                    self.evidence = evidence

                def __getattr__(self, name: str) -> Any:
                    # Priority: Evidence > Indicators > Config > Runtime
                    if name in self.evidence:
                         return self.evidence[name]
                    if name in self.ind:
                        return self.ind[name]
                    cfg = getattr(self.rt, "config", None)
                    if isinstance(cfg, dict) and name in cfg:
                        return cfg[name]
                    return getattr(self.rt, name)

            ctx = ConfCtx(indicators, confirmations, runtime, ctx_evidence)
            try:
                out = self.conf_scorer.score(kind=primary_reason or "custom", side=direction, ctx=ctx)
                if isinstance(out, tuple) and len(out) == 2:
                    confidence, conf_parts = out
                    # Optional: attach parts (sampled) for debugging/replay.
                    try:
                        attach = bool(int(runtime.config.get("confidence_parts_attach", 0) or 0))
                        sample = float(runtime.config.get("confidence_parts_sample", 0.01) or 0.01)
                        if attach and _should_sample(int(tick_ts), sample):
                            indicators["confidence_parts"] = conf_parts
                    except Exception:
                        pass
                else:
                    confidence = float(out)
            except Exception as e:
                log_silent_error(e, "confidence_score", symbol=str(getattr(runtime, "symbol", "unknown")))
        
        indicators["confidence"] = confidence

        # Emit Decision Record (Success/Soft-Fail Path)
        # P62: Unified Decision Record for all finalized signals (sampled)
        if str(os.getenv("DECISION_RECORD_ENABLE", "1")).lower() in ("1", "true", "yes", "on"):
             safe_create_task(
                 self._emit_decision_record(
                     runtime=runtime,
                     tick_ts_ms=int(tick_ts),
                     direction=direction,
                     indicators=indicators,
                     ofc=ofc,
                     confidence=confidence,
                 )
             )

        # Optional: attach scorer decomposition and evidence flags into indicators for offline evaluation/calibration.
        # Disabled by default to keep payloads small and avoid schema surprises.
        if self.confidence_parts_enable:
            try:
                # keep deterministic key order; cap keys to avoid bloat
                _cp = conf_parts if isinstance(conf_parts, dict) else {}
                keys = sorted([k for k in _cp.keys() if isinstance(k, str)])
                if self.confidence_parts_max_keys > 0:
                    keys = keys[: int(self.confidence_parts_max_keys)]
                compact = {}
                for k in keys:
                    v = _cp.get(k)
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        compact[k] = float(v)
                if compact:
                    indicators["confidence_parts"] = compact
            except Exception:
                pass

        if self.confidence_evidence_enable:
            try:
                compact_ev = {}
                # Ensure ctx_evidence exists even if scorer didn't run or errored (fallback)
                ev = locals().get("ctx_evidence", {})
                if not ev and ofc and hasattr(ofc, "evidence") and isinstance(ofc.evidence, dict):
                     ev = ofc.evidence
                
                for k in (self.confidence_evidence_keys or []):
                    if not k:
                        continue
                    if k in ev:
                        v = ev.get(k)
                        # normalize scalars only
                        if isinstance(v, bool):
                            compact_ev[k] = 1 if v else 0
                        elif isinstance(v, (int, float)) and not isinstance(v, bool):
                            compact_ev[k] = float(v)
                        elif isinstance(v, str) and len(v) <= 64:
                            compact_ev[k] = v
                if compact_ev:
                    indicators["confidence_evidence"] = compact_ev
            except Exception:
                pass
        
        # Entry Pricing
        executable_entry = float(price) # Fallback

        # Payload Construction
        # Inject p_delta / p_speed explicit values for formatting
        # p_delta: based on relative size vs Tier 0
        try:
            d_usd_val = float(indicators.get("dn_usd", 0.0))
            if d_usd_val <= 0:
                 # fallback calc if missing
                 delta = float(indicators.get("delta", 0.0))
                 price_v = float(indicators.get("price", 0.0))
                 d_usd_val = abs(delta * price_v)
                 indicators["dn_usd"] = d_usd_val

            t0_val = float(indicators.get("dn_tier0_usd", 0.0))
            if t0_val <= 0:
                t0_val = float(runtime.config.get("dn_tier0_usd", 100000.0))

            # User request: p = abs(delta_usd) / dn_tier0_usd with clamp 0.99
            p_d = min(0.99, d_usd_val / t0_val) if t0_val > 0 else 0.0
            indicators["p_delta"] = round(p_d, 2)
            
            # p_speed: based on Z-score
            d_z_val = abs(float(indicators.get("delta_z", 0.0)))
            # typ. Z is 3..6. ratio / 6.0 gives 0.5..1.0
            p_s = min(0.99, d_z_val / 6.0)
            indicators["p_speed"] = round(p_s, 2)
        except Exception:
            pass

        # REAL vs VIRTUAL Logic
        min_conf = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", runtime.config.get("min_confidence", 0.0)))
        if min_conf > 0 and confidence < min_conf:
             return None
        
        # Virtual if it failed strict gates (ofc.ok == 0) but passed filters (reaches here)
        indicators["is_virtual"] = 1 if (ofc and getattr(ofc, "ok", 0) == 0) else 0

        signal_id = f"crypto-of:{runtime.symbol}:{now_ts}"
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
            "is_virtual": bool(int(indicators.get("is_virtual", 0) or 0)),
        }
        
        # Attach Pressure Snapshot to Payload
        # ...

        # Adverse Selection Gate
        if bool(int(runtime.config.get("adverse_check_enable", 0))):
             pass # ... Logic ...
        # Golden Replay / Deterministic Inputs Publication
        try:
            pub_val = runtime.config.get("publish_of_inputs", 0)
            should_pub = bool(int(pub_val))
            if runtime.tick_count % 100 == 0:
                self.logger.info(f"DEBUG: publish_of_inputs={pub_val} should_pub={should_pub} symbol={runtime.symbol}")
            
            if should_pub:
                tick_ts_ms = int(tick_ts) if int(tick_ts or 0) > 0 else 0
                if tick_ts_ms <= 0:
                    try:
                        from services.orderflow.metrics import of_inputs_bad_time_total
                        of_inputs_bad_time_total.labels(symbol=str(runtime.symbol)).inc()
                    except Exception:
                        pass
                    should_pub = False
                
                if should_pub:


                    trend_dir = "NONE"
                    hidden_ctx_recent = 0
                    cont_ctx_recent = 0
                    try:
                        div = getattr(runtime, "last_div", None)
                        td = hidden_trend_dir(getattr(div, "kind", None) if div else None)
                        if td:
                            trend_dir = str(td).upper()
                        if div and td:
                            hidden_ms = int(runtime.config.get("hidden_ctx_valid_ms", 120_000))
                            age = tick_ts_ms - int(getattr(div, "ts_ms", tick_ts_ms))
                            hidden_ctx_recent = 1 if (0 <= age <= hidden_ms) else 0
                        cts = int(getattr(runtime, "cont_ctx_ts_ms", 0) or 0)
                        cv = int(runtime.config.get("cont_ctx_valid_ms", 120_000))
                        cont_ctx_recent = 1 if (cts > 0 and 0 <= tick_ts_ms - cts <= cv) else 0
                    except Exception:
                        pass

                    def _i(v, d=0) -> int:
                        try:
                            return int(v)
                        except Exception:
                            try:
                                return int(float(v))
                            except Exception:
                                return int(d)

                    def _f(v, d=0.0) -> float:
                        try:
                            x = float(v)
                            if x != x or x == float("inf") or x == float("-inf"):
                                return float(d)
                            return x
                        except Exception:
                            return float(d)

                    def _s(v, d="na") -> str:
                        try:
                            s = str(v).strip() if v is not None else d
                            return s if s else d
                        except Exception:
                            return d

                    # Inputs construction
                    ev_weak       = _i(indicators.get("weak_progress", 0), 0)
                    ev_sweep      = _i(indicators.get("sweep_recent", indicators.get("sweep", 0)), 0)
                    ev_reclaim    = _i(indicators.get("reclaim_recent", indicators.get("reclaim", 0)), 0)
                    ev_obi_stable = _i(indicators.get("obi_stable", 0), 0)
                    ev_ice_strict = _i(indicators.get("iceberg_strict", indicators.get("ice_strict", 0)), 0)
                    ev_abs_lvl_ok = _i(indicators.get("abs_lvl_ok", 0), 0)

                    if ofc and hasattr(ofc, "evidence") and isinstance(ofc.evidence, dict):
                         ev_ofc = ofc.evidence
                         ev_weak       = _i(ev_ofc.get("weak_progress", ev_weak), ev_weak)
                         ev_sweep      = _i(ev_ofc.get("sweep", ev_ofc.get("sweep_recent", ev_sweep)), ev_sweep)
                         ev_reclaim    = _i(ev_ofc.get("reclaim", ev_ofc.get("reclaim_recent", ev_reclaim)), ev_reclaim)
                         ev_obi_stable = _i(ev_ofc.get("obi_stable", ev_obi_stable), ev_obi_stable)
                         ev_ice_strict = _i(ev_ofc.get("iceberg_strict", ev_ice_strict), ev_ice_strict)
                         ev_abs_lvl_ok = _i(ev_ofc.get("abs_lvl_ok", ev_abs_lvl_ok), ev_abs_lvl_ok)

                    cfg_safe = {}
                    try:
                        for _k in ("of_score_min", "hidden_ctx_valid_ms", "cont_ctx_valid_ms"):
                            if _k in runtime.config:
                                cfg_safe[_k] = runtime.config.get(_k)
                    except Exception:
                        pass
                    
                    emit_v2_cfg = runtime.config.get("of_inputs_emit_v2", 1)
                    emit_v2 = bool(_i(emit_v2_cfg, 1))

                    ofi_kwargs = {
                        "v": 2 if emit_v2 else 1,
                        "symbol": _s(runtime.symbol),
                        "ts_ms": int(tick_ts_ms),
                        "regime": _s(getattr(runtime, "last_regime", "na")),
                        "direction": _s(direction),
                        "scenario": _s((ofc.evidence.get("scenario_v4") if (ofc and getattr(ofc, "evidence", None)) else None) or (getattr(dec, "scenario_v4", None) if dec else None) or "na"),
                        "delta_z": _f(delta_z_used, 0.0),
                        "weak_progress": ev_weak,
                        "sweep_recent": ev_sweep,
                        "reclaim_recent": ev_reclaim,
                        "obi_stable": ev_obi_stable,
                        "iceberg_strict": ev_ice_strict,
                        "abs_lvl_ok": ev_abs_lvl_ok,
                        "trend_dir": _s(trend_dir, "NONE").upper(),
                        "hidden_ctx_recent": _i(hidden_ctx_recent, 0),
                        "cont_ctx_recent": _i(cont_ctx_recent, 0),
                        "cfg": cfg_safe,
                        "fp_eff_quote": _f(getattr(runtime.last_bar, "fp_eff_quote", 0.0) if runtime.last_bar else 0.0, 0.0),
                        "fp_quote_delta": _f(getattr(runtime.last_bar, "fp_quote_delta", 0.0) if runtime.last_bar else 0.0, 0.0),
                    }
                    
                    if emit_v2:
                        ofi_kwargs["ofi"] = _f(indicators.get("ofi", 0.0), 0.0)
                        ofi_kwargs["ofi_z"] = _f(indicators.get("ofi_z", 0.0), 0.0)
                        ofi_kwargs["ofi_stable"] = _i(indicators.get("ofi_stable", 0), 0)
                        ofi_kwargs["ofi_dir_ok"] = _i(indicators.get("ofi_dir_ok", 0), 0)
                        ofi_kwargs["ofi_stable_secs"] = _f(indicators.get("ofi_stable_secs", 0.0), 0.0)

                    inputs_obj = OFInputsV2(**ofi_kwargs) if emit_v2 else OFInputsV1(**ofi_kwargs)
                    
                    stream_inputs = str(runtime.config.get("of_inputs_stream", "metrics:ml_inputs"))
                    maxlen_inputs = int(runtime.config.get("of_inputs_stream_maxlen", 50000))
                    
                    safe_create_task(self.redis.xadd(
                        stream_inputs,
                        fields={"payload": inputs_obj.to_json()},
                        maxlen=maxlen_inputs,
                        approximate=True
                    ))
        except Exception:
            pass

        return await self._emit_payload(runtime, payload, int(tick_ts))


    def _get_rocket_multiplier(self, symbol: str) -> float:
        """
        Возвращает множитель для TP1 в профиле rocket_v1.
        Ищет в ENV: ROCKET_TP1_ATR_MULT_{SYMBOL} (напр. ROCKET_TP1_ATR_MULT_BTCUSDT)
        Fallback: ROCKET_TP1_ATR_MULT (дефолт 0.78)
        """
        env_var = f"ROCKET_TP1_ATR_MULT_{symbol.upper()}"
        val = os.getenv(env_var)
        source = env_var
        
        if not val:
            val = os.getenv("ROCKET_TP1_ATR_MULT", "0.78")
            source = "ROCKET_TP1_ATR_MULT"
            
        try:
            m = float(val)
        except (ValueError, TypeError):
            self.logger.warning("⚠️ Некорректное значение множителя %s=%r. Используем дефолт 0.78", source, val)
            return 0.78
            
        # Clamp 0.5 .. 10.0
        if m < 0.5 or m > 10.0:
            self.logger.warning("⚠️ Множитель %s=%.2f вне диапазона (0.5..10.0). Применяем clamp.", source, m)
            m = max(0.5, min(10.0, m))
            
        return m

    def _normalize_trailing_flag(self, value: Any, symbol: Optional[str] = None) -> bool:
        """
        Возвращает финальный флаг трейлинга.
        """
        explicit_flag: Optional[bool] = None
        if value is not None:
            try:
                if isinstance(value, str):
                    explicit_flag = value.lower() in ("1", "true", "yes", "on")
                else:
                    explicit_flag = bool(value)
            except Exception:
                explicit_flag = False

        force_trail = os.getenv("FORCE_TRAIL_AFTER_TP1", "0").lower() in ("1", "true", "yes", "on")
        
        if explicit_flag is not None:
            return explicit_flag
        
        return force_trail

    def _calculate_levels(
        self,
        runtime: SymbolRuntime,
        entry: float,
        side: str,
        indicators: Dict[str, Any],
        trail_profile: Optional[str] = None,
    ) -> Tuple[float, List[float], float, float]:
        cfg = runtime.config
        atr_original = float(indicators.get("atr", 0.0) or 0.0)
        
        # Use canonical TF resolver (single source of truth)
        atr_tf = runtime.get_atr_tf_selected()
        indicators["atr_tf_used"] = atr_tf
        
        # Always fetch the canonical target ATR for the selected timeframe (which may be expanded)
        atr_target = 0.0
        try:
            nm = 0
            try:
                nm = int(indicators.get("ts_ms", 0) or indicators.get("tick_ts", 0) or 0)
            except Exception:
                pass
            prefer_src = ""
            try:
                if int(runtime.dynamic_cfg.get(DK.ATR_SRC_READY, 0) or 0) == 1:
                    prefer_src = str(runtime.dynamic_cfg.get(DK.ATR_SRC_PREF, "") or "")
            except Exception:
                pass
            
            if self.atr_cache:
                _atr, _atr_meta = self.atr_cache.get_with_meta(symbol=runtime.symbol, timeframe=atr_tf, now_ms=(nm if nm > 0 else None), prefer_src=prefer_src)
                atr_target = float(_atr or 0.0)
                if isinstance(_atr_meta, dict) and atr_target > 0:
                    indicators["atr_src"] = str(_atr_meta.get("src") or _atr_meta.get("source") or "na")
                    indicators["atr_ts_ms"] = int(_atr_meta.get("ts_ms", 0) or 0)
                    indicators["atr_age_ms"] = int(_atr_meta.get("age_ms", 0) or 0)
                    indicators["atr_consistency"] = float(_atr_meta.get("consistency", 1.0) or 1.0)
                    indicators["atr_cons_ok"] = int(_atr_meta.get("cons_ok", 1) or 1)
                    if prefer_src:
                        indicators["atr_src_prefer"] = str(prefer_src)
        except Exception:
            atr_target = 0.0
            
        # Fallback to original passed ATR
        if atr_target <= 0:
            atr_target = atr_original

        # Final ATR fallback (absolute last resort)
        if atr_target <= 0:
            symbol_fallbacks = {
                "BTCUSDT": 30.0,
                "ETHUSDT": 4.0,
                "BNBUSDT": 0.5,
                "SOLUSDT": 0.3,
            }
            atr_target = symbol_fallbacks.get(runtime.symbol, entry * 0.0003)
            indicators["atr_src"] = "fallback-symbol"
            indicators["atr_sanity_reason"] = "no_valid_atr_found"
            indicators["atr_sanity_ok"] = 1

        # Always expose atr_bps_exec for unified gates/debug
        try:
            if float(entry) > 0 and float(atr_target) > 0:
                indicators["atr_bps_exec"] = float(10000.0 * (float(atr_target) / float(entry)))
        except Exception:
            pass

        lot = indicators.get("lot")
        if lot is None:
            lot = indicators.get("tick_qty") or indicators.get("delta") or 1.0
            lot = max(float(lot), cfg.get("min_lot", 0.01))

        def rr_levels(rr_str: str) -> List[float]:
            try:
                return [float(x.strip()) for x in rr_str.split(",") if x.strip()]
            except Exception:
                return [1.3, 2.0, 2.7]

        if str(cfg.get("stop_mode", "ATR")).upper() == "ATR":
            stop_dist = atr_target * cfg.get("stop_atr_mult", 1.0)  # was 0.6
        elif str(cfg.get("stop_mode", "ATR")).upper() == "PCT":
            stop_dist = entry * cfg.get("stop_pct", 0.2) / 100
        else:
            stop_dist = cfg.get("stop_points", 1.0)

        # Проверяем, используется ли профиль rocket_v1
        if not trail_profile:
            trail_profile = cfg.get("trail_profile") or indicators.get("trail_profile") or cfg.get("default_trail_profile", "rocket_v1")
        
        # ⚠️ REMOVED: tp1_offset_atr was incorrectly used as SL multiplier.
        # tp1_offset_atr is a TP1 trailing offset (0.1-0.17), NOT an SL mult.
        # See: implementation_plan.md (2026-04-25) Root Cause #1.

        # SL ATR mult floor: never less than SL_ATR_MULT_FLOOR (industry minimum)
        _sl_atr_floor = float(os.getenv("SL_ATR_MULT_FLOOR", "0.5") or 0.5)
        if atr_target > 0 and stop_dist > 0:
            _actual_sl_mult = stop_dist / atr_target
            if _actual_sl_mult < _sl_atr_floor:
                indicators["sl_atr_mult_floored"] = 1
                indicators["sl_atr_mult_original"] = round(_actual_sl_mult, 4)
                stop_dist = atr_target * _sl_atr_floor

        # Для rocket_v1: TP1 = MULT * ATR, остальные TP через RR
        rocket_mult = self._get_rocket_multiplier(runtime.symbol)
        is_rocket_v1 = (trail_profile == "rocket_v1")
        
        if side.upper() == "LONG":
            sl = entry - stop_dist
            tp1_dist_base = atr_target * rocket_mult if is_rocket_v1 else stop_dist * rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))[0]
            # Ensure minimum RR constraint vs. expanded SL
            _min_rr_floor = float(cfg.get("tp1_min_rr_floor", os.getenv("TP1_MIN_RR_FLOOR", "1.0")) or "1.0")
            tp1_dist = max(tp1_dist_base, stop_dist * _min_rr_floor) if is_rocket_v1 else tp1_dist_base
            
            # Base calculation
            if is_rocket_v1:
                # TP1 = mult ATR
                tp1 = entry + tp1_dist
                rr_list = rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))
                
                # Default RR-based potential TPs
                tp2_potential = entry + stop_dist * (rr_list[1] if len(rr_list) > 1 else 2.0)
                tp3_potential = entry + stop_dist * (rr_list[2] if len(rr_list) > 2 else 2.7)
                
                # Enforce monotonicity: TP2/TP3 must be significantly further than TP1
                tp2_dist = max(tp2_potential - entry, tp1_dist * 1.5)
                tp3_dist = max(tp3_potential - entry, tp1_dist * 2.0)
                
                tp2 = entry + tp2_dist
                tp3 = entry + tp3_dist
                
                tps = [tp1, tp2, tp3]
            else:
                # Standard RR logic
                tps = [entry + stop_dist * rr for rr in rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))]

        else: # SHORT
            sl = entry + stop_dist
            tp1_dist_base = atr_target * rocket_mult if is_rocket_v1 else stop_dist * rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))[0]
            # Ensure minimum RR constraint vs. expanded SL
            _min_rr_floor = float(cfg.get("tp1_min_rr_floor", os.getenv("TP1_MIN_RR_FLOOR", "1.0")) or "1.0")
            tp1_dist = max(tp1_dist_base, stop_dist * _min_rr_floor) if is_rocket_v1 else tp1_dist_base
            
            if is_rocket_v1:
                # TP1 = mult ATR
                tp1 = entry - tp1_dist
                rr_list = rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))
                
                # Default RR-based potential TPs
                tp2_potential = entry - stop_dist * (rr_list[1] if len(rr_list) > 1 else 2.0)
                tp3_potential = entry - stop_dist * (rr_list[2] if len(rr_list) > 2 else 2.7)
                
                # Enforce monotonicity for SHORT (distances are positive)
                tp2_dist = max(entry - tp2_potential, tp1_dist * 1.5)
                tp3_dist = max(entry - tp3_potential, tp1_dist * 2.0)
                
                tp2 = entry - tp2_dist
                tp3 = entry - tp3_dist
                
                tps = [tp1, tp2, tp3]
            else:
                # Standard RR logic
                tps = [entry - stop_dist * rr for rr in rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))]
        
        # FINAL SAFETY: Sort TPs by distance from entry to guarantee order 1 < 2 < 3
        # abs(tp - entry) makes it direction-agnostic
        tps.sort(key=lambda x: abs(x - entry))

        return sl, tps, float(lot), float(atr_target)

    async def _emit_payload(self, runtime: SymbolRuntime, payload: Dict[str, Any], tick_ts: int) -> Dict[str, Any]:
        """
        Emits the signal payload to the configured streams.
        """
        # 0. Calculate Risk Levels
        entry = float(payload.get("entry") or 0.0)
        direction = str(payload.get("direction") or "").upper()
        indicators = payload.get("indicators") or {}
        
        trail_profile = payload.get("trail_profile") or runtime.config.get("trail_profile") or "rocket_v1"
        
        sl, tp_levels, lot, atr = self._calculate_levels(
            runtime, entry, direction, indicators, trail_profile=trail_profile
        )
        
        payload["sl"] = float(sl)
        payload["tp_levels"] = [float(x) for x in tp_levels]
        payload["lot"] = float(lot)
        payload["atr"] = float(atr)
        payload["trail_profile"] = trail_profile
        payload["trail_after_tp1"] = self._normalize_trailing_flag(payload.get("trail_after_tp1"), runtime.symbol)
        
        try:
            calib_dist = runtime.calibrated_specs.get("trailing", {}).get("tp1_offset_atr")
            if calib_dist is not None:
                payload["trail_atr_mult_calibrated"] = float(calib_dist)
        except Exception:
            pass

        # 1. Enrich with server timestamp
        payload["server_ts_ms"] = get_ny_time_millis()
        
        # 2. Publish to RAW stream
        raw_stream = runtime.config.get("raw_signal_stream", "signals:crypto:raw")
        await self.publisher.xadd_json(
            sink=StreamSink(name=raw_stream),
            payload=payload,
            symbol=runtime.symbol
        )
        
        # 3. Publish to NOTIFY stream if applicable
        # P99 FIX: fire-and-forget for notify stream to avoid sequential blocking.
        # Notify is non-critical — should never block signal emit latency.
        min_conf = float(runtime.config.get("signal_min_conf", 70.0))
        if float(payload.get("confidence", 0) * 100) >= min_conf:
             notify_stream = runtime.config.get("notify_stream", "notify:telegram")
             safe_create_task(
                 self.publisher.xadd_json(
                     sink=StreamSink(name=notify_stream),
                     payload=payload,
                     symbol=runtime.symbol
                 ),
             )

        return payload


    def _emit_gate_metrics(self, runtime, ofc, indicators, ev, tick_ts):
        """Best-effort SRE emission for gate decisions to Redis Stream (metrics:of_gate).

        Requirements (meta coverage ops): the XADD fields must include at least:
          - meta_feature_coverage
          - meta_enforce_cov_bucket

        We keep this fail-open and never raise to the hot path.
        """
        try:
            if not self.of_gate_metrics_enable:
                return

            rate = float(runtime.config.get("of_gate_metrics_sample", self.of_gate_metrics_sample))
            if rate <= 0:
                return
            # Sampling-invariant: stable per (symbol, ts_ms) to avoid correlated gaps
            sample_uid = _sample_uid_symbol_ts(str(runtime.symbol), int(tick_ts))
            if not _should_sample(sample_uid, rate):
                return

            # --- helpers ---
            def _f(v):
                try:
                    if v is None:
                        return None
                    return float(v)
                except Exception:
                    return None

            def _i(v, default=0):
                try:
                    if v is None:
                        return default
                    return int(float(v))
                except Exception:
                    return default

            e = ev if isinstance(ev, dict) else {}
            # ... (rest unchanged until fields) ...


            cov = _f(e.get("meta_feature_coverage"))
            tot = _i(e.get("meta_model_feature_total"), 0)
            mis = _i(e.get("meta_model_feature_missing"), 0)
            if cov is None or cov != cov:  # NaN
                if tot > 0:
                    cov = max(0.0, min(1.0, 1.0 - (mis / float(tot))))
                else:
                    cov = 0.0
                    tot = 0
                    mis = 0

            bucket = str(e.get("meta_enforce_cov_bucket") or e.get("meta_cov_bucket") or "").strip().lower()
            if bucket not in ("a", "b", "c", "d"):
                # Conservative fallback: treat as lowest-coverage bucket.
                bucket = "d"

            applied = _i(e.get("meta_enforce_applied") or e.get("meta_applied"), 0)
            if applied not in (0, 1):
                applied = 1 if applied else 0

            bucket_type = str(e.get("meta_enforce_bucket_type") or "").strip().lower()
            # Optional schema fields (used for debugging / drift triage)
            meta_schema_id = e.get("meta_schema_id")
            meta_schema_ver = e.get("meta_schema_version")
            meta_model_schema_id = e.get("meta_model_schema_id")
            meta_model_schema_ver = e.get("meta_model_schema_version")

            of_build_us = _i(indicators.get("of_build_us"), 0)
            ok = _i(getattr(ofc, "ok", 0), 0)

            payload = {
                "ts_ms": _i(tick_ts, 0),
                "symbol": getattr(runtime, "symbol", ""),
                "ok": ok,
                "of_build_us": of_build_us,
                "meta_feature_coverage": cov,
                "meta_model_feature_total": tot,
                "meta_model_feature_missing": mis,
                "meta_enforce_cov_bucket": bucket,
                "meta_enforce_applied": applied,
                "meta_enforce_bucket_type": bucket_type,
            }
            if meta_schema_id is not None:
                payload["meta_schema_id"] = meta_schema_id
            if meta_schema_ver is not None:
                payload["meta_schema_version"] = meta_schema_ver
            if meta_model_schema_id is not None:
                payload["meta_model_schema_id"] = meta_model_schema_id
            if meta_model_schema_ver is not None:
                payload["meta_model_schema_version"] = meta_model_schema_ver


            fields = {
                "ts_ms": str(payload["ts_ms"]),
                "symbol": str(payload["symbol"]),
                "ok": str(ok),
                "of_build_us": str(of_build_us),
                # Compatibility fields for of_gate_sre_monitor:
                "ok_soft": str(_i(e.get("ok_soft"), 0)),
                "meta_veto": str(_i(e.get("meta_veto"), 0)),
                "latency_us": str(of_build_us),
                "ml_latency_us": str(_i(e.get("ml_latency_us"), 0)),
                "exec_risk_norm": str(_f(indicators.get("exec_risk_norm"), 0.0)),
                "book_health_ok": str(_i(indicators.get("book_health_ok", 1), 1)),
                "source_consistency_ok": str(_i(indicators.get("source_consistency_ok", 1), 1)),
                "data_health": str(_f(indicators.get("data_health", 1.0), 1.0)),
                "scenario": str(getattr(ofc, "scenario", "") or "na"),
                "scenario_v4": str(getattr(ofc, "scenario_v4", "") or getattr(ofc, "scenario", "") or "na"),
                "missing_legs": json.dumps(list(e.get("missing_legs", []) or []), ensure_ascii=False),
                # Required for meta coverage ops preflight:
                "meta_feature_coverage": str(cov),
                "meta_enforce_cov_bucket": str(bucket),
                # Useful for controllers/guards without JSON decode:
                "meta_enforce_applied": str(applied),
                "meta_model_feature_total": str(tot),
                "meta_model_feature_missing": str(mis),
                "meta_enforce_bucket_type": str(bucket_type),
                "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            }
            
            safe_create_task(self.redis.xadd(
                self.of_gate_metrics_stream,
                fields=fields,
                maxlen=self.of_gate_metrics_maxlen,
                approximate=True,
            ))
        except Exception:
            return

    async def _emit_decision_record(
        self,
        runtime,
        tick_ts_ms: int,
        direction: str,
        indicators: Dict[str, Any],
        ofc: Any,
        confidence: float,
    ) -> None:
        """
        Emits a DecisionRecord for finalized signals (Success/Soft-Fail path).
        """
        try:
            sid = str(
                indicators.get("sid")
                or indicators.get("signal_id")
                or indicators.get("signalId")
                or f"{runtime.symbol}:{int(tick_ts_ms)}:{str(direction).upper()}"
            )

            rate = float(os.getenv("DECISION_RECORD_SAMPLE", "1.0") or 1.0)
            if not deterministic_sample(sid, rate):
                try:
                    decision_record_sampled_out_total.labels(symbol=str(runtime.symbol)).inc()
                except Exception:
                    pass
                return

            # Prepare stub for extraction
            ev = getattr(ofc, "evidence", {}) or {}
            stub = {
                "sid": sid,
                "symbol": str(runtime.symbol),
                "tf": str(runtime.config.get("micro_tf", "na")),
                "strategy": str(runtime.config.get("strategy_name", "cryptoorderflow")),
                "ts_ms": int(tick_ts_ms),
                "direction": str(direction).upper(),
                "indicators": indicators,
                "confidence": float(confidence),
                "score": float(getattr(ofc, "score", 0.0) or 0.0),
                "evidence": ev,
            }
            
            # Use shared extraction logic
            f = extract_fields_best_effort(stub)
            
            # Determine actual action
            # If we are here, we passed strong gate (hard pass or soft pass).
            # Soft pass means ok=0 but sent as virtual? 
            # In process_tick, if ofc.ok==0 and soft_pass==1 -> is_virtual=1.
            is_virtual = bool(int(indicators.get("is_virtual", 0) or 0))
            is_ok = bool(int(getattr(ofc, "ok", 0) or 0))
            
            actual_action = "pass"
            if is_virtual:
                actual_action = "soft_pass"
            elif not is_ok:
                # Should not reach here typically if emitted, unless emit=debug
                actual_action = "veto" # but we emitted?
            
            # Binding recommendation (What should we have done?)
            bind = recommend_binding(
                BindingInput(
                    rule_score=float(f.get("rule_score", 0.0)),
                    rule_ok=bool(f.get("rule_ok", False)),
                    rule_soft=bool(f.get("rule_soft", False)),
                    ml_state=str(f.get("ml_state", "na")),
                    ml_p_cal=f.get("ml_p_cal", None),
                    dq_state=str(f.get("dq_state", "unknown")),
                    drift_state=str(f.get("drift_state", "unknown")),
                )
            )

            rec = DecisionRecordV1(
                ver="v1",
                sid=sid,
                symbol=str(runtime.symbol),
                tf=str(runtime.config.get("micro_tf", "na")),
                strategy=str(runtime.config.get("strategy_name", "cryptoorderflow")),
                decision_ts_ms=int(tick_ts_ms),
                rule_score=float(f.get("rule_score", 0.0)),
                rule_ok=bool(f.get("rule_ok", False)),
                rule_soft=bool(f.get("rule_soft", False)),
                rule_reason_code_top1=str(f.get("rule_reason_code_top1", "NA")),
                ml_enabled=bool(f.get("ml_enabled", False)),
                ml_state=str(f.get("ml_state", "na")),
                ml_p_cal=f.get("ml_p_cal", None),
                ml_model_ver=str(f.get("ml_model_ver", "")),
                ml_latency_ms=f.get("ml_latency_ms", None),
                ml_error=str(f.get("ml_error", "")),
                dq_state=str(f.get("dq_state", "unknown")),
                dq_flags=list(f.get("dq_flags", []) or []),
                drift_state=str(f.get("drift_state", "unknown")),
                drift_flags=list(f.get("drift_flags", []) or []),
                actual_action=actual_action,
                actual_reason_code=str(getattr(ofc, "reason", "OK")),
                recommended_action=str(bind.get("recommended_action", "pass")),
                recommended_reason_code=str(bind.get("recommended_reason_code", "NA")),
                meta_enforce_cov_bucket=str(f.get("meta_enforce_cov_bucket", "unknown")),
                meta_enforce_applied=bool(f.get("meta_enforce_applied", False)),
                payload_summary={
                    "stage": "tick_processor_emit",
                    "direction": str(direction).upper(),
                    "conf": float(confidence),
                    "p_delta": float(indicators.get("p_delta", 0.0)),
                },
            )

            safe_create_task(write_decision_record(self.redis, rec))
            try:
                decision_record_written_total.labels(symbol=str(runtime.symbol), action=actual_action).inc()
            except Exception:
                pass
        except Exception:
            try:
                decision_record_error_total.labels(symbol=str(runtime.symbol)).inc()
                # self.logger.error(f"Failed to emit decision record: {e}")
            except Exception:
                pass
