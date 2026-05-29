from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any

import redis

from core.bucket2_v1 import derive_bucket2_label
from core.champion_cfg_validator import validate_champion_cfg
from core.edge_stack_mh_v1 import EdgeStackMHModelV1  # type: ignore
from core.feature_engineering import (
    RobustScalerPack,
    apply_transform,
    bucketize,
    derive_regime_label,
    derive_session_label,
)
from core.meta_model_lr import MetaModelLR
from services.ml_calibration import PlattLogitCalibrator
from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS

# Prometheus metrics (optional, fail-open if not available)
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False
    # Mock metrics for when prometheus_client is not available
    class _MockMetric:  # type: ignore
        def labels(self, **kwargs):
            return self
        def inc(self, *args, **kwargs):
            pass
        def set(self, *args, **kwargs):
            pass
        def observe(self, *args, **kwargs):
            pass
    Counter = Gauge = Histogram = lambda *args, **kwargs: _MockMetric()

# Import centralized metrics from registry (fail-open if not available)
try:
    from services.observability.metrics_registry import (
        ml_confirm_cfg_present,
        ml_confirm_cfg_valid,
        ml_confirm_enforce_share,
        ml_confirm_errors_total,
        ml_confirm_events_total,
        ml_confirm_latency_seconds,
        ml_confirm_model_load_seconds,
        ml_confirm_model_loaded,
        ml_missing_critical_total,
    )
    METRICS_REGISTRY_AVAILABLE = True
except Exception:
    METRICS_REGISTRY_AVAILABLE = False
    # Mock metrics for when registry is not available
    class _MockMetric:
        def labels(self, **kwargs):
            return self
        def inc(self, *args, **kwargs):
            pass
        def set(self, *args, **kwargs):
            pass
        def observe(self, *args, **kwargs):
            pass
    ml_confirm_events_total = ml_confirm_errors_total = ml_confirm_cfg_present = \
    ml_confirm_cfg_valid = ml_confirm_enforce_share = ml_confirm_model_loaded = \
    ml_confirm_model_load_seconds = ml_confirm_latency_seconds = ml_missing_critical_total = \
    lambda *args, **kwargs: _MockMetric()

try:
    import joblib  # type: ignore
except Exception:  # pragma: no cover
    joblib = None  # type: ignore



from .decision_policy import MLConfirmDecision

from .utils import (
    _safe_loads_ex,
    _safe_loads,
    _json_safe,
    _scenario_norm,
    _get_floor,
    _f,
    _bucket_from_scenario,
    _canon_sid,
    _canonical_sid,
    _make_sid,
    _mk_crypto_sid,
    _normalize_crypto_sid,
    _normalize_sid,
    _now_ms,
    _should_sample,
    _stable_hash_u64,
    _stable_sample,
    _stable_u01
)


def cache_ml_decision(
    r: Any,
    *,
    sid: str,
    symbol: str,
    bucket: str,
    p_edge: float,
    enforce: int,
    ok_rule: int,
    missing: int,
    model_ver: str,
    ttl_sec: int = 7 * 24 * 3600,
) -> None:
    """
    Cache ML decision for outcome emitter join.
    
    Writes to ml:dec:{sid} key with TTL (default 7 days).
    This allows outcome emitter to do O(1) join on position close.
    
    Args:
        r: Redis client (decode_responses=True), can be sync or aioredis
        sid: Signal ID (canonical format: crypto-of:SYMBOL:ts_ms)
        symbol: Trading symbol
        bucket: Bucket (trend/range/other)
        p_edge: Predicted edge probability
        enforce: Whether decision was enforced (1) or shadow (0)
        ok_rule: Whether rule gate passed (1) or failed (0)
        missing: Whether critical features were missing (1) or not (0)
        model_ver: Model version string
        ttl_sec: TTL in seconds (default: 7 days)
    """
    key = f"ml:dec:{sid}"
    payload = {
        "sid": sid,
        "symbol": symbol.upper(),
        "bucket": str(bucket).lower(),
        "p_edge": float(p_edge),
        "enforce": int(enforce),
        "ok_rule": int(ok_rule),
        "missing": int(missing),
        "model_ver": str(model_ver),  # type: ignore
        "ts_ms": int(_now_ms()),
    }
    payload_str = json.dumps(payload, separators=(",", ":"))
    try:
        import asyncio
        is_async = "aioredis" in type(r).__module__ or "asyncio" in type(r).__module__ or (hasattr(r.set, "__call__") and asyncio.iscoroutinefunction(r.set))
        if is_async:  # type: ignore
            try:
                from utils.task_manager import safe_create_task
                safe_create_task(r.set(key, payload_str, ex=ttl_sec))
            except ImportError:
                asyncio.create_task(r.set(key, payload_str, ex=ttl_sec))
        else:
            r.set(key, payload_str, ex=ttl_sec)  # type: ignore
    except Exception:  # type: ignore
        # Fail-open: don't break decision flow if cache write fails
        pass

  # type: ignore

class MetricsWriterMixin:
    def _cache_ml_decision(
        self,
        dec: MLConfirmDecision,
        *,
        sid: str,
        symbol: str,
        scenario: str,
        ok_rule: int,
    ) -> None:
        """
        Cache ML decision for outcome emitter join.
          # type: ignore
        Called after _emit_metrics to write ml:dec:{sid} cache.
        """
        if not self.r or not sid:  # type: ignore
            return

        # Extract bucket from decision or scenario
        bucket = dec.bucket or _bucket_from_scenario(scenario) or "other"

        # Determine enforce: 1 if ENFORCE mode and decision was allowed, else 0
        enforce = 1 if (self.mode == "ENFORCE" and dec.allow) else 0  # type: ignore

        # Determine missing: 1 if critical features were missing, else 0
        missing = 1 if (dec.missing and len(dec.missing) > 0) else 0

        # Extract model version
        model_ver = dec.model_run_id or getattr(self, "_model_run_id", "") or ""
        if not model_ver and self._cfg:  # type: ignore
            model_ver = str(self._cfg.get("model_ver", "") or "")  # type: ignore

        # Cache decision
        cache_ml_decision(
            self.r,  # type: ignore
            sid=sid,
            symbol=symbol,
            bucket=bucket,
            p_edge=float(dec.p_edge or 0.0),
            enforce=enforce,
            ok_rule=ok_rule,
            missing=missing,
            model_ver=model_ver,
        )  # type: ignore

    def _emit_metrics(self, dec: MLConfirmDecision, *, symbol: str, ts_ms: int, direction: str, scenario: str,
                     rule_score: float, rule_have: int, rule_need: int, cancel_spike_veto: int, ok_rule: int,
                     sid: str | None = None, indicators: dict[str, Any] | None = None) -> None:
        if not self._metrics_enable:  # type: ignore
            return
        redis = self.r  # type: ignore
        if redis is None:
            return
        try:
            # Compute canonical sid for cross-stream joins
            raw_sid = (sid or "") if sid else str(indicators.get("sid") or indicators.get("signal_id") or "") if indicators else ""
            sid = _canon_sid(symbol, ts_ms, raw_sid=raw_sid)
            # Deterministic sampling by sid (stable across restarts)
            sample_rate = float(self._metrics_sample)  # type: ignore
            if sample_rate < 1.0 and sample_rate > 0.0:
                if not _stable_sample(sid, sample_rate, salt=RS.ML_CONFIRM_METRICS):
                    return

            # Extract bucket and exec_risk_norm from indicators or decision
            bucket = dec.bucket or _bucket_from_scenario(scenario)
            exec_risk_norm = 0.0
            exec_risk_bps = 0.0

            # Extract detailed score breakdown if available
            sb = {}
            if indicators:
                exec_risk_norm = float(indicators.get("exec_risk_norm", 0.0) or 0.0)
                exec_risk_bps = float(indicators.get("exec_risk_bps", 0.0) or 0.0)
                sb = indicators.get("score_breakdown") or {}

            # ── feature_schema_ver — stamped on every payload so downstream
            # consumers (monitor_v14_of_canary --schema, drift tools, rollup)
            # can filter without an out-of-band lookup. Read from the loaded
            # model pack (dict) or model attribute (object); empty string if
            # unknown. Best-effort: never blocks emission.
            feature_schema_ver = ""
            try:
                _m = getattr(self, "_model", None)
                if isinstance(_m, dict):
                    feature_schema_ver = str(
                        _m.get("feature_schema_ver")
                        or _m.get("feature_schema_version")
                        or ""
                    )
                elif _m is not None:
                    feature_schema_ver = str(getattr(_m, "feature_schema_ver", "") or "")
            except Exception:
                feature_schema_ver = ""

            payload: dict[str, Any] = {
                "ts_ms": ts_ms,
                "sid": sid,
                "symbol": symbol,
                "mode": self.mode,  # type: ignore
                "kind": dec.kind or "",
                "model_run_id": str(dec.model_run_id or ""),
                # model_ver mirrors model_run_id so ml_predictions_writer can persist it
                "model_ver": str(dec.model_run_id or getattr(self, "_model_run_id", "") or ""),
                "feature_schema_ver": feature_schema_ver,
                "bucket": bucket,
                "cfg_source": getattr(self, "_cfg_source", "none"),
                "direction": str(direction),
                "scenario_v4": str(scenario),
                "rule_score": f"{float(rule_score):.6f}",

                # Extended score breakdown metrics (Step 1)
                "rule_base_score": f"{float(sb.get('base_score', rule_score)):.6f}",
                "rule_score_raw": f"{float(sb.get('final_score_raw', rule_score)):.6f}",
                "rule_exec_pen": f"{float(sb.get('exec_pen', 0.0)):.6f}",
                "score_raw_sum": f"{float(sb.get('raw_sum', 0.0)):.6f}",
                "score_w_sum": f"{float(sb.get('w_sum', 0.0)):.6f}",
                "score_agg": (sb.get('agg', 'unknown')),

                "rule_have": str(int(rule_have)),
                "rule_need": str(int(rule_need)),
                "have_need_ratio": f"{(float(rule_have) / max(1.0, float(rule_need))):.3f}",
                "ok_rule": str(int(ok_rule)),
                "cancel_spike_veto": str(int(cancel_spike_veto)),
                "p_edge": float(dec.p_edge or 0.0),
                "p_edge_cal": float(dec.p_edge_cal or 0.0),
                "p_edge_raw": float(dec.p_edge_raw or 0.0),
                "p_min": dec.p_min,
                "p_margin": dec.p_margin,
                "lat_ms": f"{float(dec.latency_us or 0) / 1000.0:.3f}",
                "latency_us": str(int(dec.latency_us or 0)),
                "latency_ms": f"{float(dec.latency_us or 0) / 1000.0:.3f}",
                "status": str(dec.status or ""),
                "allow": str(int(bool(dec.allow))),
                "err": str(dec.error or ""),
                "abstain": str(int(bool(dec.abstain))),
                "conf": f"{float(dec.conf or 0.0):.6f}",
                "missing_n": str(len(dec.missing or [])),
            }
            # Attach rule score breakdown (if present) for drift/debug
            if indicators and isinstance(indicators.get("score_breakdown"), dict):
                sb = indicators.get("score_breakdown") or {}
                try:
                    payload["rule_base_score"] = float(sb.get("base_score", 0.0) or 0.0)
                    payload["rule_exec_pen"] = float(sb.get("exec_pen", 0.0) or 0.0)
                    payload["rule_score_raw"] = float(sb.get("final_score_raw", sb.get("final_score", 0.0)) or 0.0)
                    payload["rule_score_01"] = float(sb.get("final_score_01", sb.get("final_score", 0.0)) or 0.0)
                    payload["score_raw_sum"] = float(sb.get("raw_sum", 0.0) or 0.0)
                    payload["score_w_sum"] = float(sb.get("w_sum", 0.0) or 0.0)
                    payload["score_agg"] = (sb.get("agg", "") or "")
                except Exception:
                    pass
            # Also attach exec risk reference if present
            if indicators and "exec_risk_ref_bps" in indicators:
                with contextlib.suppress(Exception):
                    payload["exec_risk_ref_bps"] = float(indicators.get("exec_risk_ref_bps") or 0.0)

            # Full score breakdown as JSON (P0 requirement)
            if indicators:
                sb = indicators.get("score_breakdown")
                if sb and isinstance(sb, dict):
                    with contextlib.suppress(Exception):
                        payload["score_breakdown_json"] = json.dumps(sb, separators=(",", ":"))

                # Ensure exec_pen is available at top level if needed (aliasing rule_exec_pen)
                # rule_exec_pen is already in payload, but we add exec_pen explicitly if requested
                if sb and "exec_pen" in sb:
                    payload["exec_pen"] = float(sb.get("exec_pen", 0.0) or 0.0)
            # Add exec_risk fields if present (useful for drift analysis)
            if exec_risk_norm > 0.0 or exec_risk_bps > 0.0:
                payload["exec_risk_norm"] = float(exec_risk_norm)
                payload["exec_risk_bps"] = float(exec_risk_bps)

            # Add low-cardinality context fields (useful for slicing metrics)
            if indicators:
                for k in ["spread_bucket", "session", "liq_regime", "vol_regime", "regime_bucket", "regime_group"]:
                    v = indicators.get(k)
                    if v is not None and v != "":
                        payload[k] = str(v)
                for k in ["data_health", "book_health_ok", "tick_time_age_abs_ema_ms", "tick_event_stream_skew_abs_ema_ms"]:
                    v = indicators.get(k)
                    if v is not None:
                        try:
                            payload[k] = f"{float(v):.6f}"
                        except Exception:
                            payload[k] = str(v)  # type: ignore
                # exec_risk reference if exported by rule-engine
            # Add exec_risk reference if exported by rule-engine  # type: ignore
            if indicators:
                if "exec_risk_ref_bps" in indicators:  # type: ignore
                    with contextlib.suppress(Exception):
                        payload["exec_risk_ref_bps"] = float(indicators.get("exec_risk_ref_bps") or 0.0)

            # Rule-gate score breakdown (if provided by OFConfirmEngine enrichment)  # type: ignore
            if indicators:
                sb = indicators.get('score_breakdown_small') or indicators.get('score_breakdown')
                if isinstance(sb, dict):
                    try:
                        payload['rule_base_score'] = f"{float(sb.get('base_score', 0.0) or 0.0):.6f}"
                        payload['rule_exec_pen'] = f"{float(sb.get('exec_pen', 0.0) or 0.0):.6f}"
                        payload['rule_score_raw'] = f"{float(sb.get('final_score_raw', sb.get('final_score', 0.0)) or 0.0):.6f}"
                        payload['rule_score_01'] = f"{float(sb.get('final_score_01', payload.get('rule_score', 0.0)) or 0.0):.6f}"
                        payload['score_raw_sum'] = f"{float(sb.get('raw_sum', 0.0) or 0.0):.6f}"
                        payload['score_w_sum'] = f"{float(sb.get('w_sum', 0.0) or 0.0):.6f}"
                        payload['score_agg'] = (sb.get('agg', '') or '')
                    except Exception:
                        pass

            # Add util_* fields if available
            if indicators:
                for h in ["util_h1", "util_h4", "util_h24"]:
                    u = indicators.get(h)
                    if u is not None:
                        # store as string with correct key
                        payload[h] = f"{float(u):.6f}"

            import asyncio
            is_async = "aioredis" in type(redis).__module__ or "asyncio" in type(redis).__module__ or (hasattr(redis.xadd, "__call__") and asyncio.iscoroutinefunction(redis.xadd))

            # ── Per-schema dual-write (opt-in, default OFF) ─────────────────
            # When ML_CONFIRM_METRICS_PER_SCHEMA_STREAM=1 and the loaded model
            # exposes feature_schema_ver, additionally XADD to
            # `<base>:<schema_ver>` (e.g. `metrics:ml_confirm:v15_of`). The
            # base stream is always written so existing consumers
            # (rollup worker, autopromoter, drift monitor) keep working;
            # per-schema consumers (monitor_v14_of_canary --stream-key …) get
            # a clean isolated view for canary/shadow comparisons.
            per_schema_enabled = os.getenv(
                "ML_CONFIRM_METRICS_PER_SCHEMA_STREAM", "0"
            ).strip() in ("1", "true", "TRUE", "yes")
            schema_stream = (
                f"{self._metrics_stream}:{feature_schema_ver}"  # type: ignore
                if per_schema_enabled and feature_schema_ver
                else ""
            )

            if is_async:
                try:
                    from utils.task_manager import safe_create_task
                    safe_create_task(redis.xadd(self._metrics_stream, payload, maxlen=self._metrics_maxlen, approximate=True))  # type: ignore
                    if schema_stream:
                        safe_create_task(redis.xadd(schema_stream, payload, maxlen=self._metrics_maxlen, approximate=True))  # type: ignore
                except ImportError:
                    asyncio.create_task(redis.xadd(self._metrics_stream, payload, maxlen=self._metrics_maxlen, approximate=True))  # type: ignore
                    if schema_stream:
                        asyncio.create_task(redis.xadd(schema_stream, payload, maxlen=self._metrics_maxlen, approximate=True))  # type: ignore
            else:
                redis.xadd(self._metrics_stream, payload, maxlen=self._metrics_maxlen, approximate=True)  # type: ignore
                if schema_stream:
                    with contextlib.suppress(Exception):
                        redis.xadd(schema_stream, payload, maxlen=self._metrics_maxlen, approximate=True)  # type: ignore
        except Exception as e:
            # Increment error metric and rate-limited log
            if METRICS_REGISTRY_AVAILABLE:
                self._metrics_errors_total.labels(kind=dec.kind or "unknown", reason="emit_metrics").inc()  # type: ignore
            # Rate-limited logging (at most once per 30 seconds)
            if not hasattr(self, '_last_emit_metrics_error_log_ts'):
                self._last_emit_metrics_error_log_ts = 0
            now_ms = _now_ms()
            if now_ms - self._last_emit_metrics_error_log_ts > 30000:
                import logging
                logger = logging.getLogger("ml_confirm_gate")
                logger.warning(f"ML gate: _emit_metrics error: {type(e).__name__}: {str(e)[:200]}")
                self._last_emit_metrics_error_log_ts = now_ms

