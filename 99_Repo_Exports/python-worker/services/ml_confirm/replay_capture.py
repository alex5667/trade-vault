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
from core.edge_stack_mh_v1 import EdgeStackMHModelV1
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
    class _MockMetric:
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


class ReplayCaptureMixin:
    def _capture_replay_input(self, dec: MLConfirmDecision, *, symbol: str, ts_ms: int, direction: str, scenario: str,
                              indicators: dict[str, Any], rule_score: float, rule_have: int, rule_need: int,
                              cancel_spike_veto: int, ok_rule: int) -> None:
        if not self._replay_capture:
            return
        try:
            # Compute canonical sid for cross-stream joins
            raw_sid = str(indicators.get("sid") or indicators.get("signal_id") or "")
            sid = _canon_sid(symbol, ts_ms, raw_sid=raw_sid)
            # Deterministic sampling by sid (stable across restarts)
            do_emit = True
            if float(self._replay_sample) < 0.999:
                do_emit = _stable_sample(sid, float(self._replay_sample), salt=f"ml_replay_inputs_v1|{self._replay_stream}")
            if not do_emit:
                return
            cfg = self._cfg or {}
            cfg_small = {
                "kind": cfg.get("kind", ""),
                "run_id": cfg.get("run_id", ""),
                "model_path": cfg.get("model_path", ""),
                "util_floors": cfg.get("util_floors", {}),
                "abstain_band": cfg.get("abstain_band", None),
                "conf_min": cfg.get("conf_min", None),
                "abstain_on_missing": cfg.get("abstain_on_missing", None),
                "p_min_hard_floor": cfg.get("p_min_hard_floor", None),
            }
            payload = {
                "ts_ms": int(ts_ms),
                "symbol": symbol.upper(),
                "direction": str(direction),
                "scenario_v4": str(scenario),
                "sid": str(sid),  # Added for deterministic replay
                "indicators": _json_safe(indicators),
                "rule_score": float(rule_score),
                "rule_have": int(rule_have),
                "rule_need": int(rule_need),
                "cancel_spike_veto": int(cancel_spike_veto),
                "ok_rule": int(ok_rule),
                "cfg": _json_safe(cfg_small),
            }
            # Add a compact decision summary for offline audits (does not affect feature vector).
            payload["dec_summary"] = _json_safe({
                "kind": str(dec.kind or ""),
                "mode": str(dec.mode or ""),
                "allow": int(bool(dec.allow)),
                "abstain": int(bool(dec.abstain)),
                "status": str(dec.status or ""),
                "reason": str(dec.reason or ""),
                "bucket": str(dec.bucket or ""),
                "p_edge": float(dec.p_edge or 0.0),
                "p_min": float(dec.p_min or 0.0),
                "p_margin": float(dec.p_margin or 0.0),
                "conf": float(dec.conf or 0.0),
                "missing_n": int(len(dec.missing or [])),
            })
            self.r.xadd(self._replay_stream, {
                "ts_ms": str(int(ts_ms)),
                "symbol": symbol.upper(),
                "scenario_v4": str(scenario),
                "sid": str(sid),  # Added for deterministic replay
                "model_run_id": str(dec.model_run_id or ""),
                "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            }, maxlen=self._replay_maxlen, approximate=True)
        except Exception as e:
            # Increment error metric and rate-limited log
            if METRICS_REGISTRY_AVAILABLE:
                self._metrics_errors_total.labels(kind=dec.kind or "unknown", reason="replay_capture").inc()
            # Rate-limited logging (at most once per 30 seconds)
            if not hasattr(self, '_last_replay_capture_error_log_ts'):
                self._last_replay_capture_error_log_ts = 0
            now_ms = _now_ms()
            if now_ms - self._last_replay_capture_error_log_ts > 30000:
                import logging
                logger = logging.getLogger("ml_confirm_gate")
                logger.warning(f"ML gate: _capture_replay_input error: {type(e).__name__}: {str(e)[:200]}")
                self._last_replay_capture_error_log_ts = now_ms

