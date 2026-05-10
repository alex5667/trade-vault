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


class SelectivePolicyMixin:
    @staticmethod
    def _conf_from_margin(p_margin: float) -> float:
        # stable 0..1 proxy, cheap (no dependence on p calibration)
        # grows with |margin|
        try:
            return float(1.0 - math.exp(-abs(float(p_margin))))
        except Exception:
            return 0.0

    def _apply_selective(self, dec: MLConfirmDecision, *, ok_rule: int) -> None:
        """Softening ENFORCE decisions near threshold / low confidence."""
        if self.mode != "ENFORCE" or int(ok_rule) != 1:
            if self.mode == "SHADOW":
                dec.status = dec.status or "SHADOW"
            return
        if dec.error:
            dec.status = dec.status or "ERR"
            return
        if dec.missing:
            # missing handled earlier
            return
        band = float(self._abstain_band or 0.0)
        if band > 0.0 and abs(float(dec.p_margin)) <= band:
            dec.abstain = True
            dec.allow = True
            dec.status = "ABSTAIN_BAND"
            dec.reason = f"ml_abstain_band(margin={dec.p_margin:.6f},band={band:.6f})"
            return
        cmin = float(self._conf_min or 0.0)
        if cmin > 0.0 and float(dec.conf) < cmin:
            dec.abstain = True
            dec.allow = True
            dec.status = "ABSTAIN_LOWCONF"
            dec.reason = f"ml_abstain_lowconf(conf={dec.conf:.6f},min={cmin:.6f})"

