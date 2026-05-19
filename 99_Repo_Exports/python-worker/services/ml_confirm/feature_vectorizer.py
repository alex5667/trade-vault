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
)  # type: ignore
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


class FeatureVectorizerMixin:
    def _ensure_exec_risk_norm(self, indicators: dict[str, Any]) -> None:
        # If exec_risk_norm missing, derive it to avoid fail-closed noise.
        if "exec_risk_norm" in indicators:
            return
        spread = _f(indicators.get("spread_bps"), 0.0)
        slip = _f(indicators.get("expected_slippage_bps"), 0.0)
        exec_bps = max(0.0, spread + slip)
        ref = _f(indicators.get("exec_risk_ref_bps", os.getenv("EXEC_RISK_REF_BPS", "10")), 10.0)
        if ref <= 1e-9:
            ref = 10.0
        indicators["exec_risk_bps"] = exec_bps
        indicators["exec_risk_norm"] = max(0.0, min(1.0, exec_bps / ref))

    def _build_feature_row(
        self,
        *,
        model: Any,
        indicators: dict[str, Any],
        direction: str,
        scenario: str,
        ts_ms: int,
    ) -> tuple[list[float], list[str]]:
        feature_cols: list[str] = list(getattr(model, "feature_cols", []) or [])
        missing: list[str] = []

        # critical inputs (accuracy/safety)
        critical = ["spread_bps", "expected_slippage_bps"]
        for k in critical:
            if k not in indicators:
                missing.append(k)

        # derive exec_risk_norm if possible
        self._ensure_exec_risk_norm(indicators)
        if "exec_risk_norm" not in indicators:
            missing.append("exec_risk_norm")

        d = (direction or "").upper()
        s = _scenario_norm(scenario)

        # optional feature engineering (backward compatible)
        transforms = getattr(model, "feature_transforms", None)
        if not isinstance(transforms, dict):
            transforms = {}

        rs = getattr(model, "robust_scaler", None)
        if isinstance(rs, RobustScalerPack):
            scaler = rs
        elif isinstance(rs, dict):
            scaler = RobustScalerPack(params=rs)
        else:  # type: ignore
            scaler = None  # type: ignore

        # regime/session/buckets (only used when feature_cols contain соответствующие префиксы)
        session_cfg = getattr(model, "session_cfg", None)
        if not isinstance(session_cfg, dict):
            session_cfg = {}
        session_label = derive_session_label(int(ts_ms or 0), cfg=session_cfg)

        spread_edges = getattr(model, "spread_bucket_edges", None)
        if not isinstance(spread_edges, (list, tuple)) or len(spread_edges) == 0:
            spread_edges = [2.0, 5.0, 10.0, 20.0]
        spread_bps_raw = _f(indicators.get("spread_bps", 0.0), 0.0)
        spread_bucket_idx = bucketize(float(spread_bps_raw), [float(x) for x in spread_edges])

        def _spread_bucket_label(x: float) -> str:
            try:
                edges = [float(e) for e in spread_edges]
            except Exception:
                edges = [2.0, 5.0, 10.0, 20.0]
            if not edges:
                return "b0"
            if x <= edges[0]:
                return f"le{int(edges[0])}"
            for a, b in zip(edges[:-1], edges[1:]):
                if a < x <= b:
                    return f"{int(a)}_{int(b)}"
            return f"gt{int(edges[-1])}"

        spread_bucket_label = _spread_bucket_label(float(spread_bps_raw))

        liq_cfg = getattr(model, "liq_cfg", None)
        if not isinstance(liq_cfg, dict):
            liq_cfg = {}
        liq_label = derive_regime_label(indicators.get("liq_regime"), fallback_score=_f(indicators.get("liq_score"), None), cfg=liq_cfg)  # type: ignore
        vol_label = derive_regime_label(indicators.get("vol_regime"), fallback_score=_f(indicators.get("vol_score"), None), cfg=liq_cfg)  # type: ignore

        # UTC hour/day-of-week and scenario bucket (legacy bucket:)
        tm = time.gmtime(float(int(ts_ms or 0)) / 1000.0)
        utc_hour = int(getattr(tm, "tm_hour", 0))
        utc_dow = int(getattr(tm, "tm_wday", 0))
        bucket = _bucket_from_scenario(s)
        if not bucket or bucket == "other":
            # Scenario "none" / unknown: fall back to regime label so the ML gate
            # uses a meaningful bucket instead of the catch-all "other" bin.
            _rg = str(indicators.get("regime_bucket") or indicators.get("regime") or "").lower()
            if "trend" in _rg or "bull" in _rg or "bear" in _rg:
                bucket = "trend"
            elif "range" in _rg or "chop" in _rg or "meanrev" in _rg:
                bucket = "range"
            else:
                bucket = "other"

        # B1: bucket2 (breakout/reversal/high_var) — additive categorization.
        # Producer (tick_processor) should set indicators['bucket2'].
        # Fail-open fallback: conservative derivation from scenario/id + indicators.
        try:
            bucket2 = (indicators.get("bucket2") or "").strip().lower()
        except Exception:
            bucket2 = ""
        if not bucket2:
            try:
                bucket2 = str(derive_bucket2_label(s, indicators=indicators) or "").strip().lower()
            except Exception:
                bucket2 = ""

        cache: dict[str, float] = {}

        def num(name: str) -> float:
            if name in cache:
                return cache[name]
            x = _f(indicators.get(name, 0.0), 0.0)
            # non-linear transforms (log/clip/winsor) if configured in model
            if transforms and name in transforms:
                x = apply_transform(float(x), transforms.get(name))
            # robust scaling if exported with model
            if scaler is not None:
                x = scaler.scale(name, float(x))
            cache[name] = float(x)
            return cache[name]

        row: list[float] = []
        for col in feature_cols:
            if col.startswith("f_"):
                key = col[2:]
                row.append(num(key))
            elif col.startswith("mul_"):
                # interaction term: mul_a__b -> a*b (after per-feature transform/scale)
                pair = col[4:]
                if "__" in pair:
                    a, b = pair.split("__", 1)
                    row.append(num(a) * num(b))
                else:
                    row.append(0.0)
            elif col.startswith("direction_"):
                val = col[len("direction_"):].upper()
                row.append(1.0 if val == d else 0.0)
            elif col.startswith("scenario_v4_"):
                val = col[len("scenario_v4_"):].lower()
                row.append(1.0 if val == s else 0.0)
            elif col.startswith("bucket:"):
                val = col[len("bucket:"):].lower()
                row.append(1.0 if val == str(bucket).lower() else 0.0)
            elif col.startswith("bucket2:"):
                val = col[len("bucket2:"):].lower()
                row.append(1.0 if bucket2 and val == str(bucket2).lower() else 0.0)
            elif col.startswith("session_"):
                val = col[len("session_"):].lower()
                row.append(1.0 if val == str(session_label) else 0.0)
            elif col.startswith("hour:"):
                try:
                    hh = int(col[len("hour:"):])
                except Exception:
                    hh = -1
                row.append(1.0 if hh == int(utc_hour) else 0.0)
            elif col.startswith("dow:"):
                try:
                    dd = int(col[len("dow:"):])
                except Exception:
                    dd = -1
                row.append(1.0 if dd == int(utc_dow) else 0.0)
            elif col.startswith("spread_bucket_"):
                val = col[len("spread_bucket_"):].lower()
                ok = (val == str(spread_bucket_idx)) or (val == f"b{spread_bucket_idx}") or (val == spread_bucket_label)
                row.append(1.0 if ok else 0.0)
            elif col.startswith("liq_regime_"):
                val = col[len("liq_regime_"):].lower()
                row.append(1.0 if val == str(liq_label) else 0.0)
            elif col.startswith("vol_regime_"):
                val = col[len("vol_regime_"):].lower()
                row.append(1.0 if val == str(vol_label) else 0.0)
            else:
                row.append(0.0)

        return row, missing

