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
from core.champion_cfg_validator import validate_champion_cfg  # type: ignore
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

_SHARED_MODELS: dict[str, Any] = {}
_SHARED_MODEL_STATS: dict[str, tuple[float, int]] = {} # path -> (mtime, size)
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


# Process-level shared caches to prevent redundant I/O and thundering herd.
# Keys: model_path or config_key. Values: loaded objects or dicts.
_SHARED_MODELS: dict[str, Any] = {}
_SHARED_CONFIGS: dict[str, Any] = {}
_SHARED_CONFIG_PAYLOADS: dict[str, bytes] = {}  # key -> last raw payload
_SHARED_MODEL_STATS: dict[str, tuple[float, int]] = {} # path -> (mtime, size)


def _load_model_cached(model_path: str, kind: str, logger: Any = None) -> Any | None:
    """Load model from disk or return from process-level cache if unchanged."""
    if not model_path or not os.path.exists(model_path):
        print(f"DEBUG: Model path does not exist: {model_path}", flush=True)
        return None

    try:
        mtime = os.path.getmtime(model_path)
        size = os.path.getsize(model_path)
    except Exception as e:
        print(f"DEBUG: Failed to get stats for {model_path}: {e}", flush=True)
        return None

    stats = (mtime, size)

    # Check cache
    if model_path in _SHARED_MODELS and _SHARED_MODEL_STATS.get(model_path) == stats:
        if logger:
            logger.debug(f"ML gate: Using cached model for {model_path} (kind={kind})")
        return _SHARED_MODELS[model_path]

    # Reload needed
    if logger:
        logger.info(f"ML gate: Loading model from {model_path} (kind={kind})")

    model = None
    try:
        if kind == "meta_lr":
            from core.meta_model_lr import MetaModelLR
            model = MetaModelLR.load(model_path)
        elif kind.startswith("util_mh_fastlinear") or model_path.lower().endswith(".json"):
            from core.fast_linear_util_mh import FastLinearUtilMHModel
            model = FastLinearUtilMHModel.load(model_path)
        else:
            if joblib:
                try:
                    model = joblib.load(model_path)
                except ModuleNotFoundError as e:
                    if "catboost" in str(e).lower():
                        if logger:
                            logger.error(f"ML gate: missing optional dependency 'catboost' for model {model_path}. Prediction may fail.")
                        return None
                    raise

        if model:
            # Validation
            kind_low = (kind or "").lower()
            if kind_low.startswith("util_mh"):
                if not hasattr(model, "predict_util") or not hasattr(model, "predict_unc"):
                    if logger:
                        logger.error(f"ML gate: Model at {model_path} missing predict_util/predict_unc methods")
                    return None
            elif kind_low == "edge_stack_v1":
                if not isinstance(model, dict) or model.get("kind") != "edge_stack_v1":
                    if logger:
                        logger.error(f"ML gate: Model at {model_path} is not a valid edge_stack_v1 pack")
                    return None
                required_keys = ["lr", "gbdt", "meta", "feature_cols"]
                if any(k not in model for k in required_keys):
                    if logger:
                        logger.error(f"ML gate: edge_stack_v1 model at {model_path} missing keys: {[k for k in required_keys if k not in model]}")
                    return None

            _SHARED_MODELS[model_path] = model
            _SHARED_MODEL_STATS[model_path] = stats
            if logger:
                logger.info(f"ML gate: Successfully loaded and cached model from {model_path} (type={type(model).__name__})")
            print(f"DEBUG: Successfully loaded model from {model_path}", flush=True)  # type: ignore
    except Exception as e:
        print(f"DEBUG: Failed to load model from {model_path}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        if logger:  # type: ignore
            logger.error(f"ML gate: Failed to load model from {model_path}: {e}")

    return model



class _DictPackModelView:  # type: ignore
    """Expose dict-pack model keys as attributes for _build_feature_row.

    _build_feature_row is written against an object interface (attrs like
    feature_cols/feature_transforms/robust_scaler/session_cfg/...).
    For edge_stack_v1 we load a dict-pack (joblib) and wrap it into this view  # type: ignore
    to keep train==serve feature engineering consistent.
    """

    def __init__(self, pack: dict[str, Any]):
        self.feature_cols = list(pack.get("feature_cols", []) or [])
        tf = pack.get("feature_transforms")
        self.feature_transforms = tf if isinstance(tf, dict) else {}
  # type: ignore
        # RobustScalerPack accepts either RobustScalerPack or dict params.
        self.robust_scaler = pack.get("robust_scaler")  # type: ignore
  # type: ignore
        sc = pack.get("session_cfg")
        self.session_cfg = sc if isinstance(sc, dict) else {}
  # type: ignore
        self.spread_bucket_edges = pack.get("spread_bucket_edges")  # type: ignore

        lc = pack.get("liq_cfg")
        self.liq_cfg = lc if isinstance(lc, dict) else {}



class ModelLoaderMixin:
    def _refresh_cache_if_needed(self) -> None:
        import logging
        logger = logging.getLogger("ml_confirm_gate")

        if self.mode == "OFF":  # type: ignore
            self._cfg, self._model = {}, None
            return

        now = _now_ms()
        if self._cache_loaded_ms and (now - self._cache_loaded_ms) < self._cache_ttl_ms:  # type: ignore
            return

        if not self._cache_loaded_ms and self._cfg and self._model:
            self._cache_loaded_ms = now
            return

        cfg, model = self._load_cfg_and_model()  # type: ignore
        if not cfg and self._model_load_error in ("no_cfg", "") and self._cfg:  # type: ignore
            # Transient Redis failure: preserve existing config, do NOT advance cache timestamp
            # so the next call retries instead of serving ERR_NO_CFG for the whole TTL window.
            logger.warning(
                f"ML gate: Redis returned no cfg (error={self._model_load_error}), "  # type: ignore
                f"keeping existing config from cfg_source={getattr(self, '_cfg_source', 'none')}"
            )
            return
        self._cfg = cfg or {}
        self._model = model
        self._cache_loaded_ms = now

        if model is None and self._model_load_error:  # type: ignore
            logger.warning(
                f"ML gate: Model not loaded (mode={self.mode}, cfg_source={getattr(self, '_cfg_source', 'none')}, "  # type: ignore
                f"error={self._model_load_error})"  # type: ignore
            )

        self._refresh_selective_knobs_from_cfg()  # type: ignore
        self._load_calibrator_sync(logger)  # type: ignore

