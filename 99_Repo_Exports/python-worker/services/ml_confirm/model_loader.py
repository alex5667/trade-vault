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


def _load_blend_child_models(pack: dict[str, Any], *, logger: Any = None) -> None:
    """Load child v14/v5 joblib models referenced by meta_lr_blend pack.

    Populates pack with private keys consumed by `_decide_meta_lr_blend`:
      _v14_model, _v14_scaler, _v14_features, _v5_model, _v5_features
    Backward-compatible: if `child_models` absent (legacy schema_version=1
    artifact), nothing is loaded.
    """
    children = pack.get("child_models")
    if not isinstance(children, dict):
        return
    if joblib is None:
        if logger:
            logger.error("ML gate: joblib unavailable; cannot load meta_lr_blend child models")
        return

    v14_cfg = children.get("v14") if isinstance(children.get("v14"), dict) else None
    v5_cfg = children.get("v5") if isinstance(children.get("v5"), dict) else None

    if v14_cfg:
        v14_model_path = str(v14_cfg.get("model_path") or "")
        v14_scaler_path = str(v14_cfg.get("scaler_path") or "")
        v14_features = list(v14_cfg.get("features") or [])
        if v14_model_path and os.path.exists(v14_model_path) and v14_features:
            try:
                pack["_v14_model"] = joblib.load(v14_model_path)
                pack["_v14_features"] = v14_features
                if v14_scaler_path and os.path.exists(v14_scaler_path):
                    pack["_v14_scaler"] = joblib.load(v14_scaler_path)
                else:
                    pack["_v14_scaler"] = None
            except Exception as e:
                if logger:
                    logger.error(f"ML gate: meta_lr_blend child v14 load failed ({v14_model_path}): {e}")
                pack.pop("_v14_model", None)
                pack.pop("_v14_scaler", None)
                pack.pop("_v14_features", None)
        elif logger and (v14_model_path or v14_features):
            logger.warning(
                f"ML gate: meta_lr_blend v14 child unavailable "
                f"(path_exists={os.path.exists(v14_model_path) if v14_model_path else False}, "
                f"n_features={len(v14_features)})"
            )

    if v5_cfg:
        v5_model_path = str(v5_cfg.get("model_path") or "")
        v5_features = list(v5_cfg.get("features") or [])
        if v5_model_path and os.path.exists(v5_model_path) and v5_features:
            try:
                pack["_v5_model"] = joblib.load(v5_model_path)
                pack["_v5_features"] = v5_features
            except Exception as e:
                if logger:
                    logger.error(f"ML gate: meta_lr_blend child v5 load failed ({v5_model_path}): {e}")
                pack.pop("_v5_model", None)
                pack.pop("_v5_features", None)
        elif logger and (v5_model_path or v5_features):
            logger.warning(
                f"ML gate: meta_lr_blend v5 child unavailable "
                f"(path_exists={os.path.exists(v5_model_path) if v5_model_path else False}, "
                f"n_features={len(v5_features)})"
            )


# Phase 0.2 — registry-aware shape guard. Cached per (ver, expected_n) so the
# happy path is one dict lookup; the registry import is lazy to avoid breaking
# unit tests that monkeypatch model loading without a populated registry.
_SHAPE_GUARD_LOGGED: set[str] = set()
_REGISTRY_EXPECTED_N: dict[str, int] = {}


def _registry_expected_n(ver: str) -> int | None:
    """Return registry feature_names length for `ver`, or None if unknown."""
    if not ver:
        return None
    if ver in _REGISTRY_EXPECTED_N:
        return _REGISTRY_EXPECTED_N[ver]
    try:
        from core.feature_registry import get_schema_info
        info = get_schema_info(ver)
        n = len(info.feature_names)
    except Exception:
        n = -1  # mark as unknown but cache to avoid retry storms
    _REGISTRY_EXPECTED_N[ver] = n
    return n if n > 0 else None


def _validate_edge_stack_shape(pack: dict[str, Any], model_path: str, *, logger: Any = None) -> bool:
    """Compare loaded pack's feature_cols length against registry expectations.

    Returns False to fail-closed only when the schema is registry-known and the
    model's feature count exceeds the schema. Subsets are accepted (training may
    drop low-coverage features). Unknown schema versions emit one warning and
    pass through to keep new trainers unblocked.
    """
    ver = str(pack.get("feature_schema_ver") or pack.get("feature_schema_version") or "").strip()
    cols = pack.get("feature_cols") or []
    got = len(cols)
    expected = _registry_expected_n(ver)
    try:
        from services.orderflow.metrics import ml_feature_schema_hash_mismatch_total
    except Exception:
        ml_feature_schema_hash_mismatch_total = None  # type: ignore

    if expected is None:
        # Unknown to registry (e.g. trainer-only naming like "v15_lgbm"). Log
        # once per (ver, path) so we have visibility but don't break loading.
        key = f"unknown:{ver}:{model_path}"
        if key not in _SHAPE_GUARD_LOGGED:
            _SHAPE_GUARD_LOGGED.add(key)
            if logger:
                logger.warning(
                    f"ML gate shape guard: schema_ver={ver!r} unknown to registry; "
                    f"accepting model {model_path} with n_features={got}"
                )
        return True

    if got > expected:
        if ml_feature_schema_hash_mismatch_total is not None:
            try:
                ml_feature_schema_hash_mismatch_total.labels(
                    ver=ver, expected=str(expected), got=str(got)
                ).inc()
            except Exception:
                pass
        if logger:
            logger.error(
                f"ML gate shape guard: model {model_path} has {got} feature_cols but "
                f"registry expects ≤ {expected} for schema_ver={ver!r}; fail-closed"
            )
        return False

    key = f"ok:{ver}:{model_path}:{got}"
    if key not in _SHAPE_GUARD_LOGGED:
        _SHAPE_GUARD_LOGGED.add(key)
        if logger:
            logger.info(
                f"ML gate shape guard: model {model_path} schema_ver={ver!r} "
                f"n_features={got} (registry expects ≤ {expected})"
            )
    return True


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
        elif kind == "meta_lr_blend":
            try:
                with open(model_path, "r", encoding="utf-8") as f:
                    pack = json.load(f)
            except Exception as e:
                if logger:
                    logger.error(f"ML gate: meta_lr_blend JSON load failed for {model_path}: {e}")
                return None
            if not isinstance(pack, dict):
                if logger:
                    logger.error(f"ML gate: meta_lr_blend at {model_path} is not a JSON object (got {type(pack).__name__})")
                return None
            if pack.get("kind") != "meta_lr_blend":
                if logger:
                    logger.error(f"ML gate: meta_lr_blend at {model_path} has wrong kind={pack.get('kind')!r}")
                return None
            for _k in ("intercept", "coef_v14", "coef_v5"):
                if _k not in pack:
                    if logger:
                        logger.error(f"ML gate: meta_lr_blend at {model_path} missing required key '{_k}'")
                    return None
            fn = pack.get("feature_names")
            if not (isinstance(fn, list) and len(fn) == 2 and all(isinstance(x, str) for x in fn)):
                pack["feature_names"] = ["p_v14", "p_v5"]
            _load_blend_child_models(pack, logger=logger)
            model = pack
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

                # Phase 0.2 — serve-time shape guard. Compare model.feature_cols
                # length against the registry expected count for pack's declared
                # feature_schema_ver. Fail-closed only when the registry knows the
                # schema; unknown versions log a warning so new trainer naming
                # (e.g. v15_lgbm) does not break loading.
                if not _validate_edge_stack_shape(model, model_path, logger=logger):
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

        # Dual-emit: independently load challenger cfg/model so it can be
        # scored in SHADOW alongside the champion. No-op when flag disabled.
        try:
            self._load_challenger_only_sync()  # type: ignore[attr-defined]
        except Exception as ce:
            logger.debug(f"ML gate: challenger sync load skipped: {ce}")

