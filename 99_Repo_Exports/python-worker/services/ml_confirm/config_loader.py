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
    bucketize,  # type: ignore
    derive_regime_label,
    derive_session_label,
)
from core.meta_model_lr import MetaModelLR
from services.ml_calibration import PlattLogitCalibrator
from common.isotonic_calibration import IsotonicCalibrator
from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS

_SHARED_CONFIGS: dict[str, Any] = {}
_SHARED_CONFIG_PAYLOADS: dict[str, bytes] = {}  # key -> last raw payload
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


def _read_calibrator_file(path: str) -> dict[str, Any] | None:
    """Read calibrator artifact (.json/.joblib) → raw dict, fail-soft.

    Used both by ConfigLoaderMixin._load_calibrator_sync for sibling-file
    auto-discovery and by tools/refit_meta_lr_blend_calibrator.py for
    artifact round-trip checks.
    """
    try:
        if path.endswith(".json"):
            with open(path, encoding="utf-8") as f:
                obj = json.load(f)
            return obj if isinstance(obj, dict) else None
        if path.endswith(".joblib") and joblib is not None:
            obj = joblib.load(path)
            return obj if isinstance(obj, dict) else None
    except Exception:
        return None
    return None


def _build_calibrator_from_dict(
    cal: dict[str, Any], *, logger: Any = None, src: str = "",
) -> Any | None:
    """Build a calibrator instance from its serialized dict.

    Supports type ∈ {platt_logit, isotonic}. Returned object exposes
    apply_one(p_raw) → p_cal. Returns None for unknown/invalid payloads.
    """
    try:
        ctype = (cal.get("type", "") or "").lower()
        if ctype == "platt_logit":
            return PlattLogitCalibrator.from_dict(cal)
        if ctype == "isotonic":
            return IsotonicCalibrator.from_dict(cal)
        if logger is not None:
            logger.warning(f"ML gate: unknown calibrator type={ctype!r} src={src!r}")
        return None
    except Exception as e:
        if logger is not None:
            logger.warning(f"ML gate: calibrator build failed src={src!r}: {e}")
        return None


from .decision_policy import MLConfirmDecision

from .model_loader import _load_model_cached
from .utils import (
    _safe_loads_ex,
    _safe_loads,
    _json_safe,  # type: ignore
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
    _should_sample,  # type: ignore
    _stable_hash_u64,
    _stable_sample,
    _stable_u01
)


class ConfigLoaderMixin:  # type: ignore
    def _coerce_hash_cfg(self, h: dict[str, str]) -> dict[str, Any]:
        """
        HGETALL returns strings. Keep as strings, parsing happens downstream (float/int) in existing code.
        """
        cfg: dict[str, Any] = {}  # type: ignore
        for k, v in h.items():  # type: ignore
            cfg[str(k)] = v
        cfg.setdefault("mode", "SHADOW")
        cfg.setdefault("fail_policy", "OPEN")
        cfg.setdefault("enforce_share", 0.05)
        return cfg

    def _load_cfg_and_model(self) -> tuple[dict[str, Any], Any]:  # type: ignore
        """
        Load configuration and model from Redis with shared process-level caching.
        """
        import logging
        logger = logging.getLogger("ml_confirm_gate")
  # type: ignore
        self._cfg_key_used = self.champion_key  # type: ignore
        self._cfg_source = "none"
        self._cfg_raw_len = 0
        self._cfg_parse_err = ""  # type: ignore
        self._model_load_error = ""


        raw_payload = None
  # type: ignore
        # Step 1: Resolve raw payload from Redis
        try:
            # 1a. Try Champion
            raw_p = self.r.get(self.champion_key)  # type: ignore
            if raw_p:
                try:
                    p, _, _ = _safe_loads_ex(raw_p)
                    if isinstance(p, dict) and p:
                        raw_payload = raw_p
                        self._cfg_source = "champion"
                        self._cfg_key_used = self.champion_key  # type: ignore
                except Exception:
                    pass  # type: ignore

            # 1b. Try Challenger (only if SHADOW and no successful Champion)
            if not raw_payload and self.mode == "SHADOW":  # type: ignore
                raw_p = self.r.get(self.challenger_key)  # type: ignore
                if raw_p:
                    try:
                        p, _, _ = _safe_loads_ex(raw_p)  # type: ignore
                        if isinstance(p, dict) and p:  # type: ignore
                            raw_payload = raw_p
                            self._cfg_source = "challenger"
                            self._cfg_key_used = self.challenger_key  # type: ignore
                    except Exception:
                        pass

            # 1c. Hash fallback
            if not raw_payload:
                h = self.r.hgetall(self._cfg_hash_key)  # type: ignore
                if h and isinstance(h, dict) and len(h) > 0:
                    cfg_dict = self._coerce_hash_cfg(h)
                    self._cfg_source = "hash_fallback"
                    self._cfg_key_used = self._cfg_hash_key  # type: ignore
                    # Represent as JSON for the cache/metrics
                    raw_payload = json.dumps(cfg_dict, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

                    # Bootstrap if needed (legacy behavior expected by tests)
                    self.r.set(self.champion_key, raw_payload)  # type: ignore
        except Exception as e:
            logger.error(f"ML gate: Redis error in _load_cfg_and_model: {e}")
            raw_payload = None

        if not raw_payload:
            self._model_load_error = "no_cfg"
            return {}, None

        if not raw_payload:  # type: ignore
            self._model_load_error = "no_cfg"  # type: ignore
            return {}, None

        return self._parse_and_load_from_payload(raw_payload, id(self.r), logger)  # type: ignore

    def _parse_and_load_from_payload(self, raw_payload: Any, cache_key_id: int, logger: Any) -> tuple[dict[str, Any], Any]:  # type: ignore
        self._cfg_raw_len = len(raw_payload)

        # Step 2: Check process-level cache for JSON payloads (Isolated by Redis instance ID)
        cache_key = (cache_key_id, self._cfg_key_used)  # type: ignore
        if _SHARED_CONFIG_PAYLOADS.get(cache_key) == raw_payload:  # type: ignore
            cached_cfg = _SHARED_CONFIGS.get(cache_key)  # type: ignore
            if cached_cfg:  # type: ignore
                model_path = cached_cfg.get("model_path")
                kind = (cached_cfg.get("kind", "")).lower()
                model = _load_model_cached(model_path, kind, logger=logger)
                return cached_cfg.copy(), model

        # Step 3: Parse and Validate
        try:
            payload_str = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else str(raw_payload)
            cfg, _, _ = _safe_loads_ex(payload_str)
            if not isinstance(cfg, dict):
                cfg = {}

            try:
                cfg_validated, validation_info = validate_champion_cfg(payload_str)
                # Ensure validated fields are mapped if validation succeeded
                cfg["model_path"] = cfg_validated.model_path
                cfg["kind"] = cfg_validated.kind
                cfg["mode"] = cfg_validated.mode
                cfg["enforce_share"] = cfg_validated.enforce_share
                cfg["run_id"] = cfg_validated.run_id  # type: ignore
            except Exception as ve:
                # Lenient mode: Log warning but keep the parsed JSON
                logger.warning(f"ML gate: Config validation failed for {self._cfg_key_used}, but using as-is (legacy): {ve}")

            # Update cache
            _SHARED_CONFIG_PAYLOADS[cache_key] = raw_payload  # type: ignore
            _SHARED_CONFIGS[cache_key] = cfg  # type: ignore

            # Load model
            model_path = cfg.get("model_path")
            kind = cfg.get("kind")
            model = _load_model_cached(model_path, kind, logger=logger)  # type: ignore

            if METRICS_REGISTRY_AVAILABLE:
                k = kind or "unknown"  # type: ignore
                self._metrics_cfg_present.labels(kind=k).set(1)  # type: ignore
                self._metrics_cfg_valid.labels(kind=k).set(1)  # type: ignore
                if model:
                    self._metrics_model_loaded.labels(kind=k).set(1)  # type: ignore

            return cfg.copy(), model  # type: ignore

        except Exception as e:
            # Match legacy error reporting for tests
            err_msg = str(e)
            self._cfg_parse_err = f"invalid_cfg({err_msg})" if ("mode" in err_msg or "enforce_share" in err_msg) else err_msg
            self._model_load_error = f"parse_error:{type(e).__name__}"
            logger.error(f"ML gate: Config parse/validate failed for {self._cfg_key_used}: {e}")  # type: ignore
            return {}, None

    async def refresh_async(self, redis_async: Any) -> None:
        """
        Async version of _refresh_cache_if_needed to eliminate blocking calls in main loop.  # type: ignore
        """
        import json
        import logging
        logger = logging.getLogger("ml_confirm_gate")

        if self.mode == "OFF":  # type: ignore
            self._cfg, self._model = {}, None
            return  # type: ignore

        now = _now_ms()
        # Use existing TTL
        if self._cache_loaded_ms and (now - self._cache_loaded_ms) < self._cache_ttl_ms:  # type: ignore
            return
  # type: ignore
        # Protect test overrides
        if not self._cache_loaded_ms and self._cfg and self._model:
            self._cache_loaded_ms = now
            return  # type: ignore

        # 1. Fetch from Redis (Async)
        self._cfg_key_used = self.champion_key  # type: ignore
        self._cfg_source = "none"
        raw_payload = None

        try:
            # 1a. Try Champion
            raw_p = await redis_async.get(self.champion_key)  # type: ignore
            if raw_p:
               try:
                   p, _, _ = _safe_loads_ex(raw_p)
                   if isinstance(p, dict) and p:
                       raw_payload = raw_p
                       self._cfg_source = "champion"
                       self._cfg_key_used = self.champion_key  # type: ignore
               except Exception:
                   pass

            # 1b. Challenger
            if not raw_payload and self.mode == "SHADOW":  # type: ignore
                raw_p = await redis_async.get(self.challenger_key)  # type: ignore
                if raw_p:
                    try:
                        p, _, _ = _safe_loads_ex(raw_p)
                        if isinstance(p, dict) and p:
                            raw_payload = raw_p
                            self._cfg_source = "challenger"
                            self._cfg_key_used = self.challenger_key  # type: ignore
                    except Exception:
                        pass  # type: ignore

            # 1c. Hash Fallback
            if not raw_payload:
                h = await redis_async.hgetall(self._cfg_hash_key)  # type: ignore
                if h and isinstance(h, dict) and len(h) > 0:
                     cfg_dict = self._coerce_hash_cfg(h)
                     self._cfg_source = "hash_fallback"
                     self._cfg_key_used = self._cfg_hash_key  # type: ignore
                     raw_payload = json.dumps(cfg_dict, ensure_ascii=False, separators=(",", ":")).encode("utf-8")  # type: ignore
        except Exception as e:
            logger.error(f"ML gate: Async Redis error: {e}")
            # Don't return, allow retry next loop
            return
  # type: ignore
        if not raw_payload:
            self._model_load_error = "no_cfg"
            # Do not clear existing config on momentary Redis failure, just return
            return

        # 2. Parse & Load (Run in thread to avoid blocking loop depending on model size)
        loop = asyncio.get_running_loop()
        try:
            # Use id(redis_async) for cache isolation
            cfg, model = await loop.run_in_executor(  # type: ignore
                None,
                self._parse_and_load_from_payload,
                raw_payload,
                id(redis_async),
                logger
            )
            self._cfg = cfg or {}
            self._model = model
            self._cache_loaded_ms = now

            # Refresh selective knobs logic (duplicated from sync path for now)
            self._refresh_selective_knobs_from_cfg()

            # Load calibrator logic
            if self._calibrate_enabled:  # type: ignore
                 await loop.run_in_executor(None, self._load_calibrator_sync, logger)

            # Dual-emit: independent challenger load (fire-and-forget).
            try:
                await self._load_challenger_only_async(redis_async)  # type: ignore[attr-defined]
            except Exception as ce:
                logger.debug(f"ML gate: challenger async load skipped: {ce}")

        except Exception as e:
            logger.error(f"ML gate: Async parse failed: {e}")

    def _refresh_selective_knobs_from_cfg(self) -> None:
        try:
            if self._cfg.get("abstain_band") is not None:
                self._abstain_band = float(self._cfg.get("abstain_band"))  # type: ignore
        except Exception:
            pass
        try:
            if self._cfg.get("conf_min") is not None:
                self._conf_min = float(self._cfg.get("conf_min"))  # type: ignore
        except Exception:
            pass
        try:
            if self._cfg.get("abstain_on_missing") is not None:
                self._abstain_on_missing = int(float(self._cfg.get("abstain_on_missing") or 0)) == 1
        except Exception:
            pass
        try:
            if self._cfg.get("p_min_hard_floor") is not None:
                self._p_min_hard_floor = float(self._cfg.get("p_min_hard_floor"))  # type: ignore
        except Exception:
            pass

        self._mode_by_symbol = {}
        self._enforce_share_by_symbol = {}
        try:
            mode_overrides = self._cfg.get("mode_overrides")
            if isinstance(mode_overrides, dict):
                by_sym = mode_overrides.get("by_symbol") or {}
                if isinstance(by_sym, dict):
                    _allowed = {"OFF", "SHADOW", "CANARY", "ENFORCE"}
                    for sym, m in by_sym.items():
                        m_up = str(m).strip().upper()
                        if m_up in _allowed:
                            self._mode_by_symbol[str(sym).strip().upper()] = m_up
                es_sym = mode_overrides.get("enforce_share_by_symbol") or {}
                if isinstance(es_sym, dict):
                    for sym, share in es_sym.items():
                        try:
                            sv = float(share)
                            if 0.0 <= sv <= 1.0:
                                self._enforce_share_by_symbol[str(sym).strip().upper()] = sv
                        except (TypeError, ValueError):
                            pass
        except Exception:
            pass

    def _load_calibrator_sync(self, logger: Any) -> None:
        # Re-use logic from _refresh_cache_if_needed for calibrator
        self._calibrator = None
        self._calib_type = "none"

        # Priority 1: cfg.calibrator
        cal = self._cfg.get("calibrator", None)
        if isinstance(cal, dict):
            built = _build_calibrator_from_dict(cal, logger=logger, src="cfg.calibrator")
            if built is not None:
                self._calibrator, self._calib_type = built, f"cfg_calibrator:{cal.get('type', '?')}"
                logger.info(f"ML gate: Calibrator loaded from cfg.calibrator (type={cal.get('type')})")

        # Priority 2: cfg.calibrator_path
        if self._calibrator is None:
            cal_path = self._cfg.get("calibrator_path", None)
            if cal_path and isinstance(cal_path, str) and cal_path.strip():
                try:
                    if os.path.exists(cal_path):
                        cal_dict = _read_calibrator_file(cal_path)
                        if cal_dict is not None:
                            built = _build_calibrator_from_dict(
                                cal_dict, logger=logger, src=f"path:{cal_path}",
                            )
                            if built is not None:
                                self._calibrator = built
                                self._calib_type = "cfg_calibrator_path"
                                logger.info(f"ML gate: Calibrator loaded from cfg.calibrator_path={cal_path}")
                except Exception as e:
                    logger.warning(f"ML gate: Failed to load calibrator from cfg.calibrator_path={cal_path}: {e}")

        # Priority 3: model pack
        if self._calibrator is None and self._model is not None:
            try:
                cal_dict = None
                if isinstance(self._model, dict):
                    cal_dict = self._model.get("calibrator", None)
                elif hasattr(self._model, "calibrator"):
                    cal_dict = getattr(self._model, "calibrator", None)
                if isinstance(cal_dict, dict):
                    built = _build_calibrator_from_dict(cal_dict, logger=logger, src="model_pack")
                    if built is not None:
                        self._calibrator = built
                        self._calib_type = "model_pack_calibrator"
                        logger.info("ML gate: Calibrator loaded from model pack")
            except Exception as e:
                logger.warning(f"ML gate: Failed to load calibrator from model: {e}")

        # Priority 4 (2026-05-23 fix): sibling file next to model_path.
        # Tried in order:
        #   a) {model_dir}/calibrator_{model_basename_no_ext}.json — model-
        #      specific name, written by ml_calibrator_autopilot_v1 when
        #      multiple kinds share a registry dir.
        #   b) {model_dir}/calibrator.json — generic name, written by the
        #      original meta_lr_blend_calibrator_refit_v1 and as a back-
        #      compat alias by the autopilot.
        # First match wins. tools/refit_*_calibrator.py promote artifacts
        # without touching the cfg payload — sibling discovery picks them
        # up on the next cfg cache refresh.
        if self._calibrator is None and self._model is not None:
            try:
                model_path = ""
                if isinstance(self._model, dict):
                    model_path = str(self._model.get("model_path") or self._cfg.get("model_path") or "")
                if not model_path:
                    model_path = str(self._cfg.get("model_path") or "")
                if model_path and os.path.exists(model_path):
                    model_basename = os.path.splitext(os.path.basename(model_path))[0]
                    candidates = [
                        os.path.join(os.path.dirname(model_path), f"calibrator_{model_basename}.json"),
                        os.path.join(os.path.dirname(model_path), "calibrator.json"),
                    ]
                    for sib in candidates:
                        if not os.path.exists(sib):
                            continue
                        cal_dict = _read_calibrator_file(sib)
                        if cal_dict is None:
                            continue
                        built = _build_calibrator_from_dict(
                            cal_dict, logger=logger, src=f"sibling:{sib}",
                        )
                        if built is not None:
                            self._calibrator = built
                            self._calib_type = "sibling_calibrator"
                            logger.info(f"ML gate: Calibrator loaded from sibling file {sib}")
                            break
            except Exception as e:
                logger.warning(f"ML gate: Failed to load sibling calibrator: {e}")

        if self._calibrator is None:
             logger.debug("ML gate: No calibrator loaded")

        # 2026-05-23: kind-generic gauge so dashboards / alerts can spot
        # uncalibrated kinds across the full ml_confirm matrix (not just
        # meta_lr_blend). Kind is resolved from cfg → model pack → "unknown".
        with contextlib.suppress(Exception):
            from services.observability.metrics_registry import ml_confirm_calibrator_loaded
            if ml_confirm_calibrator_loaded is not None:
                kind = ""
                try:
                    kind = str(self._cfg.get("kind") or "")
                    if not kind and isinstance(self._model, dict):
                        kind = str(self._model.get("kind") or "")
                    if not kind and self._model is not None and hasattr(self._model, "kind"):
                        kind = str(getattr(self._model, "kind", "") or "")
                except Exception:
                    pass
                ml_confirm_calibrator_loaded.labels(kind=kind or "unknown").set(
                    1 if self._calibrator is not None else 0,
                )

