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

from .model_loader import _DictPackModelView

@dataclass
class MLConfirmDecision:
    mode: str = "OFF"          # OFF|SHADOW|ENFORCE|ERR
    kind: str = "none"         # util_mh_v1|...
    allow: bool = True

    # backwards compatible fields used by OFConfirmEngine final_reason (p_edge/p_min)
    p_edge: float = 0.0
    p_min: float = 0.0

    # util_mh fields
    best_h_ms: int = 0
    score: float = 0.0
    floor: float = 0.0
    bucket: str = "other"
    util_pred: dict[str, float] | None = None
    unc: dict[str, float] | None = None
    missing: list[str] | None = None

    model_run_id: str = ""
    model_path: str = ""
    reason: str = ""
    error: str = ""

    # SRE / perf
    latency_us: int = 0

    # SRE / quality (selective prediction)
    abstain: bool = False
    conf: float = 0.0        # 0..1 proxy (see below)
    p_margin: float = 0.0    # p_edge - p_min (works for util_mh too)
    status: str = ""         # ALLOW|BLOCK|ABSTAIN_*|MISSING_*|SHADOW|OFF|ERR

    # calibration fields (for metrics and drift tracking)
    p_edge_raw: float = 0.0   # pre-calibration probability
    p_edge_cal: float = 0.0   # post-calibration probability (effective p_edge)
    calib_type: str = ""      # platt_logit|none

    # cfg diagnostics (for metrics/debug)
    cfg_key_used: str = ""
    cfg_source: str = ""        # champion|challenger
    cfg_raw_len: int = 0
    cfg_parse_err: str = ""

    # mode routing and execution risk
    effective_mode: str = ""
    mode_source: str = ""
    exec_risk_ref_bps: float = 0.0
    exec_risk_bps: float = 0.0
    exec_risk_norm: float = 0.0
    exec_pen: float = 0.0
    score_breakdown_small: dict[str, float] | None = None
    score_breakdown_json: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "kind": self.kind,
            "allow": bool(self.allow),
            "p_edge": float(self.p_edge),
            "p_min": float(self.p_min),
            "best_h_ms": int(self.best_h_ms),
            "score": float(self.score),
            "floor": float(self.floor),
            "bucket": str(self.bucket),
            "util_pred": self.util_pred or {},
            "unc": self.unc or {},
            "missing": self.missing or [],
            "model_run_id": self.model_run_id,
            "model_path": self.model_path,
            "reason": self.reason,
            "error": self.error,
            "latency_us": int(self.latency_us),
            "abstain": int(bool(self.abstain)),
            "conf": float(self.conf),
            "p_margin": float(self.p_margin),
            "status": str(self.status),
            "p_edge_raw": float(self.p_edge_raw),
            "p_edge_cal": float(self.p_edge_cal),
            "calib_type": str(self.calib_type or ""),
            "cfg_key_used": str(self.cfg_key_used or ""),
            "cfg_source": str(self.cfg_source or ""),
            "cfg_raw_len": int(self.cfg_raw_len),
            "cfg_parse_err": str(self.cfg_parse_err or ""),
            "effective_mode": str(self.effective_mode or ""),
            "mode_source": str(self.mode_source or ""),
            "exec_risk_ref_bps": float(self.exec_risk_ref_bps),
            "exec_risk_bps": float(self.exec_risk_bps),
            "exec_risk_norm": float(self.exec_risk_norm),
            "exec_pen": float(self.exec_pen),
            "score_breakdown_small": self.score_breakdown_small or {},
            "score_breakdown_json": str(self.score_breakdown_json or ""),
        }



class DecisionPolicyMixin:
    def _decide_util_mh(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        effective_mode: str | None = None,
    ) -> MLConfirmDecision:
        cfg = self._cfg
        model = self._model

        mode = effective_mode if effective_mode else self.mode
        dec = MLConfirmDecision(mode=mode, kind="util_mh_v1", allow=True)
        dec.model_run_id = (cfg.get("run_id", "") or "")
        dec.model_path = (cfg.get("model_path", "") or "")

        if model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            # Use detailed error reason if available, otherwise generic
            error_reason = self._model_load_error or "no_model_loaded"
            dec.reason = error_reason
            dec.error = error_reason
            dec.status = "ERR_NO_MODEL"
            # Explicitly set metrics to 0.0 for clarity and consistency
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.missing = []

            # Log the error for diagnostics (but not on every request to avoid spam)
            import logging
            logger = logging.getLogger("ml_confirm_gate")

            # Check if fallback was attempted
            fallback_info = ""
            if cfg.get("model_path_fallback_used"):
                original_path = cfg.get("model_path_original", "unknown")
                fallback_info = f" (fallback from {original_path} attempted but also failed)"

            if hasattr(self, '_last_error_log_ms'):
                now_ms = _now_ms()
                if now_ms - self._last_error_log_ms > 60000:  # Log at most once per minute
                    logger.error(
                        f"ML gate: Model not loaded for decision (symbol={symbol}, "
                        f"error={error_reason}, cfg_source={getattr(self, '_cfg_source', 'none')}, "
                        f"model_path={dec.model_path}{fallback_info})"
                    )
                    self._last_error_log_ms = now_ms
            else:
                logger.error(
                    f"ML gate: Model not loaded for decision (symbol={symbol}, "
                    f"error={error_reason}, cfg_source={getattr(self, '_cfg_source', 'none')}, "
                    f"model_path={dec.model_path}{fallback_info})"
                )
                self._last_error_log_ms = _now_ms()

            return dec

        x_row, missing = self._build_feature_row(model=model, indicators=indicators, direction=direction, scenario=scenario, ts_ms=ts_ms)
        dec.missing = missing

        # ENFORCE: если критические фичи реально отсутствуют -> fail-closed (точнее и безопаснее)
        if missing and mode == "ENFORCE":
            if self._abstain_on_missing:
                # selective: do not hard-block, let rule gate decide
                dec.allow = True
                dec.abstain = True
                dec.status = "ABSTAIN_MISSING_CRITICAL"
                dec.reason = f"ml_abstain_missing_critical({','.join(missing)})"
            else:
                dec.allow = False
                dec.status = "MISSING_CRITICAL_BLOCK"
                dec.reason = f"missing_critical({','.join(missing)})"
            dec.p_edge = 0.0
            dec.p_min = max(0.0, float(self._p_min_hard_floor))
            dec.p_margin = float(dec.p_edge - dec.p_min)
            dec.conf = self._conf_from_margin(dec.p_margin)
            dec.score = 0.0
            dec.floor = float(dec.p_min)
            return dec

        import numpy as np
        X = np.array([x_row], dtype=np.float32)

        util_pred = model.predict_util(X)  # dict[int]->ndarray
        unc = model.predict_unc(X)         # dict[int]->ndarray
        horizons: list[int] = list(getattr(model, "horizons", []) or list(util_pred.keys()))

        # Validate model outputs before processing
        if not horizons:
            dec.error = "no_horizons"
            dec.reason = "no_horizons(model_horizons_empty,util_pred_keys_empty)"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.score = 0.0
            dec.best_h_ms = 0
            dec.util_pred = {}
            dec.unc = {}
            dec.status = "ERR_NO_HORIZONS"
            return dec

        if not util_pred or not unc:
            dec.error = "empty_predictions"
            dec.reason = f"empty_predictions(util_pred={bool(util_pred)},unc={bool(unc)})"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.score = 0.0
            dec.best_h_ms = 0
            dec.util_pred = {}
            dec.unc = {}
            dec.status = "ERR_EMPTY_PREDICTIONS"
            return dec

        util_floors = cfg.get("util_floors") if isinstance(cfg.get("util_floors"), dict) else {}
        unc_k = _f(util_floors.get("unc_k", getattr(model, "unc_k", 0.5)), getattr(model, "unc_k", 0.5))

        best_h = 0
        best_score = -1e18
        util_pred_out: dict[str, float] = {}
        unc_out: dict[str, float] = {}
        scores_computed = False

        for h in horizons:
            if h not in util_pred or h not in unc:
                continue
            try:
                u = float(util_pred[h][0])
                un = float(unc[h][0])

                # Validate: check for NaN/Inf values
                if not (math.isfinite(u) and math.isfinite(un)):
                    import logging
                    logger = logging.getLogger("ml_confirm_gate")
                    logger.warning(f"ML gate: Non-finite prediction for horizon {h} (u={u}, unc={un})")
                    continue

                util_pred_out[str(h)] = u
                unc_out[str(h)] = un
                sc = u - unc_k * un

                # Validate computed score
                if not math.isfinite(sc):
                    import logging
                    logger = logging.getLogger("ml_confirm_gate")
                    logger.warning(f"ML gate: Non-finite score for horizon {h} (score={sc})")
                    continue

                if sc > best_score:
                    best_score = sc
                    best_h = int(h)
                    scores_computed = True
            except (IndexError, KeyError, TypeError, ValueError) as e:
                # Skip invalid predictions for this horizon, continue with others
                import logging
                logger = logging.getLogger("ml_confirm_gate")
                logger.warning(f"ML gate: Invalid prediction for horizon {h}: {e}")
                continue

        # Check if we actually computed any valid scores
        if not scores_computed or best_score <= -1e17:  # Still at initial value (with small tolerance for float precision)
            dec.error = "no_valid_scores"
            dec.reason = f"no_valid_scores(horizons={len(horizons)},computed={scores_computed},best_score={best_score:.2f})"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.score = float(best_score) if scores_computed else 0.0
            dec.best_h_ms = best_h
            dec.util_pred = util_pred_out
            dec.unc = unc_out
            dec.status = "ERR_NO_VALID_SCORES"
            bucket = _bucket_from_scenario(scenario)
            dec.bucket = bucket
            floor = _get_floor(util_floors, bucket)
            try:
                floor = max(float(floor), float(self._p_min_hard_floor))
            except Exception:
                floor = float(floor)
            dec.floor = float(floor)
            dec.allow = False  # No valid scores -> block
            return dec

        bucket = _bucket_from_scenario(scenario)
        floor = _get_floor(util_floors, bucket)
        # hard floor guardrail
        try:
            floor = max(float(floor), float(self._p_min_hard_floor))
        except Exception:
            floor = float(floor)

        dec.bucket = bucket
        dec.best_h_ms = best_h
        dec.score = float(best_score)
        dec.floor = float(floor)
        dec.util_pred = util_pred_out
        dec.unc = unc_out

        dec.allow = bool(best_score >= floor)

        # p_edge: convert utility score to probability before calibration
        # Utility scores can be negative/zero/positive, but calibrator expects [0,1]
        #
        # Solution: Use adaptive scaling based on the actual range of utility scores.
        # For very negative scores, we need more aggressive scaling to map them to a useful
        # probability range. We use a piecewise scaling approach:
        # - For scores in typical range [-5, 5]: scale by 2.5 (maps to [0.006, 0.994])
        # - For very negative scores (< -5): use more aggressive scaling to prevent all zeros
        # - For very positive scores (> 5): already near 1.0, less scaling needed

        def _sigmoid(x: float) -> float:
            """Stable sigmoid: 1 / (1 + exp(-x))"""
            if x >= 0:
                z = math.exp(-x)
                return 1.0 / (1.0 + z)
            z = math.exp(x)
            return z / (1.0 + z)

        # Adaptive scaling: more aggressive for very negative scores
        base_scale = float(self._cfg.get("p_edge_scale_factor", 2.5) or 2.5)

        if best_score < -5.0:
            # Very negative: use more aggressive scaling to prevent all zeros
            # Scale by 4x for scores < -5 to map them to at least ~0.001 range
            scale_factor = base_scale * 1.6  # 2.5 * 1.6 = 4.0
        elif best_score > 5.0:
            # Very positive: already near 1.0, less scaling needed
            scale_factor = base_scale * 0.8  # 2.5 * 0.8 = 2.0
        else:
            # Typical range [-5, 5]: use base scaling
            scale_factor = base_scale

        scaled_score = float(best_score) * scale_factor
        p_edge_from_score = _sigmoid(scaled_score)

        # Ensure minimum precision: if sigmoid produces a very small value, keep it for accuracy
        # but ensure it's not exactly 0.0 for valid scores (helps with diagnostics)
        if p_edge_from_score == 0.0 and best_score > -1e17:
            # This shouldn't happen with proper scaling, but add safety check
            # For very negative scores, ensure we get at least a tiny non-zero value
            p_edge_from_score = max(1e-6, _sigmoid(scaled_score * 1.1))

        # Store pre-calibration probability (not raw utility score)
        dec.p_edge_raw = float(p_edge_from_score)  # pre-calibration probability
        dec.p_edge_cal = float(p_edge_from_score)  # will be updated by calibrator if enabled
        dec.calib_type = str(self._calib_type or "none")

        calibrate = self._cfg.get("calibrate_p_edge", None)
        if calibrate is None:
            calibrate = True if self._calibrator is not None else False
        calibrate = bool(calibrate)

        if calibrate and self._calibrator is not None:
            # Now calibrate the probability (already in [0,1] range)
            dec.p_edge_cal = float(self._calibrator.apply_one(p_edge_from_score))

        # use calibrated p_edge for downstream thresholds/metrics
        dec.p_edge = float(dec.p_edge_cal)
        dec.p_min = float(floor)
        dec.p_margin = float(dec.p_edge - dec.p_min)
        dec.conf = self._conf_from_margin(dec.p_margin)
        dec.status = "ALLOW" if dec.allow else "BLOCK"
        dec.reason = f"util_mh(score={best_score:.4f},floor={floor:.4f},h={best_h},bucket={bucket})"

        return dec

    def _decide_edge_stack_v1(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        effective_mode: str | None = None,
    ) -> MLConfirmDecision:
        """
        Решение для edge_stack_v1: OOF stacking (LR + GBDT -> meta LR).
        
        Модель: dict-pack с ключами:
          - schema_version: 1
          - kind: "edge_stack_v1"
          - feature_cols: List[str]
          - lr: sklearn Pipeline (scaler + LR)
          - gbdt: CatBoostClassifier или HistGradientBoostingClassifier
          - meta: LogisticRegression
        
        Конфиг поддерживает:
          - p_min: глобальный порог (0..1)
          - p_min_by_bucket: {"trend": 0.55, "range": 0.60, "other": 0.50, "news": 0.65}
          - hard_p_min_floor: минимальный порог (fail-safe guardrail)
        """
        cfg = self._cfg
        model = self._model

        mode = effective_mode if effective_mode else self.mode
        dec = MLConfirmDecision(mode=mode, kind="edge_stack_v1", allow=True)
        dec.model_run_id = (cfg.get("run_id", "") or "")
        dec.model_path = (cfg.get("model_path", "") or "")

        if model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            error_reason = self._model_load_error or "no_model_loaded"
            dec.reason = error_reason
            dec.error = error_reason
            dec.status = "ERR_NO_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.missing = []
            return dec

        # Проверка структуры модели
        if not isinstance(model, dict):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "bad_model_format"
            dec.reason = f"bad_model_format(expected_dict,got={type(model).__name__})"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        if model.get("kind") != "edge_stack_v1":
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "bad_model_kind"
            dec.reason = f"bad_model_kind(expected=edge_stack_v1,got={model.get('kind')})"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        feature_cols = model.get("feature_cols", [])
        if not feature_cols:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "no_feature_cols"
            dec.reason = "no_feature_cols(model_missing_feature_cols)"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # Build features via a dict-pack view so optional transforms/scaler/buckets apply.
        view = _DictPackModelView(model)
        x_row, missing = self._build_feature_row(
            model=view,
            indicators=indicators,
            direction=direction,
            scenario=scenario,
            ts_ms=ts_ms
        )
        dec.missing = missing

        # ENFORCE: если критические фичи отсутствуют -> fail-closed
        if missing and mode == "ENFORCE":
            if self._abstain_on_missing:
                dec.allow = True
                dec.abstain = True
                dec.status = "ABSTAIN_MISSING_CRITICAL"
                dec.reason = f"ml_abstain_missing_critical({','.join(missing)})"
            else:
                dec.allow = False
                dec.status = "MISSING_CRITICAL_BLOCK"
                dec.reason = f"missing_critical({','.join(missing)})"
            dec.p_edge = 0.0
            dec.p_min = max(0.0, float(self._p_min_hard_floor))
            dec.p_margin = float(dec.p_edge - dec.p_min)
            dec.conf = self._conf_from_margin(dec.p_margin)
            dec.score = 0.0
            dec.floor = float(dec.p_min)
            return dec

        import numpy as np
        X = np.array([x_row], dtype=np.float32)

        # Получаем base модели
        lr_model = model.get("lr")
        gbdt_model = model.get("gbdt")
        meta_model = model.get("meta")

        if lr_model is None or gbdt_model is None or meta_model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "missing_base_models"
            dec.reason = f"missing_base_models(lr={lr_model is not None},gbdt={gbdt_model is not None},meta={meta_model is not None})"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # Предсказания base моделей
        try:
            p_lr = lr_model.predict_proba(X)[0, 1]  # вероятность класса 1
            p_gbdt = gbdt_model.predict_proba(X)[0, 1]
        except Exception as e:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "base_prediction_failed"
            dec.reason = f"base_prediction_failed({type(e).__name__}:{str(e)[:100]})"
            dec.status = "ERR_NON_FINITE"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # Проверка на NaN/Inf
        if not (math.isfinite(p_lr) and math.isfinite(p_gbdt)):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "non_finite_base_preds"
            dec.reason = f"non_finite_base_preds(lr={p_lr},gbdt={p_gbdt})"
            dec.status = "ERR_NON_FINITE"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # Meta предсказание
        try:
            Z = np.array([[p_lr, p_gbdt]], dtype=np.float32)
            p_edge_raw = meta_model.predict_proba(Z)[0, 1]
        except Exception as e:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "meta_prediction_failed"
            dec.reason = f"meta_prediction_failed({type(e).__name__}:{str(e)[:100]})"
            dec.status = "ERR_NON_FINITE"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        if not math.isfinite(p_edge_raw):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "non_finite_meta_pred"
            dec.reason = f"non_finite_meta_pred(p={p_edge_raw})"
            dec.status = "ERR_NON_FINITE"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # Калибровка (если включена)
        dec.p_edge_raw = float(np.clip(p_edge_raw, 0.0, 1.0))
        dec.p_edge_cal = float(dec.p_edge_raw)
        dec.calib_type = str(self._calib_type or "none")

        calibrate = self._cfg.get("calibrate_p_edge", None)
        if calibrate is None:
            calibrate = True if self._calibrator is not None else False
        calibrate = bool(calibrate)

        if calibrate and self._calibrator is not None:
            dec.p_edge_cal = float(self._calibrator.apply_one(dec.p_edge_raw))

        dec.p_edge = float(dec.p_edge_cal)

        # Определение bucket и p_min
        bucket = _bucket_from_scenario(scenario)
        dec.bucket = bucket

        # p_min из конфига: приоритет p_min_by_bucket, затем p_min, затем hard_p_min_floor
        # NOTE: Для edge_stack_v1 используется p_min (только на p_cal).
        # TODO: В будущем можно реализовать edge_floors как score_min (p_cal - unc_k*unc),
        #       чтобы учитывать uncertainty в пороге. Это потребует добавления uncertainty
        #       в модель edge_stack_v1 или использования отдельной uncertainty модели.
        p_min_by_bucket = cfg.get("p_min_by_bucket", {})
        if isinstance(p_min_by_bucket, dict) and bucket in p_min_by_bucket:
            p_min_cfg = float(p_min_by_bucket[bucket])
        else:
            p_min_cfg = float(cfg.get("p_min", 0.55))

        # hard_p_min_floor как guardrail
        hard_p_min_floor = float(cfg.get("hard_p_min_floor", 0.0))
        with contextlib.suppress(Exception):
            hard_p_min_floor = max(float(hard_p_min_floor), float(self._p_min_hard_floor))

        p_min = max(p_min_cfg, hard_p_min_floor)
        p_min = max(0.0, min(1.0, p_min))  # clamp to [0, 1]

        dec.p_min = float(p_min)
        dec.floor = float(p_min)  # для совместимости
        dec.p_margin = float(dec.p_edge - dec.p_min)
        dec.conf = self._conf_from_margin(dec.p_margin)

        # Решение
        dec.allow = bool(dec.p_edge >= dec.p_min)
        dec.status = "ALLOW" if dec.allow else "BLOCK"
        dec.reason = f"edge_stack_v1(p_edge={dec.p_edge:.4f},p_min={dec.p_min:.4f},bucket={bucket})"

        return dec

    def _decide_edge_stack_mh(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        effective_mode: str | None = None,
    ) -> MLConfirmDecision:
        """
        Решение для edge_stack_mh_v1: multi-horizon stacking с uncertainty.
        
        Модель: EdgeStackMHModelV1
          - p_lr[h], p_gbdt[h] -> p_meta[h] -> p_cal[h]
          - unc[h] = |p_lr[h] - p_gbdt[h]|
          - score[h] = p_cal[h] - unc_k * unc[h]
          - best_h = argmax_h(score[h])
          - allow if best_score >= edge_floors[bucket].floor
        """
        cfg = self._cfg
        model = self._model

        mode = effective_mode if effective_mode else self.mode
        dec = MLConfirmDecision(mode=mode, kind="edge_stack_mh_v1", allow=True)
        dec.model_run_id = (cfg.get("run_id", "") or "")
        dec.model_path = (cfg.get("model_path", "") or "")

        if model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            error_reason = self._model_load_error or "no_model_loaded"
            dec.reason = error_reason
            dec.error = error_reason
            dec.status = "ERR_NO_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.missing = []
            return dec

        # Проверка типа модели
        if not isinstance(model, EdgeStackMHModelV1):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "bad_model_type"
            dec.reason = f"bad_model_type(expected=EdgeStackMHModelV1,got={type(model).__name__})"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # P0 fix: для edge_stack_mh_v1 модель - это объект EdgeStackMHModelV1,
        # который уже имеет все нужные атрибуты (feature_cols, feature_transforms, robust_scaler, etc.)
        # поэтому передаём его напрямую (не создаём temp_model)
        x_row, missing = self._build_feature_row(
            model=model,  # НЕ temp_model - используем реальный объект модели
            indicators=indicators,
            direction=direction,
            scenario=scenario,
            ts_ms=ts_ms
        )
        dec.missing = missing

        # ENFORCE: если критические фичи отсутствуют -> fail-closed
        if missing and mode == "ENFORCE":
            if self._abstain_on_missing:
                dec.allow = True
                dec.abstain = True
                dec.status = "ABSTAIN_MISSING_CRITICAL"
                dec.reason = f"ml_abstain_missing_critical({','.join(missing)})"
            else:
                dec.allow = False
                dec.status = "MISSING_CRITICAL_BLOCK"
                dec.reason = f"missing_critical({','.join(missing)})"
            dec.p_edge = 0.0
            dec.p_min = max(0.0, float(self._p_min_hard_floor))
            dec.p_margin = float(dec.p_edge - dec.p_min)
            dec.conf = self._conf_from_margin(dec.p_margin)
            dec.score = 0.0
            dec.floor = float(dec.p_min)
            return dec

        import numpy as np
        X = np.array([x_row], dtype=np.float32)

        # Предсказания модели
        try:
            p_cal_dict = model.predict_p_cal(X)  # Dict[int, np.ndarray]
            unc_dict = model.predict_unc(X)      # Dict[int, np.ndarray]
            score_dict = model.predict_score(X)  # Dict[int, np.ndarray]
        except Exception as e:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "prediction_failed"
            dec.reason = f"prediction_failed({type(e).__name__}:{str(e)[:100]})"
            dec.status = "ERR_PREDICTION"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        horizons = model.horizons
        if not horizons:
            dec.error = "no_horizons"
            dec.reason = "no_horizons(model_horizons_empty)"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.score = 0.0
            dec.best_h_ms = 0
            dec.status = "ERR_NO_HORIZONS"
            return dec

        # Выбираем лучший горизонт по score
        best_h = 0
        best_score = -1e18
        best_p_cal = 0.0
        best_unc = 0.0

        for h in horizons:
            if h not in score_dict or h not in p_cal_dict or h not in unc_dict:
                continue
            try:
                sc = float(score_dict[h][0])
                p_cal = float(p_cal_dict[h][0])
                unc = float(unc_dict[h][0])

                if not (math.isfinite(sc) and math.isfinite(p_cal) and math.isfinite(unc)):
                    continue

                if sc > best_score:
                    best_score = sc
                    best_h = int(h)
                    best_p_cal = p_cal
                    best_unc = unc
            except (IndexError, KeyError, TypeError, ValueError):
                continue

        if best_score <= -1e17:
            dec.error = "no_valid_scores"
            dec.reason = f"no_valid_scores(horizons={len(horizons)})"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.score = 0.0
            dec.best_h_ms = best_h
            dec.status = "ERR_NO_VALID_SCORES"
            bucket = _bucket_from_scenario(scenario)
            dec.bucket = bucket
            floor = _get_floor(cfg.get("edge_floors", {}), bucket)
            try:
                floor = max(float(floor), float(self._p_min_hard_floor))
            except Exception:
                floor = float(floor)
            dec.floor = float(floor)
            dec.allow = False
            return dec

        # Определение bucket и floor
        bucket = _bucket_from_scenario(scenario)
        dec.bucket = bucket
        edge_floors = cfg.get("edge_floors", {})
        floor = _get_floor(edge_floors, bucket)
        try:
            floor = max(float(floor), float(self._p_min_hard_floor))
        except Exception:
            floor = float(floor)

        dec.best_h_ms = best_h
        dec.score = float(best_score)
        dec.floor = float(floor)

        # p_edge: используем p_cal лучшего горизонта
        dec.p_edge_raw = float(best_p_cal)
        dec.p_edge_cal = float(best_p_cal)
        dec.calib_type = "platt_logit"  # модель уже калибрована

        # use calibrated p_edge for downstream thresholds/metrics
        dec.p_edge = float(dec.p_edge_cal)
        dec.p_min = float(floor)
        dec.p_margin = float(dec.p_edge - dec.p_min)
        dec.conf = self._conf_from_margin(dec.p_margin)

        # Решение: allow if best_score >= floor
        dec.allow = bool(best_score >= floor)
        dec.status = "ALLOW" if dec.allow else "BLOCK"
        dec.reason = f"edge_stack_mh(score={best_score:.4f},floor={floor:.4f},h={best_h},bucket={bucket},unc={best_unc:.4f})"

        # Сохраняем uncertainty для метрик
        dec.unc = {str(best_h): float(best_unc)}

        return dec

    def _decide_meta_lr(
        self,
        *,
        symbol: str,
        ts_ms: int,
        direction: str,
        scenario: str,
        indicators: dict[str, Any],
        effective_mode: str | None = None,
    ) -> MLConfirmDecision:
        """Decision logic for simple MetaModelLR (logistic regression)."""
        cfg = self._cfg
        model = self._model

        mode = effective_mode if effective_mode else self.mode
        dec = MLConfirmDecision(mode=mode, kind="meta_lr", allow=True)
        dec.model_run_id = (cfg.get("run_id", "") or "")
        dec.model_path = (cfg.get("model_path", "") or "")

        if model is None:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            error_reason = self._model_load_error or "no_model_loaded"
            dec.reason = error_reason
            dec.error = error_reason
            dec.status = "ERR_NO_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            dec.missing = []
            return dec

        if not isinstance(model, MetaModelLR):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "bad_model_type"
            dec.reason = f"bad_model_type(expected=MetaModelLR,got={type(model).__name__})"
            dec.status = "ERR_BAD_MODEL"
            dec.p_edge = 0.0
            dec.p_min = 0.0
            dec.p_margin = 0.0
            dec.conf = 0.0
            return dec

        # P0 fix: MetaModelLR использует 'features' вместо 'feature_cols',
        # но имеет transforms и robust_scaler, которые нужно прокинуть в _build_feature_row
        class _MetaModelView:
            def __init__(self, meta_model: MetaModelLR):
                self.feature_cols = meta_model.features  # маппинг features -> feature_cols
                self.feature_transforms = getattr(meta_model, "transforms", {}) or {}
                self.robust_scaler = getattr(meta_model, "robust_scaler", None)
                # для session/spread/liq используем defaults (MetaModelLR обычно не имеет этих cfg)
                self.session_cfg = {}
                self.spread_bucket_edges = None
                self.liq_cfg = {}

        view = _MetaModelView(model)
        x_row, missing = self._build_feature_row(
            model=view,
            indicators=indicators,
            direction=direction,
            scenario=scenario,
            ts_ms=ts_ms
        )
        dec.missing = missing

        # ENFORCE missing check
        if missing and mode == "ENFORCE":
            if self._abstain_on_missing:
                dec.allow = True
                dec.abstain = True
                dec.status = "ABSTAIN_MISSING_CRITICAL"
                dec.reason = f"ml_abstain_missing_critical({','.join(missing)})"
            else:
                dec.allow = False
                dec.status = "MISSING_CRITICAL_BLOCK"
                dec.reason = f"missing_critical({','.join(missing)})"
            dec.p_edge = 0.0
            dec.p_min = max(0.0, float(self._p_min_hard_floor))
            dec.p_margin = float(dec.p_edge - dec.p_min)
            dec.conf = self._conf_from_margin(dec.p_margin)
            dec.score = 0.0
            dec.floor = float(dec.p_min)
            return dec

        # Predict
        # construct feat dict from row? No, predict_proba expects dict?
        # MetaModelLR.predict_proba expects Dict[str, Any]
        # BUT _build_feature_row returns List[float] for feature_cols.
        # This is inefficient: we built list, now need to rebuild dict or unsafe existing methods.
        # Actually MetaModelLR.predict_proba iterates over self.features and does lookups.
        # So we can just pass indicators directly?
        # _build_feature_row handles critical checks and derived features (like spread_bucket).
        # MetaModelLR *might* depend on derived features.
        # Let's inspect MetaModelLR.predict_proba again.
        # It calls _f(feat.get(name, 0.0)).

        # If model.features includes "spread_bucket_..." or "session_...", we need those derived.
        # _build_feature_row logic is complex and handles derivation.
        # Ideally we should refactor, but for now let's construct a feat dict from the row we just built.

        feat_dict = {}
        for i, col in enumerate(model.features):
            feat_dict[col] = x_row[i]

        try:
            p_edge_raw = model.predict_proba(feat_dict)
        except Exception as e:
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "prediction_failed"
            dec.reason = f"prediction_failed({str(e)[:100]})"
            dec.status = "ERR_PRED"
            return dec

        if not math.isfinite(p_edge_raw):
            dec.mode = "ERR"
            dec.allow = self._fail_allow()
            dec.error = "non_finite_pred"
            dec.reason = f"non_finite_pred({p_edge_raw})"
            dec.status = "ERR_NON_FINITE"
            return dec

        dec.p_edge_raw = float(p_edge_raw)
        dec.p_edge_cal = float(p_edge_raw)
        dec.calib_type = str(self._calib_type or "none")

        # Optional calibration
        calibrate = self._cfg.get("calibrate_p_edge", None)
        if calibrate is None:
            calibrate = True if self._calibrator is not None else False
        if bool(calibrate) and self._calibrator is not None:
             dec.p_edge_cal = float(self._calibrator.apply_one(dec.p_edge_raw))

        dec.p_edge = float(dec.p_edge_cal)

        # Determine p_min
        bucket = _bucket_from_scenario(scenario)
        dec.bucket = bucket

        # p_min from config
        p_min_by_bucket = cfg.get("util_floors", {}).get("by_bucket", {})
        # Flatten structure if needed or just use what we stored in init_ml... (util_floors.by_bucket.{bucket}.floor)
        # Note: init_ml_confirm_on_startup sets structure: util_floors.by_bucket.trend.floor = 0.55
        # So we can traverse that.

        floor = 0.55 # default
        try:
            uf = cfg.get("util_floors", {})
            if isinstance(uf, dict):
                bb = uf.get("by_bucket", {})
                if isinstance(bb, dict) and bucket in bb:
                    floor = float(bb[bucket].get("floor", 0.55))
                else:
                    g = uf.get("global", {})
                    floor = float(g.get("floor", 0.55))
        except Exception:
            pass

        # guardrail
        with contextlib.suppress(Exception):
            floor = max(float(floor), float(self._p_min_hard_floor))

        dec.p_min = float(floor)
        dec.floor = float(floor)
        dec.p_margin = float(dec.p_edge - dec.p_min)
        dec.conf = self._conf_from_margin(dec.p_margin)

        dec.allow = bool(dec.p_edge >= dec.p_min)
        dec.status = "ALLOW" if dec.allow else "BLOCK"
        dec.reason = f"meta_lr(p={dec.p_edge:.4f},thr={dec.p_min:.4f},bucket={bucket})"

        return dec

