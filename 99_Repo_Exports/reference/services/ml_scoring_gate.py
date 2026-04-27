# -*- coding: utf-8 -*-
"""
ML Scoring Gate — runtime inference for ML Scorer V2.

Provides MLScoringGate: a fail-open drop-in replacement for rule-based ConfidenceScorer.
Loads a trained LightGBM model (scorer_v2.joblib) and predicts R-multiple,
then calibrates to conf01 (0..1) via isotonic regression.

Design:
  - Lazy model load on first call
  - Periodic refresh (ML_SCORER_V2_REFRESH_MS, default 60s)
  - Fail-open: if model unavailable → returns None → caller uses rule-based fallback
  - Same interface as ConfidenceScorer.score(): returns (conf01, parts_dict)
"""
from __future__ import annotations

import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("ml_scoring_gate")


def _f(obj: Any, name: str, default: float = 0.0) -> float:
    """Safe float accessor from object attribute."""
    try:
        v = getattr(obj, name, default)
        if v is None:
            return float(default)
        x = float(v)
        return x if math.isfinite(x) else float(default)
    except Exception:
        return float(default)


def _f_any(obj: Any, *names: str, default: float = 0.0) -> float:
    """Return first available finite float attribute among names."""
    for n in names:
        try:
            v = getattr(obj, n)
            if v is None:
                continue
            x = float(v)
            if math.isfinite(x):
                return x
        except Exception:
            continue
    return float(default)


def _dir_sign_from_side(side: str) -> int:
    s = (side or "").upper()
    if s == "LONG":
        return 1
    if s == "SHORT":
        return -1
    return 0


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        v = float(x)
    except Exception:
        return lo
    if not math.isfinite(v):
        return lo
    return max(lo, min(hi, v))


# Feature names — must match train_ml_scorer.py NUMERIC_FEATURES + DERIVED_FEATURES
_NUMERIC_FEATURE_ATTRS = [
    ("conf_score", ["conf_score", "confidence"]),
    ("atr_14", ["atr_14", "atr"]),
    ("delta_spike_z", ["delta_spike_z", "delta_z", "z_delta"]),
    ("obi_avg_20", ["obi_avg_20", "obi_avg", "obi"]),
    ("weak_progress_ratio", ["weak_progress_ratio", "weak_progress"]),
    ("l3_spread_bps", ["l3_spread_bps", "spread_bps"]),
    ("l3_microprice_shift_bps_20", ["l3_microprice_shift_bps_20", "microprice_shift_bps_20"]),
    ("l3_microprice_velocity_bps", ["l3_microprice_velocity_bps", "microprice_velocity_bps"]),
    ("l3_obi_5", ["l3_obi_5", "obi_5"]),
    ("l3_obi_20", ["l3_obi_20", "obi_20"]),
    ("l3_obi_50", ["l3_obi_50", "obi_50"]),
    ("l3_obi_persistence_score", ["l3_obi_persistence_score", "obi_persistence_score"]),
    ("l3_cancel_to_trade_bid_5s", ["l3_cancel_to_trade_bid_5s", "cancel_to_trade_bid_5s", "cancel_to_trade_bid"]),
    ("l3_cancel_to_trade_ask_5s", ["l3_cancel_to_trade_ask_5s", "cancel_to_trade_ask_5s", "cancel_to_trade_ask"]),
    ("l3_cancel_to_trade_bid_20s", ["l3_cancel_to_trade_bid_20s", "cancel_to_trade_bid_20s"]),
    ("l3_cancel_to_trade_ask_20s", ["l3_cancel_to_trade_ask_20s", "cancel_to_trade_ask_20s"]),
    ("l3_queue_pressure_bid", ["l3_queue_pressure_bid", "queue_pressure_bid"]),
    ("l3_queue_pressure_ask", ["l3_queue_pressure_ask", "queue_pressure_ask"]),
    ("l3_market_depth_imbalance", ["l3_market_depth_imbalance", "market_depth_imbalance"]),
]


class MLScoringGate:
    """ML-based confidence scorer — controlled by USE_UNIFIED_SCORING flag.

    Fail-open design: if model is unavailable, score() returns (None, {}).
    The caller (ConfidenceScorer) should fall back to rule-based scoring.
    """

    def __init__(
        self,
        *,
        model_path: str = "",
        refresh_ms: int = 0,
    ) -> None:
        self._model_path = model_path or os.getenv(
            "ML_SCORER_V2_MODEL_PATH",
            "/var/lib/trade/ml_models/scorer_v2/scorer_v2.joblib",
        )
        self._refresh_ms = refresh_ms or int(
            os.getenv("ML_SCORER_V2_REFRESH_MS", "60000")
        )

        self._pack: Optional[Dict[str, Any]] = None
        self._model: Any = None
        self._feature_names: List[str] = []
        self._scaler_params: Dict[str, Dict[str, float]] = {}
        self._calibrator: Any = None
        self._last_load_ms: int = 0
        self._load_attempts: int = 0
        self._load_failures: int = 0

    # ------------------------------------------------------------------
    # Model lifecycle
    # ------------------------------------------------------------------

    def _should_load(self) -> bool:
        now_ms = int(time.time() * 1000)
        if self._pack is None:
            # First load: try every 10s until success
            return (now_ms - self._last_load_ms) > 10_000
        return (now_ms - self._last_load_ms) > self._refresh_ms

    def _try_load(self) -> bool:
        """Attempt to load model from disk. Returns True if successful."""
        self._last_load_ms = int(time.time() * 1000)
        self._load_attempts += 1

        try:
            import joblib as _jl

            if not os.path.isfile(self._model_path):
                if self._load_failures == 0:
                    logger.info(
                        "ML scorer model not found at %s — rule-based fallback",
                        self._model_path,
                    )
                self._load_failures += 1
                return False

            pack = _jl.load(self._model_path)
            if not isinstance(pack, dict):
                logger.error("Model pack is not a dict — ignoring")
                self._load_failures += 1
                return False

            kind = pack.get("kind", "")
            if kind != "ml_scorer_v2":
                logger.error("Unexpected model kind: %s (expected ml_scorer_v2)", kind)
                self._load_failures += 1
                return False

            self._pack = pack
            self._model = pack.get("model")
            self._feature_names = list(pack.get("feature_names", []))
            self._scaler_params = dict(pack.get("robust_scaler_params", {}))
            self._calibrator = pack.get("calibrator")
            self._load_failures = 0

            metrics = pack.get("metrics", {})
            logger.info(
                "✅ ML scorer loaded: samples=%d MAE=%.4f R²=%.4f Spearman=%.4f",
                pack.get("n_samples", 0),
                metrics.get("mae_oof", -1),
                metrics.get("r2_oof", -1),
                metrics.get("spearman_oof", -1),
            )
            return True

        except Exception as e:
            logger.error("Failed to load ML scorer: %s", e)
            self._load_failures += 1
            return False

    def _ensure_model(self) -> bool:
        """Ensure model is loaded (lazy load + refresh)."""
        if self._should_load():
            self._try_load()
        return self._model is not None

    # ------------------------------------------------------------------
    # Feature extraction (from live ctx object → feature vector)
    # ------------------------------------------------------------------

    def _extract_features(self, ctx: Any, side: str) -> Optional[List[float]]:
        """Extract feature vector from live context — matches train_ml_scorer.py order."""
        try:
            out: List[float] = []

            # Numeric features (same order as NUMERIC_FEATURES in trainer)
            for _name, attr_names in _NUMERIC_FEATURE_ATTRS:
                out.append(_f_any(ctx, *attr_names, default=0.0))

            # Derived features
            dir_sign = _dir_sign_from_side(side)
            out.append(1.0 if dir_sign > 0 else 0.0)  # direction_long

            c2t_vals = [
                _f_any(ctx, "l3_cancel_to_trade_bid_5s", "cancel_to_trade_bid_5s", "cancel_to_trade_bid", default=0.0),
                _f_any(ctx, "l3_cancel_to_trade_ask_5s", "cancel_to_trade_ask_5s", "cancel_to_trade_ask", default=0.0),
                _f_any(ctx, "l3_cancel_to_trade_bid_20s", "cancel_to_trade_bid_20s", default=0.0),
                _f_any(ctx, "l3_cancel_to_trade_ask_20s", "cancel_to_trade_ask_20s", default=0.0),
            ]
            out.append(max(c2t_vals))  # cancel_to_trade_max

            obi_5 = _f_any(ctx, "l3_obi_5", "obi_5", default=0.0)
            obi_50 = _f_any(ctx, "l3_obi_50", "obi_50", default=0.0)
            out.append(obi_5 - obi_50)  # obi_spread

            qp_bid = _f_any(ctx, "l3_queue_pressure_bid", "queue_pressure_bid", default=0.0)
            qp_ask = _f_any(ctx, "l3_queue_pressure_ask", "queue_pressure_ask", default=0.0)
            out.append(qp_bid - qp_ask)  # queue_imbalance

            return out

        except Exception as e:
            logger.error("Feature extraction failed: %s", e)
            return None

    def _scale_features(self, raw: List[float]) -> List[float]:
        """Apply robust scaler to feature vector."""
        import numpy as _np

        arr = _np.array(raw, dtype=_np.float64)
        for i, name in enumerate(self._feature_names):
            if name in self._scaler_params:
                c = self._scaler_params[name]["center"]
                s = max(self._scaler_params[name]["scale"], 1e-12)
                arr[i] = (arr[i] - c) / s
        return arr.tolist()

    # ------------------------------------------------------------------
    # Inference
    # ------------------------------------------------------------------

    def _predict_r(self, features: List[float]) -> Optional[float]:
        """Run model inference → predicted R-multiple."""
        try:
            import numpy as _np

            x = _np.array([features], dtype=_np.float64)
            pred = self._model.predict(x)
            v = float(pred[0])
            return v if math.isfinite(v) else None
        except Exception as e:
            logger.error("Model predict failed: %s", e)
            return None

    def _calibrate_to_conf01(self, predicted_r: float) -> float:
        """Map predicted R-multiple → conf01 (0..1)."""
        if self._calibrator is not None:
            try:
                import numpy as _np

                p = self._calibrator.predict([predicted_r])
                return _clamp(float(p[0]), 0.05, 0.98)
            except Exception:
                pass

        # Fallback: sigmoid mapping
        #   R = 0 → conf = 0.50
        #   R = 1 → conf ≈ 0.73
        #   R = 2 → conf ≈ 0.88
        try:
            return _clamp(1.0 / (1.0 + math.exp(-predicted_r)), 0.05, 0.98)
        except Exception:
            return 0.50

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def score(
        self,
        *,
        kind: str = "",
        side: str = "",
        ctx: Any = None,
    ) -> Tuple[Optional[float], Dict[str, Any]]:
        """Score a signal using ML model.

        Returns:
            (conf01, parts) if model available.
            (None, {}) if model unavailable — caller should fallback to rule-based.
        """
        parts: Dict[str, Any] = {"scorer": "ml_v2"}

        if not self._ensure_model():
            parts["ml_status"] = "model_unavailable"
            return None, parts

        if ctx is None:
            parts["ml_status"] = "no_context"
            return None, parts

        # Extract features
        raw_features = self._extract_features(ctx, side)
        if raw_features is None:
            parts["ml_status"] = "feature_extraction_failed"
            return None, parts

        # Scale
        scaled_features = self._scale_features(raw_features)

        # Predict R-multiple
        predicted_r = self._predict_r(scaled_features)
        if predicted_r is None:
            parts["ml_status"] = "predict_failed"
            return None, parts

        # Calibrate to conf01
        conf01 = self._calibrate_to_conf01(predicted_r)

        # Model metadata
        model_age_ms = int(time.time() * 1000) - int(
            (self._pack or {}).get("trained_at_ms", 0)
        )

        parts.update({
            "ml_status": "ok",
            "ml_predicted_r": predicted_r,
            "ml_conf01": conf01,
            "ml_model_age_ms": model_age_ms,
            "ml_kind": kind,
        })

        return conf01, parts

    @property
    def is_loaded(self) -> bool:
        return self._model is not None

    @property
    def model_metrics(self) -> Dict[str, Any]:
        if self._pack is None:
            return {}
        return dict(self._pack.get("metrics", {}))
