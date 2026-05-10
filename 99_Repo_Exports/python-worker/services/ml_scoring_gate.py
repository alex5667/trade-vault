from __future__ import annotations

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
import logging
import math
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("ml_scoring_gate")


def _f(obj: Any, name: str, default: float = 0.0) -> float:
    """Safe float accessor from object attribute OR dict key."""
    try:
        if isinstance(obj, dict):
            v = obj.get(name, default)
        else:
            v = getattr(obj, name, default)
        if v is None:
            return default
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _fd(d: dict, name: str, default: float = 0.0) -> float:
    """Safe float accessor from dict key."""
    try:
        v = d.get(name, default)
        if v is None:
            return default
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Legacy Feature Schema V2 (17 raw + 6 derived = 23 features)
# Used by scorer_v2/v3 .joblib files trained with train_ml_scorer*.py
# ---------------------------------------------------------------------------
_NUMERIC_FEATURE_ATTRS: list[str] = [
    "atr_14",
    "obi_avg_20",
    "weak_progress_ratio",
    "l3_spread_bps",
    "l3_microprice_shift_bps_20",
    "l3_microprice_velocity_bps",
    "l3_obi_5",
    "l3_obi_20",
    "l3_obi_50",
    "l3_obi_persistence_score",
    "l3_cancel_to_trade_bid_5s",
    "l3_cancel_to_trade_ask_5s",
    "l3_cancel_to_trade_bid_20s",
    "l3_cancel_to_trade_ask_20s",
    "l3_queue_pressure_bid",
    "l3_queue_pressure_ask",
    "l3_market_depth_imbalance",
]


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
    return default


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


# Hardcoded feature names and hashes removed to support dynamic schema loading


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
        # V3 path takes priority; fall back to V2 legacy var; final default = V3
        self._model_path = model_path or (
            os.getenv("ML_SCORER_V3_MODEL_PATH")
            or os.getenv("ML_SCORER_V2_MODEL_PATH")
            or "/var/lib/trade/ml_models/scorer_v3/scorer_v3.joblib"
        )
        self._refresh_ms = refresh_ms or int(
            os.getenv("ML_SCORER_V2_REFRESH_MS", "60000")
        )
        # Phase 4.1: ML acceptance threshold
        raw_thr = os.getenv("ML_CONFIDENCE_THRESHOLD", "").strip()
        self._threshold: float | None = None
        if raw_thr:
            try:
                t = float(raw_thr)
                if 0.0 <= t <= 1.0 and math.isfinite(t):
                    self._threshold = t
                else:
                    logger.warning(
                        "ML_CONFIDENCE_THRESHOLD=%r out of [0,1] — ignored",
                        raw_thr,
                    )
            except ValueError:
                logger.warning(
                    "ML_CONFIDENCE_THRESHOLD=%r not a float — ignored",
                    raw_thr,
                )

        # [NEW] Throttle for mtime checks (reduce syscalls)
        self._mtime_check_interval_ms = 30_000
        self._last_mtime_check_ms = 0
        self._last_mtime = 0.0

        self._pack: dict[str, Any] | None = None
        self._model: Any = None
        self._feature_names: list[str] = []
        self._scaler_params: dict[str, dict[str, float]] = {}
        self._calibrator: Any = None
        self._last_load_ms: int = 0
        self._load_attempts: int = 0
        self._load_failures: int = 0

    # ------------------------------------------------------------------
    # Model lifecycle
    # ------------------------------------------------------------------

    def _should_load(self) -> bool:
        now_ms = get_ny_time_millis()
        if self._pack is None:
            # First load: try every 10s until success
            return (now_ms - self._last_load_ms) > 10_000

        if (now_ms - self._last_load_ms) > self._refresh_ms:
            # [REMEDIATION P4.1] Throttle mtime check to reduce syscall pressure
            if (now_ms - self._last_mtime_check_ms) < self._mtime_check_interval_ms:
                return False

            self._last_mtime_check_ms = now_ms
            try:
                mtime = os.path.getmtime(self._model_path)
                if self._last_mtime == mtime:
                    self._last_load_ms = now_ms
                    return False
                self._last_mtime = mtime
            except Exception:
                pass
            return True
        return False

    def _try_load(self) -> bool:
        """Attempt to load model from disk. Returns True if successful."""
        self._last_load_ms = get_ny_time_millis()
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
            if kind not in ("ml_scorer_v2", "ml_scorer_v3"):
                logger.error("Unexpected model kind: %s (expected ml_scorer_v2 or ml_scorer_v3)", kind)
                self._load_failures += 1
                return False

            self._pack = pack
            self._model = pack.get("model")
            self._feature_names = list(pack.get("feature_names", []))
            self._scaler_params = dict(pack.get("robust_scaler_params", {}))
            self._calibrator = pack.get("calibrator")
            self._load_failures = 0

            # Extract schema properties — support both old and new key names
            self._feature_schema_version = pack.get(
                "feature_schema_version", pack.get("schema_version", 0)
            )
            self._feature_schema_ver = (pack.get("feature_schema_ver", "")).lower().strip()
            self._feature_hash = pack.get("feature_hash", "")

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

    def _extract_features(self, ctx: Any, side: str) -> list[float] | None:
        """Extract feature vector from live context dynamically based on model schema."""
        try:
            ts_ms = getattr(ctx, "ts_ms", 0)
            if not ts_ms and isinstance(ctx, dict):
                ts_ms = ctx.get("ts_ms", 0)

            scenario = getattr(ctx, "scenario", "none")
            if not scenario and isinstance(ctx, dict):
                scenario = ctx.get("scenario", "none")

            cancel_spike_veto = getattr(ctx, "cancel_spike_veto", False)
            if not cancel_spike_veto and isinstance(ctx, dict):
                cancel_spike_veto = ctx.get("cancel_spike_veto", False)

            if hasattr(ctx, "indicators") and isinstance(ctx.indicators, dict):
                ind = ctx.indicators
            elif isinstance(ctx, dict) and "indicators" in ctx:
                ind = ctx["indicators"]
            else:
                ind = ctx if isinstance(ctx, dict) else vars(ctx) if hasattr(ctx, "__dict__") else {}

            sv = str(getattr(self, "_feature_schema_version", "3")).lower()
            sv_tag = getattr(self, "_feature_schema_ver", "").lower().strip()
            n_model_features = len(self._feature_names)

            # Routing: sv_tag ALWAYS takes priority over numeric sv.
            # Exception: if the model's actual feature count matches legacy v2 (23),
            # use v2 extractor regardless of sv_tag — train/serve parity takes precedence.
            if n_model_features == len(_NUMERIC_FEATURE_ATTRS) + 6:
                # 23-feature model → always use legacy v2 extractor
                sv = "2"
            elif sv_tag in ("v4_of", "v4"):
                sv = "4"
            elif sv_tag in ("v5_of", "v5"):
                sv = "5"
            elif sv_tag in (
                "v6_of", "v6", "v7_of", "v7", "v7_of_stable",
                "v9_of", "v9", "v10_of", "v10",
                "v11_of", "v11", "v12_of", "v12", "v13_of", "v13",
            ):
                sv = "registry"
            elif sv_tag in ("v3_unified",):
                sv = "v3_unified"
            elif sv_tag in ("v3", "3"):
                sv = "3"
            elif sv_tag in ("v2", "2"):
                sv = "2"
            # else: numeric sv used as-is

            if sv == "registry":
                # Generic vectorizer via feature_registry
                try:
                    from core.feature_registry import get_schema_info
                    schema_info = get_schema_info(sv_tag)
                    vec: list[float] = []
                    for feat_name in schema_info.feature_names:
                        if feat_name.startswith(("n:", "b:")):
                            key = feat_name[2:]
                            vec.append(_fd(ind, key, 0.0))
                        elif feat_name == "dir:LONG":
                            vec.append(1.0 if _dir_sign_from_side(side) > 0 else 0.0)
                        elif feat_name == "dir:SHORT":
                            vec.append(1.0 if _dir_sign_from_side(side) < 0 else 0.0)
                        elif feat_name.startswith("bucket:"):
                            bucket = feat_name[len("bucket:"):]
                            vec.append(1.0 if (scenario or "").lower() == bucket else 0.0)
                        elif feat_name.startswith("hour:"):
                            import datetime as _dt
                            try:
                                dt = _dt.datetime.fromtimestamp(int(ts_ms) / 1000.0, _dt.timezone.utc)
                                vec.append(1.0 if dt.hour == int(feat_name[5:]) else 0.0)
                            except Exception:
                                vec.append(0.0)
                        elif feat_name.startswith("dow:"):
                            import datetime as _dt
                            try:
                                dt = _dt.datetime.fromtimestamp(int(ts_ms) / 1000.0, _dt.timezone.utc)
                                vec.append(1.0 if dt.weekday() == int(feat_name[4:]) else 0.0)
                            except Exception:
                                vec.append(0.0)
                        else:
                            vec.append(0.0)
                    return vec
                except Exception as e:
                    logger.error("Registry vectorizer failed for %s: %s", sv_tag, e)
                    return None

            elif sv in ("2", "v2"):
                # Legacy 23-feature schema (17 raw + 6 derived)
                out: list[float] = [_fd(ind, attr) for attr in _NUMERIC_FEATURE_ATTRS]
                out.append(1.0 if _dir_sign_from_side(side) > 0 else 0.0)  # direction_long
                c2t = [
                    _fd(ind, "l3_cancel_to_trade_bid_5s"),
                    _fd(ind, "l3_cancel_to_trade_ask_5s"),
                    _fd(ind, "l3_cancel_to_trade_bid_20s"),
                    _fd(ind, "l3_cancel_to_trade_ask_20s"),
                ]
                out.append(max(c2t))  # cancel_to_trade_max
                out.append(_fd(ind, "l3_obi_5") - _fd(ind, "l3_obi_50"))  # obi_spread
                out.append(_fd(ind, "l3_queue_pressure_bid") - _fd(ind, "l3_queue_pressure_ask"))  # queue_imbalance
                outlier_count = sum(1.0 for v in out if abs(v) > 10.0)
                out.append(outlier_count)
                out.append(1.0 if outlier_count > 0 else 0.0)
                return out

            elif sv == "v3_unified":
                out: list[float] = []
                # 1. Legacy features
                for attr in ["atr_14", "obi_avg_20", "weak_progress_ratio"]:
                    if attr == "obi_avg_20":
                        out.append(_fd(ind, "obi_avg_20", _fd(ind, "obi_20", 0.0)))
                    else:
                        out.append(_fd(ind, attr))

                # 2. L3 features
                for attr in [
                    "l3_spread_bps", "l3_microprice_shift_bps_20", "l3_microprice_velocity_bps",
                    "l3_obi_5", "l3_obi_20", "l3_obi_50", "l3_obi_persistence_score",
                    "l3_cancel_to_trade_bid_5s", "l3_cancel_to_trade_ask_5s",
                    "l3_cancel_to_trade_bid_20s", "l3_cancel_to_trade_ask_20s",
                    "l3_queue_pressure_bid", "l3_queue_pressure_ask", "l3_market_depth_imbalance"
                ]:
                    out.append(_fd(ind, attr))
                    
                # 3. Golden features
                for attr in ["delta_z", "exec_risk_bps", "ofi_z", "spread_bps", "burst_z", "data_health", "fill_prob_proxy"]:
                    if attr == "data_health":
                        out.append(_fd(ind, attr, 1.0))
                    else:
                        out.append(_fd(ind, attr, 0.0))
                        
                # 4. Derived features
                direction_sign = 1.0 if _dir_sign_from_side(side) > 0 else -1.0
                out.append(1.0 if direction_sign > 0 else 0.0)  # direction_long
                
                c2t = [
                    _fd(ind, "l3_cancel_to_trade_bid_5s"),
                    _fd(ind, "l3_cancel_to_trade_ask_5s"),
                    _fd(ind, "l3_cancel_to_trade_bid_20s"),
                    _fd(ind, "l3_cancel_to_trade_ask_20s"),
                ]
                out.append(max(c2t) if c2t else 0.0)  # cancel_to_trade_max
                out.append(_fd(ind, "l3_obi_5") - _fd(ind, "l3_obi_50"))  # obi_spread
                out.append(_fd(ind, "l3_queue_pressure_bid") - _fd(ind, "l3_queue_pressure_ask"))  # queue_imbalance
                
                ind_delta_z = _fd(ind, "delta_z", 0.0)
                ind_ofi_z = _fd(ind, "ofi_z", 0.0)
                ind_spread_bps = _fd(ind, "spread_bps", 0.0)
                ind_obi = _fd(ind, "obi_avg_20", _fd(ind, "obi_20", 0.0))
                
                out.append(ind_delta_z * direction_sign)
                out.append(ind_ofi_z * direction_sign)
                out.append(ind_spread_bps * ind_obi)
                
                outlier_count = sum(1.0 for v in out if abs(v) > 10.0)
                out.append(outlier_count)
                out.append(1.0 if outlier_count > 0 else 0.0)
                return out

            elif sv == "3":
                from core.ml_feature_schema import build_feature_vector
                vec, _ = build_feature_vector(
                    symbol=getattr(ctx, "symbol", "unknown"),
                    ts_ms=ts_ms,
                    direction=side,
                    scenario=scenario,
                    indicators=ind,
                    rule_score=0.0, rule_have=0, rule_need=0,
                    cancel_spike_veto=int(cancel_spike_veto),
                    schema_ver=3
                )
                return vec
            elif sv in ("v4", "v4_of", "4"):
                from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF
                schema = MLFeatureSchemaV4OF()
                return schema.vectorize(
                    ts_ms=ts_ms, direction=side, scenario=scenario,
                    indicators=ind, cancel_spike_veto=bool(cancel_spike_veto)
                )
            elif sv in ("v5", "v5_of", "5"):
                from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OF
                schema = MLFeatureSchemaV5OF()
                return schema.vectorize(
                    ts_ms=ts_ms, direction=side, scenario=scenario,
                    indicators=ind, cancel_spike_veto=bool(cancel_spike_veto)
                )
            else:
                logger.error(
                    "Unsupported schema: sv=%r sv_tag=%r — check model pack metadata",
                    sv, sv_tag,
                )
                return None

        except Exception as e:
            logger.error("Feature extraction failed: %s", e)
            return None

    def _scale_features(self, raw: list[float]) -> list[float]:
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

    def _predict_r(self, features: list[float]) -> float | None:
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
    ) -> tuple[float | None, dict[str, Any]]:
        """Score a signal using ML model.

        Returns:
            (conf01, parts) if model available.
            (None, {}) if model unavailable — caller should fallback to rule-based.
        """
        parts: dict[str, Any] = {"scorer": "ml_v2"}

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

        from services.orderflow.metrics import ml_feature_mismatch_total, ml_scorer_latency_ms, ml_scorer_status_total
        t0 = time.monotonic_ns()

        # Schema matching
        schema_mismatch = False
        if len(raw_features) != len(self._feature_names):
            schema_mismatch = True
            model_ver = str(self._pack.get("kind", "unknown")) if self._pack else "unknown"
            schema_ver = str(getattr(self, "_feature_schema_version", 0))
            ml_feature_mismatch_total.labels(
                symbol=getattr(ctx, "symbol", "unknown"),
                model_ver=model_ver,
                schema_ver=schema_ver
            ).inc()
            logger.error("MLScoringGate schema mismatch: extracted features do not match model expectations (fail-closed)")
            parts["ml_status"] = "schema_mismatch"
            ml_scorer_status_total.labels(symbol=getattr(ctx, "symbol", "unknown"), status="fail-closed", mode="enforce").inc()
            ml_scorer_latency_ms.labels(symbol=getattr(ctx, "symbol", "unknown"), status="fail-closed").observe((time.monotonic_ns() - t0) / 1_000_000.0)

            # FAIL CLOSED: We return (0.0, parts) to explicitly reject the trade instead of None (which triggers rule-based fail-open)
            return 0.0, parts

        # Scale
        scaled_features = self._scale_features(raw_features)

        # Predict R-multiple
        predicted_r = self._predict_r(scaled_features)
        if predicted_r is None:
            parts["ml_status"] = "predict_failed"
            ml_scorer_status_total.labels(symbol=getattr(ctx, "symbol", "unknown"), status="fail-open", mode="enforce").inc()
            ml_scorer_latency_ms.labels(symbol=getattr(ctx, "symbol", "unknown"), status="fail-open").observe((time.monotonic_ns() - t0) / 1_000_000.0)
            return None, parts

        # Calibrate to conf01
        conf01 = self._calibrate_to_conf01(predicted_r)

        # Model metadata
        model_age_ms = get_ny_time_millis() - int(
            (self._pack or {}).get("trained_at_ms", 0)
        )

        parts.update({
            "ml_status": "ok",
            "ml_predicted_r": predicted_r,
            "ml_conf01": conf01,
            "ml_model_age_ms": model_age_ms,
            "ml_kind": kind,
        })

        # Phase 4.1: surface threshold info for observability. Default = no
        # threshold configured → ml_accept == ml_accept_unbounded (always 1).
        # Consumers that want to apply the threshold do so at the call site.
        if self._threshold is not None:
            parts["ml_threshold"] = float(self._threshold)
            parts["ml_accept"] = 1 if conf01 >= self._threshold else 0
        else:
            parts["ml_threshold"] = None
            parts["ml_accept"] = None

        ml_scorer_status_total.labels(symbol=getattr(ctx, "symbol", "unknown"), status="ok", mode="enforce").inc()
        ml_scorer_latency_ms.labels(symbol=getattr(ctx, "symbol", "unknown"), status="ok").observe((time.monotonic_ns() - t0) / 1_000_000.0)
        return conf01, parts

    @property
    def is_loaded(self) -> bool:
        return self._model is not None

    @property
    def model_metrics(self) -> dict[str, Any]:
        if self._pack is None:
            return {}
        return dict(self._pack.get("metrics", {}))
