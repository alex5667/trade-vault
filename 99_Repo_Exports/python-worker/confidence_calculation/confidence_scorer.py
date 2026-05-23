from __future__ import annotations

import math
from typing import Any

try:
    from ..utils.helpers import _f
except (ImportError, ValueError):
    try:
        from utils.helpers import _f
    except ImportError:
        def _f(v): return float(v) # Fallback for local tests


import json
import logging
import os

logger = logging.getLogger("ConfidenceScorer")

# Phase 3: Optional ML support
try:
    import lightgbm as lgb
except ImportError:
    lgb = None

try:
    import joblib
except ImportError:
    joblib = None

# Categorical features for the Phase-3 scorer. Symmetric encoder shared
# with the trainer (`tools.train_scorer_model_v1`) so train and serve stay
# aligned. Failure to import (e.g. partial deployment) falls back to a
# no-op encoder so regular numeric features still score correctly.
try:
    from core.scorer_categorical_features import (
        encode_categorical_from_ctx as _scorer_encode_cat_from_ctx,
        is_categorical_feature_name as _scorer_is_cat_feature,
    )
except ImportError:  # pragma: no cover — fallback for partial deploys
    def _scorer_encode_cat_from_ctx(_ctx: Any) -> dict[str, int]:  # type: ignore[misc]
        return {}
    def _scorer_is_cat_feature(name: str) -> bool:  # type: ignore[misc]
        return name.startswith("_cat_")

# --- Phase 3 ML scorer: mtime-cached loader -------------------------------
# Previously the model was joblib.load()'ed on EVERY signal score, which
# (a) burned CPU on a hot path, and (b) hammered the log when the file was
# missing (ML_SCORING_ENABLE=1 + no scorer_model.lgb yet). The cache:
#   - returns (None, None) silently when the file is absent
#   - reloads only when mtime changes (auto-picks up retrain output)
#   - logs at INFO on successful (re)load and at WARNING on load failure
_ML_MODEL_CACHE: dict[str, Any] = {"path": None, "mtime": 0.0, "model": None, "features": None}


def _load_ml_scorer(ml_model_path: str) -> tuple[Any, Any]:
    """Returns (model, feature_names) or (None, None). Cached by mtime."""
    if not ml_model_path or joblib is None:
        return (None, None)
    try:
        mtime = os.path.getmtime(ml_model_path)
    except OSError:
        # File not yet produced by the trainer — silently skip.
        return (None, None)
    cache = _ML_MODEL_CACHE
    if (cache["path"] == ml_model_path
            and cache["mtime"] == mtime
            and cache["model"] is not None):
        return (cache["model"], cache["features"])
    # Reload
    try:
        feats_path = ml_model_path.replace(".lgb", ".features")
        model = joblib.load(ml_model_path)
        features = joblib.load(feats_path)
    except Exception as e:
        logger.warning("ML scorer reload failed: %s (path=%s)", e, ml_model_path)
        return (None, None)
    cache["path"] = ml_model_path
    cache["mtime"] = mtime
    cache["model"] = model
    cache["features"] = features
    logger.info("ML scorer loaded: %s (mtime=%s, n_features=%d)",
                ml_model_path, mtime, len(features) if features else 0)
    return (model, features)


def _crypto_conf_factor(
    ctx: SignalContext,
    signal_kind: str,
    weights_path: str | None = None,
    ml_model_path: str | None = None,
) -> tuple[float, dict[str, float] | None]:
    """
    Final Confidence Scorer (Phases 1-3).
    """

    def _clamp01(x: float) -> float:
        return max(0.0, min(1.0, float(x)))

    # Load dynamic weights (Phase 2)
    tuning = {}
    if weights_path and os.path.exists(weights_path):
        try:
            with open(weights_path) as f:
                data = json.load(f)
                tuning = data.get("suggested_weights", {})
        except Exception as e:
            logger.error(f"Failed to load weights: {e}")

    def _cfgf(name: str, default: float) -> float:
        # Priority: Calibrated Weights -> SignalContext -> Default
        if name in tuning:
             return float(tuning[name])
        try:
            v = getattr(ctx, name, None)
            if v is not None and not hasattr(v, "__call__"): # avoid mocks/methods
                return float(v)
            return default
        except Exception:
            return default

    def _sat(raw: float, cap: float) -> float:
        cap = max(float(cap), 1e-9)
        raw = max(float(raw), 0.0)
        return cap * (1.0 - math.exp(-raw / cap))

    parts: dict[str, float] = {}

    # --- Regime & ATR ---
    regime_raw = getattr(ctx, "market_mode", "neutral")
    if hasattr(regime_raw, "__call__"): regime_raw = "neutral" # handle mock
    regime = "trend" if any(x in str(regime_raw).lower() for x in ["trend", "momentum"]) else "range"

    atr_val = getattr(ctx, "atr_q_main", 0.5)
    if hasattr(atr_val, "__call__"): atr_val = 0.5
    atr_q = _clamp01(atr_val)
    # ...
    atr_regime = 1.0
    if atr_q < 0.3: atr_regime = (atr_q - 0.05) / 0.25
    elif atr_q > 0.7: atr_regime = (0.95 - atr_q) / 0.25
    atr_regime = _clamp01(atr_regime)

    parts.update({"atr_q": atr_q, "atr_regime": atr_regime})

    # --- Features ---
    def _gv(name: str, default: float) -> float:
        try:
            val = getattr(ctx, name, default)
            if hasattr(val, "__call__"): val = default
            return float(val)
        except Exception:
            return default

    main_z = abs(_gv("main_z", _gv("delta_z", 0.0)))
    z_core = _clamp01((main_z - 1.0) / 3.0)

    obi_z = abs(_gv("obi_z", 0.0))
    obi_persist = _clamp01((obi_z - 0.5) / 2.0)

    weak_ratio = _gv("weak_ratio", _gv("range_vs_atr", 1.0))
    progress = 1.0
    if weak_ratio < 0.4: progress = (weak_ratio - 0.2) / 0.2
    elif weak_ratio > 1.2: progress = (1.5 - weak_ratio) / 0.3
    progress = _clamp01(progress)

    # --- Base Weights (Regime-aware) ---
    w_z, w_obi, w_prog = 0.4, 0.3, 0.3
    if regime == "trend":
        w_z *= _cfgf("z_trend_m", 1.2)
        w_obi *= _cfgf("obi_trend_m", 1.1)
    else:
        w_prog *= _cfgf("prog_range_m", 1.3)

    w_sum = w_z + w_obi + w_prog
    base = (w_z/w_sum)*z_core + (w_obi/w_sum)*obi_persist + (w_prog/w_sum)*progress
    parts["base_score"] = _clamp01(base)

    # --- Bonuses ---
    def _has(k):
        confs = getattr(ctx, "confirmations", [])
        if not isinstance(confs, (list, tuple)): confs = []
        if k in confs: return True

        ev = getattr(ctx, "evidence", {})
        if not isinstance(ev, dict): ev = {}
        if k in ev: return True

        return False

    b_raw = 0.0
    if _has("reclaim"): b_raw += _cfgf("b_reclaim", 0.05)
    if _has("sweep"): b_raw += _cfgf("b_sweep", 0.03)
    if _has("rsi_agree"): b_raw += _cfgf("b_rsi", 0.02)
    if _has("div_match"): b_raw += _cfgf("b_div", 0.03)

    # Anti-correlation & Synergy
    if regime == "trend" and main_z > 3.0: b_raw *= 0.5 # dampen oscillators
    if _has("sweep") and _has("reclaim"): b_raw += 0.02 # synergy

    bonus = min(b_raw, 0.15)
    parts["bonus"] = bonus

    final_score = _clamp01(base + bonus)

    # --- Phase 3: ML Fusion ---
    if os.getenv("ML_SCORING_ENABLE") == "1" and lgb and joblib and ml_model_path:
        model, feature_names = _load_ml_scorer(ml_model_path)
        if model is not None and feature_names:
            try:
                # Categorical features (_cat_*) are derived from the context via the
                # shared encoder, NOT from `getattr(ctx, "_cat_*")` (which would
                # silently default to 0.0 and break train/serve symmetry).
                cat_values = _scorer_encode_cat_from_ctx(ctx) if any(
                    _scorer_is_cat_feature(fn) for fn in feature_names
                ) else {}

                # Prepare feature vector
                f_vec = []
                for fn in feature_names:
                    if _scorer_is_cat_feature(fn):
                        f_vec.append(float(cat_values.get(fn, -1)))
                    else:
                        f_vec.append(float(getattr(ctx, fn, 0.0)))

                ml_prob = model.predict([f_vec])[0]
                parts["ml_prob"] = ml_prob

                # Late fusion: 60% base, 40% ML
                alpha = _cfgf("ml_fusion_alpha", 0.4)
                final_score = (1 - alpha) * final_score + alpha * ml_prob
            except Exception as e:
                logger.warning(f"ML Scoring failed: {e}")

    parts["confidence"] = _clamp01(final_score)
    return parts["confidence"], parts


class ConfidenceScorer:
    def __init__(self, *args, **kwargs):
        self.weights_path = os.getenv("SCORER_WEIGHTS_PATH", "python-worker/config/suggested_weights.json")
        self.ml_model_path = os.getenv("SCORER_ML_MODEL_PATH", "python-worker/ml_models/scorer_model.lgb")

    def score(self, kind: str, side: str, ctx: Any) -> tuple[float, dict[str, float] | None]:
        # Inject side into ctx.evidence if not present
        if hasattr(ctx, "evidence") and isinstance(ctx.evidence, dict):
            ctx.evidence["side"] = side

        return _crypto_conf_factor(
            ctx,
            kind,
            weights_path=self.weights_path,
            ml_model_path=self.ml_model_path
        )
