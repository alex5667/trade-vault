import collections
import logging
import os
from typing import Any

import joblib

logger = logging.getLogger("ml_confirm_gate.model")

class BoundedLRUCache(collections.OrderedDict):
    """LRU Cache to prevent OOM when loading many ML models."""
    def __init__(self, maxsize: int = 30, *args: Any, **kwds: Any) -> None:
        self.maxsize = maxsize
        super().__init__(*args, **kwds)

    def __getitem__(self, key: Any) -> Any:
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def get(self, key: Any, default: Any = None) -> Any:
        if key in self:
            self.move_to_end(key)
            return super().__getitem__(key)
        return default

    def __setitem__(self, key: Any, value: Any) -> None:
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            oldest_key, oldest_value = self.popitem(last=False)
            del oldest_value


_SHARED_MODELS: BoundedLRUCache = BoundedLRUCache(maxsize=30)
_SHARED_MODEL_STATS: BoundedLRUCache = BoundedLRUCache(maxsize=30)


class _DictPackModelView:
    """Expose dict-pack model keys as attributes for _build_feature_row."""
    def __init__(self, pack: dict[str, Any]):
        self.feature_cols = list(pack.get("feature_cols", []) or [])
        tf = pack.get("feature_transforms")
        self.feature_transforms = tf if isinstance(tf, dict) else {}
        self.robust_scaler = pack.get("robust_scaler")
        sc = pack.get("session_cfg")
        self.session_cfg = sc if isinstance(sc, dict) else {}
        self.spread_bucket_edges = pack.get("spread_bucket_edges")
        lc = pack.get("liq_cfg")
        self.liq_cfg = lc if isinstance(lc, dict) else {}


def _load_model_cached(model_path: str, kind: str, logger: Any = None, force_stat_check: bool = True) -> Any | None:
    """Load model from disk or return from process-level cache if unchanged."""
    if not model_path:
        return None

    if not force_stat_check and model_path in _SHARED_MODELS:
        return _SHARED_MODELS[model_path]

    if not os.path.exists(model_path):
        if logger:
            logger.debug(f"ML gate: Model path does not exist: {model_path}")
        return None

    try:
        mtime = os.path.getmtime(model_path)
        size = os.path.getsize(model_path)
    except Exception as e:
        if logger:
            logger.warning(f"ML gate: Failed to get stats for {model_path}: {e}")
        return None

    stats = (mtime, size)

    if model_path in _SHARED_MODELS and _SHARED_MODEL_STATS.get(model_path) == stats:
        if logger:
            logger.debug(f"ML gate: Using cached model for {model_path} (kind={kind})")
        return _SHARED_MODELS[model_path]

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
            try:
                model = joblib.load(model_path)
            except ModuleNotFoundError as e:
                if "catboost" in str(e).lower():
                    if logger:
                        logger.error(f"ML gate: missing optional dependency 'catboost' for model {model_path}. Prediction may fail.")
                    return None
                raise

        if model:
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
                        logger.error(f"ML gate: edge_stack_v1 model at {model_path} missing keys")
                    return None

                # SCHEMA GUARD
                fcols = list(model.get("feature_cols", []))
                expected_hash = model.get("feature_cols_hash")
                n_features_expected = model.get("n_features_expected")
                schema_version = model.get("feature_schema_version")

                if expected_hash:
                    import hashlib
                    actual_hash = hashlib.md5(",".join(fcols).encode("utf-8")).hexdigest()
                    if actual_hash != expected_hash:
                        if logger:
                            logger.error(f"ML gate: schema hash mismatch for {model_path}. Expected {expected_hash}, got {actual_hash}. Schema drift detected!")
                        return None

                if n_features_expected and len(fcols) != n_features_expected:
                    if logger:
                        logger.error(f"ML gate: n_features mismatch for {model_path}. Expected {n_features_expected}, got {len(fcols)}")
                    return None

                _strict_env = (os.environ.get("EDGE_STACK_STRICT_FEATURE_COLS", "0") or "0").strip().lower()
                if _strict_env in ("1", "true", "yes"):
                    _bad = [c for c in fcols if str(c).startswith("scenario_v4_")]
                    if _bad:
                        if logger:
                            logger.error(
                                f"ML gate: strict feature_cols rejects scenario_v4_* columns "
                                f"(found={_bad[:5]}); set EDGE_STACK_STRICT_FEATURE_COLS=0 to disable"
                            )
                        return None

            _SHARED_MODELS[model_path] = model
            _SHARED_MODEL_STATS[model_path] = stats
            if logger:
                logger.info(f"ML gate: Successfully loaded and cached model from {model_path} (type={type(model).__name__})")
    except Exception as e:
        if logger:
            logger.error(f"ML gate: Failed to load model from {model_path}: {e}")

    return model
