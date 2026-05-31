from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Any, Callable

from core.feature_engineering import RobustScalerPack, apply_transform, log1p_signed, clip

# python-worker/core/meta_model_lr.py
from utils.time_utils import get_ny_time_millis

try:
    import numpy as np  # type: ignore
except ImportError:  # pragma: no cover
    np = None  # type: ignore


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _sigmoid(x: float) -> float:
    # stable sigmoid
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        v = float(x)
        if not math.isfinite(v):
            return d
        return v
    except Exception:
        return d


@dataclass
class MetaModelLR:
    """Portable logistic regression model.

    JSON format (backward compatible):
      {
        "features": [...],
        "intercept": ...,
        "coef": [...],
        "threshold": 0.5,

        # optional:
        "transforms": { "f1": {"type":"log1p"}, "f2": {"type":"clip","lo":0,"hi":50} },
        "robust_scaler": { "f1": {"center": 1.23, "scale": 0.45}, ... }
      }
    """

    features: list[str]
    intercept: float
    coef: list[float]
    threshold: float = 0.5

    schema_name: str = "legacy"
    schema_version: int = 0
    schema_hash: str = ""
    feature_cols_hash: str = ""

    created_ms: int = 0
    model_signature: str = ""

    transforms: dict[str, Any] = field(default_factory=dict)
    robust_scaler: RobustScalerPack | None = None

    # Lazy hot-path compilation (built on first predict_proba call).
    # Why: hot inference loop iterates 300+ features; per-feature isinstance
    # checks, transform dict lookups and scaler dict.get'ы dominated latency
    # (~9.6ms p99). Precomputing closures + numpy vectors brings it to ~1ms.
    _compiled: bool = field(default=False, repr=False, compare=False)
    _features_tuple: tuple = field(default_factory=tuple, repr=False, compare=False)
    _coef_arr: Any = field(default=None, repr=False, compare=False)  # np.ndarray | list
    _coef_list: list = field(default_factory=list, repr=False, compare=False)
    _transform_fns: list = field(default_factory=list, repr=False, compare=False)
    _transform_idx: list = field(default_factory=list, repr=False, compare=False)
    _scaler_centers: Any = field(default=None, repr=False, compare=False)
    _scaler_scales: Any = field(default=None, repr=False, compare=False)
    _has_scaler: bool = field(default=False, repr=False, compare=False)

    @staticmethod
    def compute_feature_cols_hash(cols: list[str]) -> str:
        return _sha256_hex(",".join([str(x) for x in cols]))[:16]

    @staticmethod
    def compute_signature(d: dict[str, Any]) -> str:
        # Exclude signature itself
        dd = dict(d)
        dd.pop("model_signature", None)
        s = json.dumps(dd, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return _sha256_hex(s)[:16]

    @staticmethod
    def load(path: str) -> MetaModelLR:
        d = json.loads(open(path, encoding="utf-8").read())
        tf = d.get("transforms") if isinstance(d.get("transforms"), dict) else {}
        rs = d.get("robust_scaler") if isinstance(d.get("robust_scaler"), dict) else {}

        features = list(d.get("features", []) or [])
        schema_name = str(d.get("schema_name") or d.get("feature_schema") or "")
        schema_version = int(d.get("schema_version") or d.get("feature_schema_version") or 0)

        # Backward compatibility:
        # - older artifacts sometimes stored schema_hash in feature_cols_hash
        cols_hash = str(d.get("feature_cols_hash") or d.get("schema_hash") or d.get("schema_cols_hash") or "")
        schema_hash = str(d.get("schema_hash") or d.get("feature_cols_hash") or d.get("schema_cols_hash") or cols_hash or "")

        # Ensure feature_cols_hash is set if features are present and it's missing
        if not cols_hash and features:
            cols_hash = MetaModelLR.compute_feature_cols_hash(features)

        created_ms = int(d.get("created_ms") or 0)
        model_signature = (d.get("model_signature") or "")

        rs_pack = None
        if rs:
            params: dict[str, dict[str, float]] = {}
            for k, v in rs.items():
                if not isinstance(v, dict):
                    continue
                try:
                    params[str(k)] = {
                        "center": float(v.get("center", 0.0)),
                        "scale": float(v.get("scale", 1.0)),
                    }
                except Exception:
                    continue
            rs_pack = RobustScalerPack(params=params)

        return MetaModelLR(
            features=features,
            intercept=float(d.get("intercept", 0.0)),
            coef=[float(x) for x in (d.get("coef", []) or [])],
            threshold=float(d.get("threshold", 0.5)),
            schema_name=schema_name or "legacy",
            schema_version=schema_version,
            schema_hash=schema_hash,
            feature_cols_hash=cols_hash,
            created_ms=created_ms,
            model_signature=model_signature,
            transforms={str(k): v for k, v in tf.items()},
            robust_scaler=rs_pack,
        )

    def to_dict(self) -> dict[str, Any]:
        """Export to JSON-compatible dict."""
        rs_dict = {}
        if self.robust_scaler and hasattr(self.robust_scaler, "params"):
            # Reconstruct robust scaler dict
            for k, p in self.robust_scaler.params.items():
                rs_dict[k] = {"center": float(p.get("center", 0.0)), "scale": float(p.get("scale", 1.0))}

        return {
            "features": list(self.features),
            "intercept": float(self.intercept),
            "coef": [float(x) for x in self.coef],
            "threshold": float(self.threshold),
            "transforms": dict(self.transforms or {}),
            "robust_scaler": rs_dict,
            "schema_name": str(self.schema_name or "legacy"),
            "schema_version": int(self.schema_version or 0),
            "schema_hash": str(self.schema_hash or ""),
            "feature_cols_hash": str(self.feature_cols_hash or ""),
            "created_ms": int(self.created_ms or 0),
            "model_signature": str(self.model_signature or ""),
        }

    def dump(self, path: str) -> None:
        """Save to JSON file (fills created_ms, feature_cols_hash and signature if missing)."""
        if not self.created_ms:
            self.created_ms = get_ny_time_millis()
        if not self.feature_cols_hash and self.features:
            self.feature_cols_hash = MetaModelLR.compute_feature_cols_hash(self.features)

        d = self.to_dict()
        # compute signature over canonical dict (excluding signature)
        sig = MetaModelLR.compute_signature(d)
        self.model_signature = sig
        d["model_signature"] = sig

        with open(path, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2, ensure_ascii=False)

    def signature_ok(self) -> bool:
        try:
            if not self.model_signature:
                return False
            want = MetaModelLR.compute_signature(self.to_dict())
            return str(self.model_signature) == str(want)
        except Exception:
            return False

    def ensure_signature(self) -> None:
        if not self.signature_ok():
            raise ValueError("meta_model_signature_invalid")

    def _transform_one(self, name: str, v: float) -> float:
        if self.transforms and name in self.transforms:
            v = apply_transform(float(v), self.transforms.get(name))
        if self.robust_scaler:
            v = self.robust_scaler.scale(name, float(v))
        return float(v)

    @staticmethod
    def _compile_transform(spec: Any) -> Callable[[float], float] | None:
        """Pre-bind a transform spec into a closure. Returns None for identity.

        Saves ~5 dict lookups + 2 isinstance calls per feature per call.
        """
        if spec is None:
            return None
        if isinstance(spec, str):
            t = spec.lower()
            s: dict[str, Any] = {}
        elif isinstance(spec, dict):
            t = str(spec.get("type") or spec.get("name") or "identity").lower()
            s = spec
        else:
            return None

        if t in ("identity", "none", ""):
            return None
        if t == "log1p":
            def _fn_log1p(x: float) -> float:
                if x < 0:
                    return log1p_signed(x)
                try:
                    return math.log1p(x)
                except Exception:
                    return x
            return _fn_log1p
        if t in ("log1p_signed", "signed_log1p"):
            return log1p_signed
        if t in ("clip", "clamp", "winsor", "winsorize"):
            lo = s.get("lo", None)
            hi = s.get("hi", None)
            lo_f = float(lo) if lo is not None else None
            hi_f = float(hi) if hi is not None else None
            def _fn_clip(x: float, _lo=lo_f, _hi=hi_f) -> float:
                try:
                    return clip(x, _lo, _hi)
                except Exception:
                    return x
            return _fn_clip
        return None

    def _compile(self) -> None:
        n = len(self.features)
        feats = tuple(self.features)
        coefs = [float(c) for c in self.coef]

        tfs: list = [None] * n
        tf_idx: list[int] = []
        if self.transforms:
            for i, name in enumerate(feats):
                spec = self.transforms.get(name)
                fn = self._compile_transform(spec)
                if fn is not None:
                    tfs[i] = fn
                    tf_idx.append(i)

        has_scaler = False
        centers_list: list[float] = [0.0] * n
        scales_list: list[float] = [1.0] * n
        if self.robust_scaler and self.robust_scaler.params:
            params = self.robust_scaler.params
            for i, name in enumerate(feats):
                p = params.get(name)
                if not p:
                    continue
                c = float(p.get("center", 0.0))
                sc = float(p.get("scale", 1.0))
                if not math.isfinite(sc) or abs(sc) < 1e-9:
                    sc = 1.0
                if c != 0.0 or sc != 1.0:
                    has_scaler = True
                centers_list[i] = c
                scales_list[i] = sc

        self._features_tuple = feats
        self._coef_list = coefs
        if np is not None:
            self._coef_arr = np.asarray(coefs, dtype=np.float64)
            self._scaler_centers = np.asarray(centers_list, dtype=np.float64)
            self._scaler_scales = np.asarray(scales_list, dtype=np.float64)
        else:
            self._coef_arr = coefs
            self._scaler_centers = centers_list
            self._scaler_scales = scales_list
        self._transform_fns = tfs
        self._transform_idx = tf_idx
        self._has_scaler = has_scaler
        self._compiled = True

    def predict_proba(self, feat: dict[str, Any]) -> float:
        if not self._compiled:
            self._compile()

        feats = self._features_tuple
        n = len(feats)
        tfs = self._transform_fns
        tf_idx = self._transform_idx

        if np is not None:
            # Build value vector with safe-float semantics (None/NaN/Inf -> 0).
            vals = np.empty(n, dtype=np.float64)
            for i in range(n):
                raw = feat.get(feats[i], 0.0)
                if raw is None:
                    vals[i] = 0.0
                    continue
                try:
                    v = float(raw)
                except Exception:
                    vals[i] = 0.0
                    continue
                if v != v or v == math.inf or v == -math.inf:
                    vals[i] = 0.0
                else:
                    vals[i] = v
            # Apply per-feature transforms only where non-identity.
            for i in tf_idx:
                fn = tfs[i]
                if fn is not None:
                    try:
                        vals[i] = float(fn(float(vals[i])))
                    except Exception:
                        pass
            # Vectorized robust scaling.
            if self._has_scaler:
                vals = (vals - self._scaler_centers) / self._scaler_scales
            s = float(self.intercept) + float(np.dot(self._coef_arr, vals))
            return _sigmoid(s)

        # Pure-Python fallback (still avoids isinstance/dict-dance from old path).
        s = float(self.intercept)
        coefs = self._coef_list
        centers = self._scaler_centers
        scales = self._scaler_scales
        has_scaler = self._has_scaler
        for i in range(n):
            raw = feat.get(feats[i], 0.0)
            if raw is None:
                v = 0.0
            else:
                try:
                    v = float(raw)
                    if v != v or v == math.inf or v == -math.inf:
                        v = 0.0
                except Exception:
                    v = 0.0
            fn = tfs[i]
            if fn is not None:
                try:
                    v = float(fn(v))
                except Exception:
                    pass
            if has_scaler:
                v = (v - centers[i]) / scales[i]
            s += coefs[i] * v
        return _sigmoid(s)

    def predict(self, feat: dict[str, Any]) -> int:
        return 1 if self.predict_proba(feat) >= float(self.threshold) else 0
