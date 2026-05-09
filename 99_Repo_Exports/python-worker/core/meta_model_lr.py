from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Any

from core.feature_engineering import RobustScalerPack, apply_transform

# python-worker/core/meta_model_lr.py
from utils.time_utils import get_ny_time_millis


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

    def predict_proba(self, feat: dict[str, Any]) -> float:
        s = float(self.intercept)
        for name, w in zip(self.features, self.coef):
            v = _f(feat.get(name, 0.0), 0.0)
            v = self._transform_one(name, v)
            s += float(w) * float(v)
        return float(_sigmoid(s))

    def predict(self, feat: dict[str, Any]) -> int:
        return 1 if self.predict_proba(feat) >= float(self.threshold) else 0
