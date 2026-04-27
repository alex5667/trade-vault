# python-worker/core/meta_model_lr.py
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Dict, List


def _sigmoid(x: float) -> float:
    # stable sigmoid
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


@dataclass
class MetaModelLR:
    features: List[str]
    intercept: float
    coef: List[float]
    threshold: float = 0.5
    schema_version: str = ""

    @staticmethod
    def load(path: str) -> "MetaModelLR":
        d = json.loads(open(path, "r", encoding="utf-8").read())
        return MetaModelLR(
            features=list(d["features"]),
            intercept=float(d["intercept"]),
            coef=[float(x) for x in d["coef"]],
            threshold=float(d.get("threshold", 0.5)),
            schema_version=str(d.get("schema_version") or d.get("feature_schema") or d.get("feature_schema_version") or ""),
        )

    def predict_proba(self, feat: Dict[str, Any]) -> float:
        s = float(self.intercept)
        for name, w in zip(self.features, self.coef):
            try:
                v = float(feat.get(name, 0.0) or 0.0)
            except Exception:
                v = 0.0
            s += float(w) * v
        return float(_sigmoid(s))

    def predict(self, feat: Dict[str, Any]) -> int:
        return 1 if self.predict_proba(feat) >= float(self.threshold) else 0
