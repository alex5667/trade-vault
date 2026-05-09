from __future__ import annotations

# core/fast_linear_util_mh.py
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.feature_engineering import RobustScalerPack


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _dot(coef: list[float], x: list[float]) -> float:
    s = 0.0
    for w, v in zip(coef, x):
        s += float(w) * float(v)
    return float(s)


@dataclass
class HorizonLinear:
    intercept: float
    coef: list[float]
    # uncertainty proxy (e.g., median absolute residual on val); constant is fine for gating.
    unc: float = 0.0


@dataclass
class FastLinearUtilMHModel:
    """Portable util_mh model: b + dot(w, x_row).

    Intended for ultra-low-latency online inference and easy rollout.
    Implements the same interface as your joblib UtilMHModel: predict_util/predict_unc.

    JSON format (suggested):
      {
        "kind": "util_mh_fastlinear_v1",
        "feature_cols": [...],
        "horizons_ms": [60000, 300000, ...],
        "weights": {
          "60000": {"intercept": 0.1, "coef": [..], "unc": 0.03},
          ...
        },
        "feature_transforms": {...},
        "robust_scaler": {...},
        "spread_bucket_edges": [2,5,10,20],
        "session_cfg": {"session_hours": {...}},
        "liq_cfg": {"regime_thresholds": [0.33,0.66]}
      }
    """

    feature_cols: list[str]
    horizons_ms: list[int]
    models: dict[int, HorizonLinear]

    # Optional feature engineering used by ml_confirm_gate._build_feature_row
    feature_transforms: dict[str, Any]
    robust_scaler: RobustScalerPack | None = None
    spread_bucket_edges: list[float] | None = None
    session_cfg: dict[str, Any] | None = None
    liq_cfg: dict[str, Any] | None = None

    @staticmethod
    def load(path: str) -> FastLinearUtilMHModel:
        d = json.loads(Path(path).read_text(encoding="utf-8"))
        fcols = list(d.get("feature_cols") or [])
        h_ms = [int(x) for x in (d.get("horizons_ms") or [])]
        w = d.get("weights") if isinstance(d.get("weights"), dict) else {}

        models: dict[int, HorizonLinear] = {}
        for hk, hv in w.items():
            if not isinstance(hv, dict):
                continue
            h = int(hk)
            coef = [float(x) for x in (hv.get("coef") or [])]
            models[h] = HorizonLinear(
                intercept=float(hv.get("intercept", 0.0)),
                coef=coef,
                unc=float(hv.get("unc", 0.0)),
            )

        tf = d.get("feature_transforms") if isinstance(d.get("feature_transforms"), dict) else {}
        rs = d.get("robust_scaler") if isinstance(d.get("robust_scaler"), dict) else {}
        rs_pack = RobustScalerPack(params={str(k): {"center": float(v.get("center", 0.0)), "scale": float(v.get("scale", 1.0))}
                                           for k, v in rs.items() if isinstance(v, dict)}) if rs else None

        spread_edges = d.get("spread_bucket_edges")
        if isinstance(spread_edges, list) and len(spread_edges) > 0:
            spread_edges = [float(x) for x in spread_edges]
        else:
            spread_edges = None

        session_cfg = d.get("session_cfg") if isinstance(d.get("session_cfg"), dict) else None
        liq_cfg = d.get("liq_cfg") if isinstance(d.get("liq_cfg"), dict) else None

        return FastLinearUtilMHModel(
            feature_cols=fcols,
            horizons_ms=h_ms,
            models=models,
            feature_transforms={str(k): v for k, v in tf.items()},
            robust_scaler=rs_pack,
            spread_bucket_edges=spread_edges,
            session_cfg=session_cfg,
            liq_cfg=liq_cfg,
        )

    def predict_util(self, X: list[list[float]], horizons_ms: list[int]) -> dict[int, list[float]]:
        # X: [[x1, x2, ...]]
        x = list((X or [[0.0]])[0])
        out: dict[int, list[float]] = {}
        for h in horizons_ms:
            hh = int(h)
            m = self.models.get(hh)
            if not m:
                out[hh] = [0.0]
                continue
            out[hh] = [float(m.intercept + _dot(m.coef, x))]
        return out

    def predict_unc(self, X: list[list[float]], horizons_ms: list[int]) -> dict[int, list[float]]:
        out: dict[int, list[float]] = {}
        for h in horizons_ms:
            hh = int(h)
            m = self.models.get(hh)
            out[hh] = [float(m.unc if m else 0.0)]
        return out










