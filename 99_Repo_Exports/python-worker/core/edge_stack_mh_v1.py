from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List
import numpy as np


@dataclass
class EdgeStackMHModelV1:
    """
    Multi-horizon edge probability model with strict OOF stacking.

    Inference:
      p_lr[h], p_gbdt[h] -> p_meta[h] -> p_cal[h]
      unc[h] = |p_lr[h] - p_gbdt[h]|
      score[h] = p_cal[h] - unc_k * unc[h]
    """
    feature_cols: List[str]
    horizons: List[int]
    unc_k: float

    # Preprocessing (must exist in your repo; you already use it in ml_confirm_gate)
    scaler: Any

    # Base models per horizon
    lr: Dict[int, Any]
    gbdt: Dict[int, Any]

    # Meta models per horizon (trained strictly on OOF base preds)
    meta: Dict[int, Any]

    # Calibrators per horizon (trained strictly on OOF meta preds)
    calibrator: Dict[int, Any]

    def _transform(self, X: np.ndarray) -> np.ndarray:
        if self.scaler is None:
            return X
        # Pass feature_cols to transform for proper column mapping
        return self.scaler.transform(X, feature_names=self.feature_cols)

    def predict_base(self, X: np.ndarray) -> Dict[int, Dict[str, np.ndarray]]:
        Xs = self._transform(X)
        out: Dict[int, Dict[str, np.ndarray]] = {}
        for h in self.horizons:
            plr = self.lr[h].predict_proba(Xs)[:, 1]
            pgb = self.gbdt[h].predict_proba(Xs)[:, 1]
            out[h] = {"lr": plr, "gbdt": pgb}
        return out

    def predict_p_raw(self, X: np.ndarray) -> Dict[int, np.ndarray]:
        base = self.predict_base(X)
        out: Dict[int, np.ndarray] = {}
        for h in self.horizons:
            Z = np.column_stack([base[h]["lr"], base[h]["gbdt"]])
            out[h] = self.meta[h].predict_proba(Z)[:, 1]
        return out

    def predict_p_cal(self, X: np.ndarray) -> Dict[int, np.ndarray]:
        p_raw = self.predict_p_raw(X)
        out: Dict[int, np.ndarray] = {}
        for h in self.horizons:
            cal = self.calibrator.get(h)
            if cal is None:
                out[h] = p_raw[h]
            else:
                out[h] = np.asarray([cal.apply_one(float(p)) for p in p_raw[h]], dtype=np.float64)
        return out

    def predict_unc(self, X: np.ndarray) -> Dict[int, np.ndarray]:
        base = self.predict_base(X)
        out: Dict[int, np.ndarray] = {}
        for h in self.horizons:
            out[h] = np.abs(base[h]["gbdt"] - base[h]["lr"])
        return out

    def predict_score(self, X: np.ndarray) -> Dict[int, np.ndarray]:
        p = self.predict_p_cal(X)
        un = self.predict_unc(X)
        out: Dict[int, np.ndarray] = {}
        for h in self.horizons:
            out[h] = p[h] - float(self.unc_k) * un[h]
        return out

