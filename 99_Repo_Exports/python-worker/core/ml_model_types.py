from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass
class UtilMHModelV1:
    """Multi-horizon utility model: predicts util_r per horizon, uncertainty via ensemble disagreement."""
    feature_cols: list[str]
    horizons: list[int]
    unc_k: float

    # Per horizon models (Ridge + GBDT ensemble)
    ridge: dict[int, Any]
    gbdt: dict[int, Any]

    def predict_util(self, X: np.ndarray) -> dict[int, np.ndarray]:
        """Predict expected utility per horizon (ensemble average)."""
        out: dict[int, np.ndarray] = {}
        for h in self.horizons:
            p1 = self.ridge[h].predict(X)
            p2 = self.gbdt[h].predict(X)
            out[h] = 0.5 * (p1 + p2)
        return out

    def predict_unc(self, X: np.ndarray) -> dict[int, np.ndarray]:
        """Predict uncertainty per horizon (ensemble disagreement)."""
        out: dict[int, np.ndarray] = {}
        for h in self.horizons:
            p1 = self.ridge[h].predict(X)
            p2 = self.gbdt[h].predict(X)
            out[h] = np.abs(p2 - p1)
        return out

    def predict_score(self, X: np.ndarray) -> dict[int, np.ndarray]:
        """Predict risk-adjusted score: util - unc_k * unc."""
        u = self.predict_util(X)
        un = self.predict_unc(X)
        k = float(self.unc_k)
        return {h: (u[h] - k * un[h]) for h in self.horizons}
