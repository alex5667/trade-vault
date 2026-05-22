from __future__ import annotations

"""Extended calibration diagnostics for confidence models.

Complements classic ECE/Brier with:
- MCE (worst calibration pocket)
- calibration slope/intercept via logistic regression on logit(p)
- sharpness mean / entropy
- probability mass near 0.5
"""

from dataclasses import dataclass
from typing import Any

import numpy as np

_EPS = 1e-6


def _as_arrays(y: Any, p: Any) -> tuple[np.ndarray, np.ndarray]:
    yy = np.asarray(y, dtype=np.float64).reshape(-1)
    pp = np.asarray(p, dtype=np.float64).reshape(-1)
    n = min(len(yy), len(pp))
    yy = yy[:n]
    pp = pp[:n]
    m = np.isfinite(yy) & np.isfinite(pp)
    yy = np.clip(yy[m], 0.0, 1.0)
    pp = np.clip(pp[m], _EPS, 1.0 - _EPS)
    return yy, pp


def binary_entropy(p: Any, *, base2: bool = True) -> np.ndarray:
    pp = np.asarray(p, dtype=np.float64)
    pp = np.clip(pp, _EPS, 1.0 - _EPS)
    h = -(pp * np.log(pp) + (1.0 - pp) * np.log(1.0 - pp))
    if base2:
        h = h / np.log(2.0)
    return h


def ece(y: Any, p: Any, *, bins: int = 20) -> float:
    yy, pp = _as_arrays(y, p)
    if len(yy) == 0:
        return float("nan")
    edges = np.linspace(0.0, 1.0, int(max(2, bins)) + 1)
    out = 0.0
    for i in range(len(edges) - 1):
        lo, hi = float(edges[i]), float(edges[i + 1])
        m = (pp >= lo) & (pp < hi) if i < len(edges) - 2 else (pp >= lo) & (pp <= hi)
        if not np.any(m):
            continue
        out += float(np.mean(m)) * abs(float(np.mean(yy[m])) - float(np.mean(pp[m])))
    return float(out)


def mce(y: Any, p: Any, *, bins: int = 20) -> float:
    """Maximum Calibration Error – worst single bin calibration gap."""
    yy, pp = _as_arrays(y, p)
    if len(yy) == 0:
        return float("nan")
    edges = np.linspace(0.0, 1.0, int(max(2, bins)) + 1)
    worst = 0.0
    for i in range(len(edges) - 1):
        lo, hi = float(edges[i]), float(edges[i + 1])
        m = (pp >= lo) & (pp < hi) if i < len(edges) - 2 else (pp >= lo) & (pp <= hi)
        if not np.any(m):
            continue
        worst = max(worst, abs(float(np.mean(yy[m])) - float(np.mean(pp[m]))))
    return float(worst)


def brier(y: Any, p: Any) -> float:
    yy, pp = _as_arrays(y, p)
    if len(yy) == 0:
        return float("nan")
    return float(np.mean((pp - yy) ** 2))


def sharpness_mean(p: Any) -> float:
    """Average |p - 0.5| * 2 ∈ [0, 1]; 0 = fully grey, 1 = fully decisive."""
    pp = np.asarray(p, dtype=np.float64)
    pp = pp[np.isfinite(pp)]
    if len(pp) == 0:
        return float("nan")
    return float(np.mean(np.abs(np.clip(pp, 0.0, 1.0) - 0.5)) * 2.0)


def sharpness_entropy(p: Any) -> float:
    """Mean binary entropy of predictions; 1 = flat, 0 = fully decisive."""
    pp = np.asarray(p, dtype=np.float64)
    pp = pp[np.isfinite(pp)]
    if len(pp) == 0:
        return float("nan")
    return float(np.mean(binary_entropy(pp, base2=True)))


def prob_mass_near_half(p: Any, *, half_width: float = 0.05) -> float:
    """Fraction of predictions in (0.5-hw, 0.5+hw); high = too grey."""
    pp = np.asarray(p, dtype=np.float64)
    pp = pp[np.isfinite(pp)]
    if len(pp) == 0:
        return float("nan")
    hw = max(0.0, min(0.49, float(half_width)))
    lo, hi = 0.5 - hw, 0.5 + hw
    return float(np.mean((pp >= lo) & (pp <= hi)))


def _sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(z, -60.0, 60.0)))


def calibration_regression(
    y: Any,
    p: Any,
    *,
    ridge: float = 1e-6,
    max_iter: int = 50,
    tol: float = 1e-8,
) -> dict[str, float]:
    """Fit logistic regression y ~ intercept + slope * logit(p) via IRLS/ridge.

    slope ≈ 1 and intercept ≈ 0 indicates perfect calibration.
    slope < 1 → overconfident; slope > 1 → underconfident.
    Returns default (1.0, 0.0) for degenerate inputs (too few rows, extreme class imbalance).
    """
    yy, pp = _as_arrays(y, p)
    if len(yy) < 3:
        return {"calibration_slope": 1.0, "calibration_intercept": 0.0}
    y_mean = float(np.mean(yy))
    if y_mean <= _EPS or y_mean >= 1.0 - _EPS:
        return {"calibration_slope": 1.0, "calibration_intercept": 0.0}
    x = np.log(pp / (1.0 - pp))
    X = np.column_stack([np.ones_like(x), x])
    beta = np.zeros(2, dtype=np.float64)
    eye = np.eye(2, dtype=np.float64)
    eye[0, 0] = 0.0  # don't regularize intercept
    for _ in range(int(max_iter)):
        eta = X @ beta
        mu = _sigmoid(eta)
        w = np.clip(mu * (1.0 - mu), 1e-6, None)
        z = eta + (yy - mu) / w
        XtW = X.T * w
        lhs = XtW @ X + float(ridge) * eye
        rhs = XtW @ z
        try:
            beta_new = np.linalg.solve(lhs, rhs)
        except np.linalg.LinAlgError:
            return {"calibration_slope": 1.0, "calibration_intercept": 0.0}
        if float(np.max(np.abs(beta_new - beta))) <= float(tol):
            beta = beta_new
            break
        beta = beta_new
    return {"calibration_intercept": float(beta[0]), "calibration_slope": float(beta[1])}


@dataclass(frozen=True)
class CalibrationExtendedConfig:
    bins: int = 20
    near_half_width: float = 0.05


def report(y: Any, p: Any, *, bins: int = 20, near_half_width: float = 0.05) -> dict[str, float]:
    """Full extended calibration report.

    Returns dict with keys:
        n, ece, mce, brier,
        calibration_slope, calibration_intercept,
        sharpness_mean, sharpness_entropy, prob_mass_near_half
    """
    yy, pp = _as_arrays(y, p)
    if len(yy) == 0:
        return {
            "n": 0,
            "ece": float("nan"),
            "mce": float("nan"),
            "brier": float("nan"),
            "calibration_slope": float("nan"),
            "calibration_intercept": float("nan"),
            "sharpness_mean": float("nan"),
            "sharpness_entropy": float("nan"),
            "prob_mass_near_half": float("nan"),
        }
    reg = calibration_regression(yy, pp)
    return {
        "n": int(len(yy)),
        "ece": ece(yy, pp, bins=bins),
        "mce": mce(yy, pp, bins=bins),
        "brier": brier(yy, pp),
        "calibration_slope": float(reg.get("calibration_slope", float("nan"))),
        "calibration_intercept": float(reg.get("calibration_intercept", float("nan"))),
        "sharpness_mean": sharpness_mean(pp),
        "sharpness_entropy": sharpness_entropy(pp),
        "prob_mass_near_half": prob_mass_near_half(pp, half_width=near_half_width),
    }
