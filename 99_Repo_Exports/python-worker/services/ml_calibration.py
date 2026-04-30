from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


def clip_prob(p: float, eps: float = 1e-6) -> float:
    """
    Clip probability to [eps, 1-eps] to avoid numerical issues.
    Handles NaN by returning 0.5.
    """
    if p != p:  # NaN
        return 0.5
    if p < eps:
        return eps
    if p > 1.0 - eps:
        return 1.0 - eps
    return p


def logit(p: float) -> float:
    """
    Logit transform: log(p / (1-p))
    """
    p = clip_prob(p)
    return math.log(p / (1.0 - p))


def sigmoid(x: float) -> float:
    """
    Stable sigmoid: 1 / (1 + exp(-x))
    Uses numerically stable computation to avoid overflow.
    """
    # stable sigmoid
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


@dataclass
class PlattLogitCalibrator:
    """
    Calibrator on probability space using logit(p):
      p_cal = sigmoid(a * logit(p_raw) + b)

    Это эквивалент "temperature scaling" (если b=0) и Platt scaling (если a,b оба обучаемые)
    но работает напрямую по p_raw (не требуя доступа к исходным logits модели).

    Parameters:
        a: slope parameter (default 1.0 = no scaling)
        b: intercept parameter (default 0.0 = no shift)
        eps: clipping epsilon for probabilities
    """
    a: float = 1.0
    b: float = 0.0
    eps: float = 1e-6

    def apply_one(self, p_raw: float) -> float:
        """
        Apply calibration to a single probability.
        """
        lr = logit(clip_prob(float(p_raw), self.eps))
        return sigmoid(self.a * lr + self.b)

    def apply(self, probs: List[float]) -> List[float]:
        """
        Apply calibration to a list of probabilities.
        """
        return [self.apply_one(p) for p in probs]

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize calibrator to dict for storage in Redis cfg.
        """
        return {"type": "platt_logit", "a": float(self.a), "b": float(self.b), "eps": float(self.eps)}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "PlattLogitCalibrator":
        """
        Deserialize calibrator from dict (loaded from Redis cfg).
        """
        return PlattLogitCalibrator(
            a=float(d.get("a", 1.0) or 1.0)
            b=float(d.get("b", 0.0) or 0.0)
            eps=float(d.get("eps", 1e-6) or 1e-6)
        )


def brier_score(probs: List[float], y: List[int]) -> float:
    """
    Brier score: mean squared error between probabilities and binary targets.
    Lower is better. Range: [0, 1].
    """
    if not probs:
        return 0.0
    s = 0.0
    n = 0
    for p, t in zip(probs, y):
        pp = float(p)
        tt = 1.0 if int(t) == 1 else 0.0
        s += (pp - tt) * (pp - tt)
        n += 1
    return s / float(n) if n > 0 else 0.0


def logloss(probs: List[float], y: List[int], eps: float = 1e-6) -> float:
    """
    Log loss (binary cross-entropy): -mean(y*log(p) + (1-y)*log(1-p))
    Lower is better. Range: [0, +inf).
    """
    if not probs:
        return 0.0
    s = 0.0
    n = 0
    for p, t in zip(probs, y):
        pp = clip_prob(float(p), eps)
        tt = 1.0 if int(t) == 1 else 0.0
        s += -(tt * math.log(pp) + (1.0 - tt) * math.log(1.0 - pp))
        n += 1
    return s / float(n) if n > 0 else 0.0


def ece_score(probs: List[float], y: List[int], n_bins: int = 15) -> Tuple[float, List[Dict[str, float]]]:
    """
    Expected Calibration Error (ECE): sum_k (|acc_k - conf_k| * (n_k / n))
    
    ECE measures how well-calibrated probabilities are:
    - acc_k: actual accuracy in bin k
    - conf_k: average predicted confidence in bin k
    - n_k: number of samples in bin k
    
    Returns:
        (ece, bins) where bins is a list of dicts with keys: n, conf, acc
        for reliability report visualization.
    """
    if not probs:
        return 0.0, []
    n = len(probs)
    bins = [{"n": 0, "conf": 0.0, "acc": 0.0} for _ in range(n_bins)]
    for p, t in zip(probs, y):
        pp = clip_prob(float(p))
        idx = int(pp * n_bins)
        if idx == n_bins:
            idx = n_bins - 1
        bins[idx]["n"] += 1
        bins[idx]["conf"] += pp
        bins[idx]["acc"] += 1.0 if int(t) == 1 else 0.0
    ece = 0.0
    out_bins: List[Dict[str, float]] = []
    for b in bins:
        nk = int(b["n"])
        if nk <= 0:
            continue
        conf = float(b["conf"]) / float(nk)
        acc = float(b["acc"]) / float(nk)
        w = float(nk) / float(n)
        ece += abs(acc - conf) * w
        out_bins.append({"n": float(nk), "conf": float(conf), "acc": float(acc)})
    return float(ece), out_bins


def fit_platt_logit(probs: List[float], y: List[int], *, l2: float = 1e-3, max_iter: int = 50) -> PlattLogitCalibrator:
    """
    Fit Platt scaling on logit space using scipy.optimize.minimize.
    
    Minimizes: NLL + 0.5*l2*(a^2+b^2)
    
    Uses L-BFGS-B optimization on feature x=logit(p_raw) to predict y
    avoiding the divergence risks of unbounded Newton iteration on highly separated data.
    
    Parameters:
        probs: raw probabilities from model (list of floats in [0,1])
        y: binary targets (list of ints: 0 or 1)
        l2: L2 regularization strength (default 1e-3)
        max_iter: maximum iterations for optimization (default 50)
    
    Returns:
        Fitted PlattLogitCalibrator with optimized a and b parameters.
    """
    if not probs:
        return PlattLogitCalibrator()

    import numpy as np
    from scipy.optimize import minimize
    
    xs = np.array([logit(clip_prob(float(p))) for p in probs], dtype=np.float64)
    ts = np.array([1.0 if int(t) == 1 else 0.0 for t in y], dtype=np.float64)
    
    def loss(w):
        a, b = w
        z = a * xs + b
        p = 1.0 / (1.0 + np.exp(-z))
        p = np.clip(p, 1e-15, 1.0 - 1e-15)
        return -np.mean(ts * np.log(p) + (1.0 - ts) * np.log(1.0 - p)) + 0.5 * l2 * (a * a + b * b)

    res = minimize(loss, [1.0, 0.0], method='L-BFGS-B', options={'maxiter': max_iter})
    return PlattLogitCalibrator(a=float(res.x[0]), b=float(res.x[1]))















