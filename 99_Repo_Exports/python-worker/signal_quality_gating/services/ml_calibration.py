from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


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

    Это эквивалент "temperature scaling" (если b=0) и Platt scaling (если a,b оба обучаемые),
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

    def apply(self, probs: list[float]) -> list[float]:
        """
        Apply calibration to a list of probabilities.
        """
        return [self.apply_one(p) for p in probs]

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize calibrator to dict for storage in Redis cfg.
        """
        return {"type": "platt_logit", "a": float(self.a), "b": float(self.b), "eps": float(self.eps)}

    @staticmethod
    def from_dict(d: dict[str, Any]) -> PlattLogitCalibrator:
        """
        Deserialize calibrator from dict (loaded from Redis cfg).
        """
        return PlattLogitCalibrator(
            a=float(d.get("a", 1.0) or 1.0),
            b=float(d.get("b", 0.0) or 0.0),
            eps=float(d.get("eps", 1e-6) or 1e-6),
        )


def brier_score(probs: list[float], y: list[int]) -> float:
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


def logloss(probs: list[float], y: list[int], eps: float = 1e-6) -> float:
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


def ece_score(probs: list[float], y: list[int], n_bins: int = 15) -> tuple[float, list[dict[str, float]]]:
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
    out_bins: list[dict[str, float]] = []
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


def fit_platt_logit(probs: list[float], y: list[int], *, l2: float = 1e-3, max_iter: int = 50) -> PlattLogitCalibrator:
    """
    Fit Platt scaling on logit space using IRLS / Newton-like optimization.
    
    Minimizes: NLL + 0.5*l2*(a^2+b^2)
    
    This is deterministic, fast, and has no sklearn dependency.
    Uses logistic regression on feature x=logit(p_raw) to predict y.
    
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

    xs = [logit(clip_prob(float(p))) for p in probs]
    ts = [1.0 if int(t) == 1 else 0.0 for t in y]
    a = 1.0
    b = 0.0

    for _ in range(int(max_iter)):
        # gradients and Hessian (2x2)
        g_a = l2 * a
        g_b = l2 * b
        h_aa = l2
        h_ab = 0.0
        h_bb = l2

        for x, t in zip(xs, ts):
            z = a * x + b
            p = sigmoid(z)
            w = p * (1.0 - p)
            # grad
            g_a += (p - t) * x
            g_b += (p - t)
            # Hessian
            h_aa += w * x * x
            h_ab += w * x
            h_bb += w

        # solve H * delta = g
        det = h_aa * h_bb - h_ab * h_ab
        if det <= 1e-12:
            break
        da = ( g_a * h_bb - g_b * h_ab) / det
        db = (-g_a * h_ab + g_b * h_aa) / det

        # step
        a -= da
        b -= db

        if abs(da) + abs(db) < 1e-8:
            break

    return PlattLogitCalibrator(a=float(a), b=float(b))











