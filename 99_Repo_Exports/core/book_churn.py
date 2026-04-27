from __future__ import annotations

from dataclasses import dataclass


def clip01(x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    return float(x)


@dataclass
class ChurnScore:
    """
    Derived churn score from robust z of book-update rate.
    """
    rate_hz: float
    rate_z: float
    churn_score: float
    churn_hi: int


def compute_churn_from_z(*, rate_hz: float, rate_z: float, z_start: float, z_full: float, z_hi: float) -> ChurnScore:
    """
    churn_score in [0..1] grows when rate_z exceeds z_start and reaches 1 at z_full.
    churn_hi is a boolean threshold for hard gating logic.
    """
    zs = float(z_start)
    zf = float(z_full) if float(z_full) > float(z_start) else float(z_start) + 1.0
    p = 0.0
    if float(rate_z) > zs:
        p = clip01((float(rate_z) - zs) / (zf - zs))
    hi = 1 if float(rate_z) >= float(z_hi) else 0
    return ChurnScore(rate_hz=float(rate_hz), rate_z=float(rate_z), churn_score=float(p), churn_hi=int(hi))
