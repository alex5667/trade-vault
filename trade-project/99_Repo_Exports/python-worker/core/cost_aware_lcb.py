from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


@dataclass
class ArmStats:
    arm: str
    n: int
    mean: float
    std: float
    stderr: float
    lcb: float


def compute_r_adj(payload: dict[str, Any]) -> float:
    """Cost-aware realized result in R-units.

    r_adj = r_mult - slip_R - fees_R

    slip_usd = turnover_roundtrip * (expected_slippage_bps/10000)
    slip_R = slip_usd / risk_usd

    fees_R is guarded by env toggles to prevent double-counting.
    """
    r_mult = _f(payload.get("r_mult"), 0.0)
    risk_usd = max(1e-9, _f(payload.get("risk_usd"), 0.0))

    turn = _f(payload.get("turnover_roundtrip"), 0.0)

    slip_bps = _f(payload.get("p0_slippage_bps_est"), 0.0)
    if slip_bps <= 0:
        slip_bps = _f(payload.get("expected_slippage_bps_at_entry"), 0.0)
    if slip_bps <= 0:
        slip_bps = _f(payload.get("expected_slippage_bps"), 0.0)

    cap = float(os.getenv("LCB_SLIPPAGE_BPS_CAP", "250"))
    if cap > 0:
        slip_bps = max(0.0, min(slip_bps, cap))

    slip_usd = max(0.0, turn) * (max(0.0, slip_bps) / 10000.0)
    slip_R = slip_usd / risk_usd

    fees_usd = _f(payload.get("fees_usd"), 0.0)
    fees_already_net = os.getenv("LCB_FEES_ALREADY_NET", "1") == "1"
    subtract_fees = os.getenv("LCB_SUBTRACT_FEES", "0") == "1"
    fees_R = (fees_usd / risk_usd) if (subtract_fees and not fees_already_net) else 0.0

    return float(r_mult - slip_R - fees_R)


def lcb_from_samples(xs: list[float], *, z: float) -> tuple[float, float, float, float]:
    """Returns (mean, std, stderr, lcb) for samples xs."""
    n = len(xs)
    if n <= 0:
        return (0.0, 0.0, float("inf"), float("-inf"))
    mean = sum(xs) / float(n)
    if n == 1:
        return (mean, 0.0, float("inf"), mean)
    var = sum((x - mean) ** 2 for x in xs) / float(n - 1)
    std = math.sqrt(max(0.0, var))
    stderr = std / math.sqrt(float(n))
    lcb = mean - z * stderr
    return (mean, std, stderr, lcb)


def compute_arm_stats(samples_by_arm: dict[str, list[float]], *, z: float, min_n: int, floor: float) -> list[ArmStats]:
    out: list[ArmStats] = []
    for arm, xs in samples_by_arm.items():
        n = len(xs)
        if n < min_n:
            continue
        mean, std, stderr, lcb = lcb_from_samples(xs, z=z)
        if lcb < floor:
            continue
        out.append(ArmStats(arm=str(arm), n=n, mean=mean, std=std, stderr=stderr, lcb=lcb))
    out.sort(key=lambda a: a.lcb, reverse=True)
    return out

