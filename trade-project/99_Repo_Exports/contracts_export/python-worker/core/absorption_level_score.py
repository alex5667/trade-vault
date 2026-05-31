from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
import math


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return float(x)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


@dataclass
class AbsLevel:
    ok: bool
    score: float
    bias: str              # LONG/SHORT/NONE (expected bounce dir)
    dir_match: bool
    ladder_len: int
    poc_on_edge: int
    eff_delta: float
    parts: Dict[str, float]


def compute_absorption_level_score(
    *,
    bar: Any,
    direction: str,
    delta_z: float,
    weak_progress: bool,
    iceberg_strict: bool,
    reclaim_recent: bool,
    cfg: Dict[str, Any],
) -> AbsLevel:
    """
    Absorption-on-level score v2 (modular).
    Uses footprint-lite features (ladder/POC/eff) + external confirmations (iceberg/reclaim/wp/z).
    """
    bias = str(getattr(bar, "fp_absorption_bias", "NONE") or "NONE").upper()
    direction_u = str(direction).upper()
    dir_match = (bias in ("LONG", "SHORT") and bias == direction_u)

    low_len = _i(getattr(bar, "fp_ladder_low_len", 0), 0)
    high_len = _i(getattr(bar, "fp_ladder_high_len", 0), 0)
    ladder_len = max(low_len, high_len)

    poc_on_edge = _i(getattr(bar, "fp_poc_on_edge", 0), 0)
    # Prefer portable eff_quote; fallback to legacy eff_delta if missing.
    eff_q = _f(getattr(bar, "fp_eff_quote", 0.0), 0.0)
    eff = eff_q if eff_q > 0.0 else _f(getattr(bar, "fp_eff_delta", 0.0), 0.0)

    # S1: delta spike + (weak progress OR low eff)
    z_th = _f(cfg.get("abs_lvl_z_th", 2.0), 2.0)
    # threshold for eff_quote (bps per 1 USDT delta-notional)
    eff_th = _f(cfg.get("abs_lvl_eff_quote_th", 0.0020), 0.0020)

    # Optional: require minimum notional to avoid noise when delta is tiny
    min_qd = _f(cfg.get("abs_lvl_min_quote_delta", 0.0), 0.0)
    qd = _f(getattr(bar, "fp_quote_delta", 0.0), 0.0)
    notional_ok = (qd >= min_qd) if min_qd > 0 else True

    s1 = 1.0 if (abs(_f(delta_z)) >= z_th and notional_ok and (bool(weak_progress) or (eff > 0.0 and eff <= eff_th))) else 0.0

    # S2: ladder score
    ladder_norm = _f(cfg.get("abs_lvl_ladder_norm", 3.0), 3.0)
    s2 = _clamp01(float(ladder_len) / max(1.0, ladder_norm))

    # S3: iceberg
    s3 = 1.0 if bool(iceberg_strict) else 0.0

    # S4: reclaim
    s4 = 1.0 if bool(reclaim_recent) else 0.0

    # S5: poc_on_edge
    s5 = 1.0 if int(poc_on_edge) == 1 else 0.0

    w1 = _f(cfg.get("abs_lvl_w1", 0.30), 0.30)
    w2 = _f(cfg.get("abs_lvl_w2", 0.20), 0.20)
    w3 = _f(cfg.get("abs_lvl_w3", 0.20), 0.20)
    w4 = _f(cfg.get("abs_lvl_w4", 0.20), 0.20)
    w5 = _f(cfg.get("abs_lvl_w5", 0.10), 0.10)

    parts = {
        "s1_z_wp_eff": float(s1),
        "s2_ladder": float(s2),
        "s3_iceberg": float(s3),
        "s4_reclaim": float(s4),
        "s5_poc_edge": float(s5),
    }

    score = _clamp01(w1 * s1 + w2 * s2 + w3 * s3 + w4 * s4 + w5 * s5)
    th = _f(cfg.get("abs_lvl_score_th", 0.60), 0.60)

    # IMPORTANT: absorption-on-level must have a directional bias and match trade direction
    ok = bool(score >= th and dir_match)

    return AbsLevel(
        ok=ok,
        score=float(score),
        bias=bias,
        dir_match=bool(dir_match),
        ladder_len=int(ladder_len),
        poc_on_edge=int(poc_on_edge),
        eff_delta=float(eff),
        parts=parts,
    )
