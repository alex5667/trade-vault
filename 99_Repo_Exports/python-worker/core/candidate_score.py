from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _clip01(x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    return float(x)


@dataclass
class CandidateScore:
    score: float
    comp: dict[str, float]
    veto: str = ""  # optional hard veto reason


def compute_candidate_score(
    *,
    of_confirm_score: float,
    delta_z: float,
    confirmations: list[str],
    spread_z: float,
    spread_bps: float,
    book_stale_ms: int,
    book_rate_z: float,
    cfg: dict[str, Any],
    pressure_hi: bool,
) -> CandidateScore:
    """
    Unified scoring for burst top-1 selection.

    Base:
      - of_confirm_score (0..1) strongest
      - delta_z strength (scaled)
      - bonuses from confirmations
    Penalties:
      - spread_z (robust) and/or spread_bps (absolute)
      - book_stale_ms (freshness)
    Optional hard veto when pressure_hi.
    """
    comp: dict[str, float] = {}
    score = 0.0

    # Base
    s_of = float(of_confirm_score or 0.0)
    score += s_of
    comp["of"] = s_of

    dz = float(delta_z or 0.0)
    s_dz = _clip01(abs(dz) / 3.0) * float(cfg.get("cand_w_delta_z", 0.25))
    score += s_dz
    comp["delta_z"] = s_dz

    # Confirmation bonuses (robust: presence-only)
    b = 0.0
    if any(c.startswith("obi_stable=") for c in confirmations):
        b += float(cfg.get("cand_b_obi", 0.20))
    if "iceberg_strict=1" in confirmations:
        b += float(cfg.get("cand_b_ice", 0.20))
    if any(c.startswith("absorption=") for c in confirmations):
        b += float(cfg.get("cand_b_absorption", 0.10))
    if any(c.startswith("reclaim=") for c in confirmations) or any(c == "reclaim=1" for c in confirmations):
        b += float(cfg.get("cand_b_reclaim", 0.10))
    score += b
    comp["bonuses"] = b

    # Penalties: spread_z
    z0 = float(cfg.get("spread_z_penalty_start", 2.0))
    z1 = float(cfg.get("spread_z_penalty_full", 4.0))
    pz = 0.0
    if spread_z > z0:
        pz = _clip01((float(spread_z) - z0) / max(1e-9, (z1 - z0))) * float(cfg.get("cand_p_spread_z", 0.25))
        score -= pz
    comp["pen_spread_z"] = -pz

    # Penalties: absolute spread bps (fallback if z not stable)
    sb0 = float(cfg.get("spread_bps_penalty_start", 8.0))
    sb1 = float(cfg.get("spread_bps_penalty_full", 20.0))
    pb = 0.0
    if spread_bps > sb0:
        pb = _clip01((float(spread_bps) - sb0) / max(1e-9, (sb1 - sb0))) * float(cfg.get("cand_p_spread_bps", 0.15))
        score -= pb
    comp["pen_spread_bps"] = -pb

    # Penalties: book staleness
    st0 = int(cfg.get("book_stale_penalty_start_ms", 800))
    st1 = int(cfg.get("book_stale_penalty_full_ms", 2500))
    ps = 0.0
    if book_stale_ms > st0:
        ps = _clip01((float(book_stale_ms) - float(st0)) / max(1.0, float(st1 - st0))) * float(cfg.get("cand_p_book_stale", 0.20))
        score -= ps
    comp["pen_book_stale"] = -ps

    # Penalty: book churn (high update-rate z)
    cz0 = float(cfg.get("book_rate_z_penalty_start", 2.0))
    cz1 = float(cfg.get("book_rate_z_penalty_full", 5.0))
    pc = 0.0
    if float(book_rate_z) > cz0:
        pc = _clip01((float(book_rate_z) - cz0) / max(1e-9, (cz1 - cz0))) * float(cfg.get("cand_p_book_churn", 0.15))
        score -= pc
    comp["pen_book_churn_z"] = -pc

    # Hard veto only under high pressure (avoid noisy trading in thin/news)
    if pressure_hi:
        zmax = float(cfg.get("pressure_spread_z_max", 4.0))
        smax = int(cfg.get("pressure_book_stale_max_ms", 2000))
        cmax = float(cfg.get("pressure_book_rate_z_max", 6.0))
        if spread_z >= zmax:
            return CandidateScore(score=float(score), comp=comp, veto="PRESSURE_VETO_SPREAD_Z")
        if book_stale_ms >= smax:
            return CandidateScore(score=float(score), comp=comp, veto="PRESSURE_VETO_BOOK_STALE")
        if float(book_rate_z) >= cmax:
            return CandidateScore(score=float(score), comp=comp, veto="PRESSURE_VETO_BOOK_CHURN")

    # --- Adverse selection penalty (expected slippage in bps) ---
    try:
        slip = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)  # type: ignore
        w_slip = float(cfg.get("w_slip", 0.08))  # score penalty per bps
        score -= w_slip * slip
    except Exception:
        pass

    # --- Data health multiplier (fail-open for stream, but reduce rank) ---
    try:
        dh = float(indicators.get("data_health", 1.0) or 1.0)  # type: ignore
        score *= max(0.25, min(1.0, dh))
    except Exception:
        pass

    return CandidateScore(score=float(score), comp=comp, veto="")
