from __future__ import annotations

"""Pure policy for normalized derivatives context.

This gate is intentionally small and deterministic so it can be reused by:
- services.orderflow.signal_pipeline
- services.smt_entry_policy_service
- future regime overlays

Profiles
--------
- default/soft: annotate only
- strict: tighten execution assumptions
- hard: tighten + optional veto on multi-flag crowding
"""

from dataclasses import dataclass
from typing import List


@dataclass
class DerivativesContextDecision:
    hit: bool
    mode: str
    flags: List[str]
    crowding_score: float
    tighten_add_bps: float
    veto: bool
    veto_reason: str
    caution: bool


def _map_profile(profile: str) -> str:
    p = str(profile or "default").strip().lower()
    if p in {"default", "soft", "monitor"}:
        return "monitor"
    if p in {"strict", "tighten"}:
        return "tighten"
    if p in {"hard", "veto"}:
        return "veto"
    return "monitor"


def evaluate_derivatives_context(
    *
    profile: str
    funding_rate_z: float
    basis_bps: float
    funding_extreme: int
    basis_extreme: int
    oi_accel: int
    thr_funding_z: float
    thr_basis_bps: float
    require_oi_for_veto: bool
    tighten_mult: float
    tighten_cap_bps: float
) -> DerivativesContextDecision:
    mode = _map_profile(profile)

    fz = float(funding_rate_z or 0.0)
    bb = float(basis_bps or 0.0)
    fx = int(funding_extreme or 0)
    bx = int(basis_extreme or 0)
    ox = int(oi_accel or 0)
    thr_fz = float(thr_funding_z or 0.0)
    thr_bb = float(thr_basis_bps or 0.0)

    flags: List[str] = []
    if fx or (thr_fz > 0.0 and fz >= thr_fz):
        flags.append("funding_extreme")
    if bx or (thr_bb > 0.0 and abs(bb) >= thr_bb):
        flags.append("basis_extreme")
    if ox:
        flags.append("oi_accel")

    score = float(len(flags))
    hit = bool(flags)
    caution = bool(len(flags) >= 1)

    tighten_add = 0.0
    if mode in {"tighten", "veto"} and hit:
        sev = 0.0
        if thr_fz > 0.0:
            sev = max(sev, max(0.0, fz - thr_fz))
        if thr_bb > 0.0:
            sev = max(sev, max(0.0, abs(bb) - thr_bb) / max(thr_bb, 1e-9))
        sev += 0.5 * max(0.0, score - 1.0)
        tighten_add = min(float(tighten_cap_bps or 0.0), max(0.0, float(tighten_mult or 0.0)) * sev)

    veto = False
    veto_reason = ""
    if mode == "veto" and hit:
        if require_oi_for_veto:
            veto = bool(("funding_extreme" in flags and "basis_extreme" in flags and "oi_accel" in flags))
        else:
            veto = bool(len(flags) >= 2)
        if veto:
            veto_reason = "deriv_ctx:" + ",".join(flags)

    return DerivativesContextDecision(
        hit=hit
        mode=mode
        flags=flags
        crowding_score=float(score)
        tighten_add_bps=float(tighten_add)
        veto=bool(veto)
        veto_reason=str(veto_reason)
        caution=bool(caution)
    )
