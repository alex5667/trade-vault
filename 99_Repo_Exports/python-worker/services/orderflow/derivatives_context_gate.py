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


@dataclass
class DerivativesContextDecision:
    hit: bool
    mode: str
    flags: list[str]
    crowding_score: float
    tighten_add_bps: float
    veto: bool
    veto_reason: str
    caution: bool


def _map_profile(profile: str) -> str:
    p = (profile or "default").strip().lower()
    if p in {"default", "soft", "monitor"}:
        return "monitor"
    if p in {"strict", "tighten"}:
        return "tighten"
    if p in {"hard", "veto"}:
        return "veto"
    return "monitor"


def evaluate_derivatives_context(
    *,
    profile: str,
    funding_rate_z: float,
    basis_bps: float,
    funding_extreme: int,
    basis_extreme: int,
    oi_accel: int,
    thr_funding_z: float,
    thr_basis_bps: float,
    require_oi_for_veto: bool,
    tighten_mult: float,
    tighten_cap_bps: float,
) -> DerivativesContextDecision:
    mode = _map_profile(profile)

    fz = float(funding_rate_z or 0.0)
    bb = float(basis_bps or 0.0)
    fx = int(funding_extreme or 0)
    bx = int(basis_extreme or 0)
    ox = int(oi_accel or 0)
    thr_fz = float(thr_funding_z or 0.0)
    thr_bb = float(thr_basis_bps or 0.0)

    flags: list[str] = []
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
            veto = bool("funding_extreme" in flags and "basis_extreme" in flags and "oi_accel" in flags)
        else:
            veto = bool(len(flags) >= 2)
        if veto:
            veto_reason = "deriv_ctx:" + ",".join(flags)

    return DerivativesContextDecision(
        hit=hit,
        mode=mode,
        flags=flags,
        crowding_score=float(score),
        tighten_add_bps=float(tighten_add),
        veto=bool(veto),
        veto_reason=str(veto_reason),
        caution=bool(caution),
    )


# ─── v2 gate ─────────────────────────────────────────────────────────────────
# evaluate_derivatives_context_v2 extends v1 with:
#   - crowding (long_short_ratio_z)
#   - taker imbalance (taker_buy_sell_imbalance)
#   - breadth confirmation (market_breadth_ret_24h, leader_btc_eth_confirm)
#   - liquidation stress (liq_imbalance_z)
#
# Backward compat: v1 gate unchanged. v2 is additive (new fields, new flags).
# Veto: only hard profile + multi-flag crowding (not on single breadth/liq flag).

def evaluate_derivatives_context_v2(
    *,
    profile: str,
    side: str,
    funding_rate_z: float,
    basis_bps: float,
    oi_accel: int,
    long_short_ratio_z: float = 0.0,
    taker_buy_sell_imbalance: float = 0.0,
    liq_imbalance_z: float = 0.0,
    market_breadth_ret_24h: float = 0.0,
    leader_btc_eth_confirm: float = 0.0,
    # v1 threshold params with defaults
    thr_funding_z: float = 3.0,
    thr_basis_bps: float = 10.0,
    require_oi_for_veto: bool = True,
    tighten_mult: float = 1.0,
    tighten_cap_bps: float = 8.0,
) -> DerivativesContextDecision:
    mode = _map_profile(profile)
    side_up = (side or "").strip().upper()

    fz = float(funding_rate_z or 0.0)
    bb = float(basis_bps or 0.0)
    ox = int(oi_accel or 0)
    lsz = float(long_short_ratio_z or 0.0)
    taker = float(taker_buy_sell_imbalance or 0.0)
    liq_z = float(liq_imbalance_z or 0.0)
    breadth = float(market_breadth_ret_24h or 0.0)
    leader = float(leader_btc_eth_confirm or 0.0)

    flags: list[str] = []

    # Core flags (same as v1)
    if abs(fz) >= thr_funding_z:
        flags.append("funding_extreme")
    if abs(bb) >= thr_basis_bps:
        flags.append("basis_extreme")
    if ox:
        flags.append("oi_accel")

    # Crowding flags (side-aware)
    if side_up == "BUY" and lsz >= 2.5:
        flags.append("long_crowded")
    if side_up == "SELL" and lsz <= -2.5:
        flags.append("short_crowded")

    # Breadth divergence flags
    if side_up == "BUY" and breadth < -0.01:
        flags.append("breadth_against_long")
    if side_up == "SELL" and breadth > 0.01:
        flags.append("breadth_against_short")

    # Leader divergence
    if leader < 0:
        flags.append("leader_diverged")

    # Liquidation stress (context-sensitive, not automatic veto)
    if abs(liq_z) >= 3.0:
        flags.append("liq_stress")

    score = float(len(flags))
    hit = bool(flags)
    caution = hit

    # Tighten logic: severity based on distance from threshold
    tighten_add = 0.0
    if mode in {"tighten", "veto"} and hit:
        sev = 0.0
        if thr_funding_z > 0.0:
            sev = max(sev, max(0.0, abs(fz) - thr_funding_z))
        if thr_basis_bps > 0.0:
            sev = max(sev, max(0.0, abs(bb) - thr_basis_bps) / max(thr_basis_bps, 1e-9))
        # Each extra flag above 1 adds severity
        sev += 0.5 * max(0.0, score - 1.0)
        tighten_add = min(
            float(tighten_cap_bps),
            max(0.0, float(tighten_mult)) * sev,
        )

    # Veto: only in hard/veto mode, requires multi-flag crowding
    # breadth/liq/leader alone never cause veto (context-only)
    veto = False
    veto_reason = ""
    core_flags = {"funding_extreme", "basis_extreme", "oi_accel", "long_crowded", "short_crowded"}
    core_hit = [f for f in flags if f in core_flags]

    if mode == "veto" and hit:
        if require_oi_for_veto:
            veto = bool(
                "funding_extreme" in flags
                and "basis_extreme" in flags
                and "oi_accel" in flags
            )
        else:
            veto = bool(len(core_hit) >= 2)
        if veto:
            veto_reason = "deriv_ctx:" + ",".join(flags)

    return DerivativesContextDecision(
        hit=hit,
        mode=mode,
        flags=flags,
        crowding_score=float(score),
        tighten_add_bps=float(tighten_add),
        veto=bool(veto),
        veto_reason=str(veto_reason),
        caution=bool(caution),
    )

