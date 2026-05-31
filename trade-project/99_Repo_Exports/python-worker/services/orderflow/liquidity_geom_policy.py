from __future__ import annotations

"""services.orderflow.liquidity_geom_policy

Shared, pure logic for Phase C (P2) liquidity geometry/resiliency gating.

This module is intentionally dependency-free so it can be unit-tested and reused
in both:
  - services.orderflow.signal_pipeline (LiquidityGate)
  - services.smt_entry_policy_service (EntryPolicyGate overlay)

Profiles:
  - default/soft: annotate only
  - strict: tighten only
  - hard: tighten + veto
"""


from dataclasses import dataclass


@dataclass
class LiqGeomDecision:
    flags: list[str]
    slope_min: float
    tighten_add_bps: float
    veto: bool
    veto_reason: str


def slope_min(slope_bid: float, slope_ask: float) -> float:
    """Return min of bid/ask slopes, ignoring zeroes.

    If both are zero (unknown), returns 0.0.
    Uses sentinel 1e18 to treat 0 as 'missing' rather than 'flat'.
    """
    sb = float(slope_bid or 0.0)
    sa = float(slope_ask or 0.0)
    out = min(sb if sb > 0 else 1e18, sa if sa > 0 else 1e18)
    return 0.0 if out >= 1e18 else float(out)


def evaluate_liq_geom(
    *,
    profile: str,
    slope_bid: float,
    slope_ask: float,
    dws_bps: float,
    recovery_ms: int,
    thr_slope: float,
    thr_dws: float,
    thr_recovery_ms: int,
    tighten_cap_bps: float,
    tighten_mult: float,
) -> LiqGeomDecision:
    """Return flags + tighten add + veto decision.

    Thresholds are considered disabled when <= 0.
    Tighten add is bounded to [0..cap].

    Severity-based tighten formula:
      sev = max(individual_severities) in [0..1]
      add = min(mult * cap * sev, cap)

    This is intentionally conservative: uses the most severe single breach
    rather than summing breaches, to avoid over-penalizing correlated signals.
    """
    p = (profile or "default").strip().lower()
    if p not in {"default", "soft", "strict", "hard"}:
        p = "default"

    sb = float(slope_bid or 0.0)
    sa = float(slope_ask or 0.0)
    smin = slope_min(sb, sa)
    dws = float(dws_bps or 0.0)
    rec = int(recovery_ms or 0)
    ts = float(thr_slope or 0.0)
    td = float(thr_dws or 0.0)
    tr = int(thr_recovery_ms or 0)

    flags: list[str] = []
    # Slope flag: only if slope is known (>0) and below threshold
    if ts > 0.0 and smin > 0.0 and smin < ts:
        flags.append("slope_low")
    # DWS flag: only if dws is known (>0) and above threshold
    if td > 0.0 and dws > 0.0 and dws > td:
        flags.append("dws_high")
    # Recovery flag: only if recovery timeout set and exceeded
    if tr > 0 and rec > tr:
        flags.append("recovery_slow")

    cap = max(0.0, float(tighten_cap_bps or 0.0))
    mult = max(0.0, float(tighten_mult or 0.0))

    # Compute severity per flag type (bounded to [0..1])
    sev = 0.0
    if ts > 0.0 and smin > 0.0 and smin < ts:
        sev = max(sev, min(1.0, (ts - smin) / max(ts, 1e-9)))
    if td > 0.0 and dws > td:
        sev = max(sev, min(1.0, (dws - td) / max(td, 1e-9)))
    if tr > 0 and rec > tr:
        sev = max(sev, min(1.0, (rec - tr) / max(float(tr), 1.0)))

    # Tighten: only in strict/hard and only if flags raised
    add = max(0.0, min(mult * cap * sev, cap)) if (flags and p in {"strict", "hard"}) else 0.0
    veto = bool(flags and p == "hard")
    veto_reason = "liq_geom:" + ",".join(flags) if veto else ""
    return LiqGeomDecision(
        flags=flags,
        slope_min=float(smin),
        tighten_add_bps=float(add),
        veto=veto,
        veto_reason=veto_reason,
    )
