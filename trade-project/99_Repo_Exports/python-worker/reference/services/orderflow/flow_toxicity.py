from __future__ import annotations

"""Flow toxicity metrics and enforcement helpers (Phase D / P3).

We want to detect situations when order-flow signals look strong (OFI/OBI),
*but execution is likely toxic* (adverse selection / price impact).

This module provides:
- `compute_ofi_norm_notional`: normalize best-level OFI by near-touch depth (USD) so the
  scale is comparable across symbols and regimes.
- `normal_cdf`: optional VPIN thresholding via CDF.
- `evaluate_flow_toxicity`: a deterministic policy function used by:
    - services.orderflow.signal_pipeline (LiquidityGate-like enforcement)
    - services.smt_entry_policy_service (EntryPolicy overlay)

Design principles (production):
- Fail-open by default. If inputs are missing, do not veto.
- Deterministic: no wall-clock usage inside evaluation.
- Low latency: pure math only, no Redis/DB calls.
"""


import math
from dataclasses import dataclass
from typing import Any


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def normal_cdf(z: float) -> float:
    """Standard normal CDF using erf (no scipy dependency)."""
    try:
        zf = float(z)
        if not math.isfinite(zf):
            return 0.5
        return 0.5 * (1.0 + math.erf(zf / math.sqrt(2.0)))
    except Exception:
        return 0.5


def compute_ofi_norm_notional(*, ofi_best_qty: float, mid: float, depth_usd_near: float) -> float:
    """Compute notional-normalized OFI.

    Inputs:
      ofi_best_qty: best-level OFI in *quantity units* (from book deltas).
      mid: mid price.
      depth_usd_near: near-touch depth in USD (e.g., bid+ask notional within 1bp).

    Output:
      ofi_norm: dimensionless fraction-like proxy (how large the incremental flow is
      relative to near depth).

    Notes:
      - We convert qty->USD via `ofi_best_qty * mid`.
      - Depth must be USD to make the ratio scale-invariant across symbols.
    """
    oq = _f(ofi_best_qty, 0.0)
    m = _f(mid, 0.0)
    d = _f(depth_usd_near, 0.0)
    if m <= 0.0 or d <= 0.0:
        return 0.0
    ofi_usd = oq * m
    # Bound extreme values to avoid destabilizing downstream gates.
    # In practice, ofi_norm above ~10 is already "very toxic".
    out = ofi_usd / max(d, 1e-9)
    if not math.isfinite(out):
        return 0.0
    return float(max(-50.0, min(50.0, out)))


@dataclass
class FlowToxicityDecision:
    hit: bool
    mode: str  # monitor|tighten|veto
    flags: list[str]
    tighten_add_bps: float = 0.0
    veto: bool = False
    veto_reason: str = ""


def _map_profile_to_mode(profile: str) -> str:
    p = (profile or "").strip().lower()
    if p in {"default", "soft", "monitor"}:
        return "monitor"
    if p in {"strict", "tighten"}:
        return "tighten"
    if p in {"hard", "veto"}:
        return "veto"
    return "monitor"


def evaluate_flow_toxicity(
    *,
    profile: str,
    ofi_norm_z: float,
    thr_ofi_norm_z: float,
    vpin_cdf: float,
    thr_vpin_cdf: float,
    # Optional: combine with TCA (hard-veto only when both flow toxic + exec unhealthy)
    tca_is_p95_bps: float,
    tca_perm_impact_p95_bps: float,
    thr_is_p95_bps: float,
    thr_perm_impact_p95_bps: float,
    tighten_mult: float,
    tighten_cap_bps: float,
    veto_without_tca: bool = False,
) -> FlowToxicityDecision:
    """Pure policy: decide monitor/tighten/veto from flow-toxicity inputs.

    Rules:
      - flow_bad := (ofi_norm_z > thr) OR (vpin_cdf > thr)
      - default/soft: annotate only
      - strict: tighten execution cost proxy (expected_slippage_bps)
      - hard: veto ONLY when flow_bad AND (tca_bad OR veto_without_tca)

    Tighten add (bps): proportional to *excess toxicity* beyond threshold(s), bounded.
    """

    mode = _map_profile_to_mode(profile)
    z = _f(ofi_norm_z, 0.0)
    thrz = _f(thr_ofi_norm_z, 0.0)
    vc = _f(vpin_cdf, 0.0)
    thrv = _f(thr_vpin_cdf, 0.0)

    flags: list[str] = []
    ofi_bad = bool(thrz > 0.0 and z > thrz)
    vpin_bad = bool(thrv > 0.0 and vc > thrv)
    flow_bad = bool(ofi_bad or vpin_bad)

    if ofi_bad:
        flags.append("ofi_norm_z")
    if vpin_bad:
        flags.append("vpin_cdf")

    is_p95 = _f(tca_is_p95_bps, 0.0)
    imp_p95 = _f(tca_perm_impact_p95_bps, 0.0)
    thr_is = _f(thr_is_p95_bps, 0.0)
    thr_imp = _f(thr_perm_impact_p95_bps, 0.0)

    tca_bad = bool((thr_is > 0.0 and is_p95 > thr_is) or (thr_imp > 0.0 and imp_p95 > thr_imp))
    if tca_bad:
        flags.append("tca_bad")

    # annotate-only if nothing is bad
    if not flow_bad:
        return FlowToxicityDecision(hit=False, mode=mode, flags=[])

    tighten_add = 0.0
    if mode in {"tighten", "veto"}:
        # excess from OFI z: linear beyond threshold
        ex = 0.0
        if ofi_bad and thrz > 0.0:
            ex += max(0.0, z - thrz)
        # VPIN is [0..1]; convert excess CDF into a similar scale.
        if vpin_bad and thrv > 0.0:
            ex += max(0.0, vc - thrv) * 5.0
        tighten_add = float(min(max(0.0, _f(tighten_cap_bps, 0.0)), max(0.0, _f(tighten_mult, 1.0)) * ex))

    veto = False
    veto_reason = ""
    if mode == "veto":
        if (tca_bad or veto_without_tca):
            veto = True
            veto_reason = "flow_toxic"

    return FlowToxicityDecision(
        hit=True,
        mode=mode,
        flags=flags,
        tighten_add_bps=float(tighten_add),
        veto=bool(veto),
        veto_reason=str(veto_reason),
    )
