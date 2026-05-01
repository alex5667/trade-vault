from __future__ import annotations

"""Pure-policy cross-venue context gate.

This gate is intentionally small and deterministic. All I/O happens in the
caller (SignalPipeline); this module only evaluates flags and decisions.

Profiles
--------
- default / monitor: annotate indicators only (no tighten, no veto).
- strict  / tighten: apply tighten_add_bps when adverse flags detected.
- hard    / veto:    tighten + optional hard veto on ≥2 independent adverse flags.

Fail-open semantics
-------------------
- stale context → caller skips gate, no flags emitted.
- Missing venues (stale_count > threshold): only emits venue_stale flag, never
  causes veto by itself (avoids veto due to venue outage).

Veto conditions (HARD mode only)
---------------------------------
- ≥2 adverse flags (venue_direction_disagree, venue_dislocation, venue_mid_spread_wide,
  trade_imbalance_against_{long|short})
- AND stale_count ≤ max_stale_count (stale venues can't be used as veto evidence)
"""

from dataclasses import dataclass
from typing import List


@dataclass
class CrossVenueDecision:
    hit: bool             # any flags detected
    mode: str             # monitor | tighten | veto
    flags: List[str]      # all detected flags
    tighten_add_bps: float
    veto: bool
    veto_reason: str


_ADVERSE_FLAGS = frozenset({
    "venue_direction_disagree",
    "venue_dislocation",
    "venue_mid_spread_wide",
    "trade_imbalance_against_long",
    "trade_imbalance_against_short",
})


def _map_profile(profile: str) -> str:
    p = str(profile or "monitor").strip().lower()
    if p in {"default", "soft", "monitor"}:
        return "monitor"
    if p in {"strict", "tighten"}:
        return "tighten"
    if p in {"hard", "veto"}:
        return "veto"
    return "monitor"


def evaluate_crossvenue_context(
    *,
    profile: str,
    side: str,
    direction_agree: float,
    trade_imbalance: float,
    dislocation_z: float,
    mid_spread_bps: float,
    stale_count: int,
    # Thresholds
    min_agree: float,
    max_dislocation_z: float,
    max_mid_spread_bps: float,
    max_stale_count: int,
    tighten_mult: float,
    tighten_cap_bps: float,
) -> CrossVenueDecision:
    """Evaluate cross-venue context against current signal direction.

    All arguments are scalars — no I/O, no side-effects.
    """
    mode = _map_profile(profile)
    side_up = str(side or "").strip().upper()
    flags: List[str] = []

    # ── Stale venues ─────────────────────────────────────────────────────────
    if stale_count > max_stale_count:
        flags.append("venue_stale")

    # ── Direction agreement ──────────────────────────────────────────────────
    if direction_agree < min_agree:
        flags.append("venue_direction_disagree")

    # ── Venue dislocation (robust-z) ─────────────────────────────────────────
    if dislocation_z > max_dislocation_z:
        flags.append("venue_dislocation")

    # ── Cross-venue mid spread ────────────────────────────────────────────────
    if mid_spread_bps > max_mid_spread_bps:
        flags.append("venue_mid_spread_wide")

    # ── Trade imbalance vs signal direction ───────────────────────────────────
    # imbalance > 0  → net buyer aggression (BUY-side pressure)
    # imbalance < 0  → net seller aggression (SELL-side pressure)
    if side_up == "BUY" and trade_imbalance < -0.15:
        flags.append("trade_imbalance_against_long")
    if side_up == "SELL" and trade_imbalance > 0.15:
        flags.append("trade_imbalance_against_short")

    # ── Adverse check (excluding venue_stale which is data-quality, not price) ─
    adverse_flags = [f for f in flags if f in _ADVERSE_FLAGS]
    adverse = bool(adverse_flags)

    # ── Tighten ───────────────────────────────────────────────────────────────
    tighten = 0.0
    if mode in {"tighten", "veto"} and adverse:
        severity = float(len(adverse_flags))
        tighten = min(float(tighten_cap_bps), severity * float(tighten_mult))

    # ── Veto (HARD mode only) ─────────────────────────────────────────────────
    # Requirements:
    #   1. mode == "veto"
    #   2. ≥2 independent ADVERSE flags (price/direction evidence, not stale)
    #   3. stale_count ≤ max_stale_count (can't veto on stale evidence)
    veto = False
    veto_reason = ""
    if mode == "veto" and len(adverse_flags) >= 2 and stale_count <= max_stale_count:
        veto = True
        veto_reason = "crossvenue_ctx:" + ",".join(adverse_flags)

    return CrossVenueDecision(
        hit=bool(flags),
        mode=mode,
        flags=flags,
        tighten_add_bps=float(tighten),
        veto=bool(veto),
        veto_reason=veto_reason,
    )
