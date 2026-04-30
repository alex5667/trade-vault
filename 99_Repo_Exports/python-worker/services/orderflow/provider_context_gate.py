from __future__ import annotations

"""Provider context gate — pure policy, deterministic, no I/O.

This gate acts on slow market-context data (CoinPaprika / CMC fallback).

Rules
-----
- Provider fallback → NO hard veto; only caution / tighten flags.
- Provider disagreement → tighten when profile in {strict, tighten, hard}.
- Missing provider context → fail-open (veto=False, no tighten).
- Top loser on BUY / top gainer on SELL → warning flag only.
"""

from typing import Dict, List


def evaluate_provider_context(
    *
    profile: str
    side: str
    provider_quality: str
    mcap_disagreement_bps: float
    volume_disagreement_bps: float
    btc_dom_disagreement_bps: float
    provider_btc_dominance: float
    provider_rel_strength_24h: float
    provider_top_gainer: int
    provider_top_loser: int
    max_disagreement_bps: float = 100.0
    tighten_mult: float = 1.0
    tighten_cap_bps: float = 4.0
) -> Dict:
    """Evaluate provider context and return a decision dict.

    Returns
    -------
    dict with keys:
        flags            : List[str]
        tighten_add_bps  : float  (0 if no tighten)
        veto             : bool   (always False — provider ctx never hard-veto)
        veto_reason      : str
    """
    _profile = str(profile or "monitor").strip().lower()
    _side = str(side or "").strip().upper()
    flags: List[str] = []

    # Provider quality degradation
    if provider_quality == "fallback":
        flags.append("provider_fallback_active")
    elif provider_quality == "degraded":
        flags.append("provider_data_degraded")
    elif provider_quality == "unknown":
        flags.append("provider_quality_unknown")

    # Cross-provider disagreement
    if mcap_disagreement_bps > max_disagreement_bps:
        flags.append("mcap_provider_disagreement")
    if volume_disagreement_bps > max_disagreement_bps:
        flags.append("volume_provider_disagreement")
    if btc_dom_disagreement_bps > max_disagreement_bps:
        flags.append("btc_dom_provider_disagreement")

    # Side-specific universe flags (informational)
    if _side == "BUY" and provider_top_loser:
        flags.append("symbol_top_loser_against_long")
    if _side == "SELL" and provider_top_gainer:
        flags.append("symbol_top_gainer_against_short")

    # Count adverse disagreement flags (excludes informational/fallback flags)
    adverse_flags = [
        f for f in flags
        if f.endswith("disagreement") or f == "provider_data_degraded"
    ]
    adverse = bool(adverse_flags)

    # Tighten only in strict/tighten/hard profiles when adverse
    tighten = 0.0
    if _profile in {"strict", "tighten", "hard"} and adverse:
        tighten = min(float(tighten_cap_bps), len(adverse_flags) * float(tighten_mult))

    # Provider fallback NEVER hard-veto
    return {
        "flags": flags
        "tighten_add_bps": round(tighten, 4)
        "veto": False
        "veto_reason": ""
    }
