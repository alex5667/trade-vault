from __future__ import annotations

"""Pure policy for DefiLlama slow macro/liquidity context.

This gate is intentionally small and deterministic. DefiLlama data is
aggregated on-chain DeFi context, NOT a tick-level entry trigger.

Profiles
--------
- default/monitor: annotate only (flags in indicators)
- strict/tighten: tighten execution cost assumptions
- hard/veto: tighten + optional veto on multi-flag adverse regime
"""

from dataclasses import dataclass
from typing import List


@dataclass
class DefiLlamaContextDecision:
    hit: bool
    mode: str
    flags: List[str]
    risk_score: float
    tighten_add_bps: float
    veto: bool
    veto_reason: str


def _map_profile(profile: str) -> str:
    p = str(profile or "monitor").strip().lower()
    if p in {"default", "soft", "monitor"}:
        return "monitor"
    if p in {"strict", "tighten"}:
        return "tighten"
    if p in {"hard", "veto"}:
        return "veto"
    return "monitor"


def evaluate_defillama_context(
    *
    profile: str
    side: str
    stablecoin_mcap_delta_1d: float
    stablecoin_mcap_delta_7d: float
    btc_dominance_momentum: float
    chain_tvl_delta_1d_pct: float
    dex_volume_spike_z: float
    fees_revenue_momentum: float
    tighten_mult: float
    tighten_cap_bps: float
) -> DefiLlamaContextDecision:
    mode = _map_profile(profile)
    side_up = str(side or "").strip().upper()

    flags: List[str] = []

    # Global stablecoin + BTC dominance regime
    if stablecoin_mcap_delta_1d > 0 and stablecoin_mcap_delta_7d > 0 and btc_dominance_momentum < 0:
        flags.append("alt_risk_on")

    if stablecoin_mcap_delta_1d < 0 and stablecoin_mcap_delta_7d < 0 and btc_dominance_momentum > 0:
        flags.append("risk_off")

    # Chain-specific context
    if chain_tvl_delta_1d_pct < -1.0:
        flags.append("chain_tvl_down")

    if dex_volume_spike_z >= 2.0:
        flags.append("dex_volume_spike")

    if fees_revenue_momentum > 0:
        flags.append("ecosystem_activity_up")

    # Side-specific risk
    adverse = False
    if side_up == "BUY" and ("risk_off" in flags or "chain_tvl_down" in flags):
        adverse = True
    if side_up == "SELL" and "alt_risk_on" in flags:
        adverse = True

    tighten = 0.0
    if mode in {"tighten", "veto"} and adverse:
        tighten = min(float(tighten_cap_bps), float(tighten_mult) * 2.0)

    # DefiLlama is slow context: veto only with hard + multiple adverse flags
    veto = False
    reason = ""
    if mode == "veto" and adverse and len(flags) >= 2:
        veto = True
        reason = "defillama_ctx:" + ",".join(flags)

    return DefiLlamaContextDecision(
        hit=bool(flags)
        mode=mode
        flags=flags
        risk_score=float(len(flags))
        tighten_add_bps=float(tighten)
        veto=bool(veto)
        veto_reason=reason
    )
