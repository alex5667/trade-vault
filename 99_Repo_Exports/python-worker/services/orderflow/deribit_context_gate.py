from dataclasses import dataclass
from typing import List


@dataclass
class DeribitContextDecision:
    hit: bool
    mode: str
    flags: List[str]
    tighten_add_bps: float
    risk_multiplier: float
    veto: bool
    veto_reason: str


def evaluate_deribit_context(
    *,
    profile: str,
    side: str,
    vol_regime: str,
    iv_z: float,
    funding_8h: float,
    basis_bps: float,
    tighten_cap_bps: float,
) -> DeribitContextDecision:
    """
    Evaluate Deribit volatility context and produce a risk/tighten decision.

    Key constraints:
    - NO hard veto from Deribit context (fail-open architecture).
    - Only adds tighten_add_bps and reduces risk_multiplier.
    - In monitor mode: flags only, no tightening applied.
    - In tighten mode: applies execution caution based on regime.

    Regimes:
      vol_stress     → risk_multiplier=0.60, tighten=up to 6bps
      vol_expansion  → risk_multiplier=0.80, tighten=up to 3bps
      vol_compression → mark only, no tighten (breakout caution)
      normal         → no action
    """
    mode = {
        "default": "monitor",
        "monitor": "monitor",
        "soft": "monitor",
        "strict": "tighten",
        "tighten": "tighten",
        "hard": "tighten",  # intentionally no hard veto for Deribit
    }.get(str(profile or "monitor").lower(), "monitor")

    flags: List[str] = []

    regime = str(vol_regime or "unknown").lower()

    # Regime flags
    if regime == "vol_stress":
        flags.append("deribit_vol_stress")
    elif regime == "vol_expansion":
        flags.append("deribit_vol_expansion")
    elif regime == "vol_compression":
        flags.append("deribit_vol_compression")

    # IV z-score flags
    if iv_z >= 3.0:
        flags.append("deribit_iv_extreme")
    elif iv_z >= 1.5:
        flags.append("deribit_iv_high")

    # Funding extreme flag
    if abs(funding_8h) >= 0.001:
        flags.append("deribit_funding_extreme")

    # Basis wide flag (>20bps signals mark/index divergence)
    if abs(basis_bps) >= 20.0:
        flags.append("deribit_basis_wide")

    tighten = 0.0
    risk_mult = 1.0

    if mode == "tighten":
        if "deribit_vol_stress" in flags or "deribit_iv_extreme" in flags:
            tighten = min(float(tighten_cap_bps), 6.0)
            risk_mult = 0.60
        elif "deribit_vol_expansion" in flags or "deribit_iv_high" in flags:
            tighten = min(float(tighten_cap_bps), 3.0)
            risk_mult = 0.80
        elif "deribit_vol_compression" in flags:
            # Compression: mark it but don't tighten; breakout needs real-time OF confirm.
            tighten = 0.0
            risk_mult = 1.0

    # Deribit never issues a hard veto — it is a slow volatility context layer only.
    return DeribitContextDecision(
        hit=bool(flags),
        mode=mode,
        flags=flags,
        tighten_add_bps=float(tighten),
        risk_multiplier=float(risk_mult),
        veto=False,
        veto_reason="",
    )
