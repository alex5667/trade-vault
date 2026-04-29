from dataclasses import dataclass
from typing import List

@dataclass
class SentimentContextDecision:
    hit: bool
    mode: str
    flags: List[str]
    risk_multiplier: float
    tighten_add_bps: float
    veto: bool
    veto_reason: str

def evaluate_sentiment_context(
    *,
    profile: str,
    side: str,
    sentiment_regime: str,
    fear_greed_value: int,
    fear_greed_delta_1d: int,
    fear_greed_delta_7d: int,
    base_risk_multiplier: float,
    tighten_cap_bps: float,
) -> SentimentContextDecision:
    mode = {
        "default": "monitor",
        "monitor": "monitor",
        "soft": "monitor",
        "strict": "tighten",
        "tighten": "tighten",
        "hard": "tighten",  # intentionally no veto
    }.get(str(profile or "monitor").lower(), "monitor")

    flags: List[str] = []

    regime = str(sentiment_regime or "unknown").lower()

    if regime == "extreme_fear":
        flags.append("sentiment_extreme_fear")
    elif regime == "fear":
        flags.append("sentiment_fear")
    elif regime == "greed":
        flags.append("sentiment_greed")
    elif regime == "extreme_greed":
        flags.append("sentiment_extreme_greed")

    if fear_greed_delta_1d >= 10:
        flags.append("sentiment_fast_greed_shift")
    elif fear_greed_delta_1d <= -10:
        flags.append("sentiment_fast_fear_shift")

    # Conservative risk multiplier.
    rm = max(0.25, min(1.0, float(base_risk_multiplier or 1.0)))

    tighten = 0.0
    if mode == "tighten":
        if regime in {"extreme_fear", "extreme_greed"}:
            tighten = min(float(tighten_cap_bps), 2.0)
        elif regime in {"fear", "greed"}:
            tighten = min(float(tighten_cap_bps), 1.0)

    # Never hard-veto from daily sentiment.
    return SentimentContextDecision(
        hit=bool(flags),
        mode=mode,
        flags=flags,
        risk_multiplier=float(rm),
        tighten_add_bps=float(tighten),
        veto=False,
        veto_reason="",
    )
