from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple


@dataclass
class NeedDecision:
    need_rev: int
    need_cont: int
    reason: str


def compute_strong_need_same_tick(
    *,
    scenario: str,
    pressure_hi: bool,
    churn_hi: bool,
    regime: str,
    unstable: bool,
    cfg: Dict[str, Any],
) -> NeedDecision:
    """
    Same-tick escalation policy.
    Default: use cfg strong_need_*.
    Escalate to 3 when:
      - pressure_hi OR churn_hi OR unstable OR thin/news/illiquid regime
    Optionally escalate to 4 in extreme:
      - pressure_hi AND (churn_hi OR unstable) AND thin/news
    """
    base_rev = int(cfg.get("strong_need_reversal", 2))
    base_cont = int(cfg.get("strong_need_continuation", 2))
    base_rev = max(1, base_rev)
    base_cont = max(1, base_cont)

    rg = (regime or "na").lower()
    is_thin = rg in ("thin", "news", "illiquid")

    need = 0
    reason = "BASE"
    if pressure_hi or churn_hi or unstable or is_thin:
        need = max(int(cfg.get("strong_need_escalated", 3)), 3)
        reason = "ESCALATED"
    # extreme escalation (optional)
    if bool(int(cfg.get("strong_need_extreme_enable", 1))) and pressure_hi and is_thin and (churn_hi or unstable):
        need = max(int(cfg.get("strong_need_extreme", 4)), 4)
        reason = "EXTREME"

    if need <= 0:
        return NeedDecision(need_rev=base_rev, need_cont=base_cont, reason=reason)

    # scenario-specific override (can be tuned separately)
    if scenario == "reversal":
        return NeedDecision(need_rev=max(base_rev, need), need_cont=base_cont, reason=reason)
    if scenario == "continuation":
        return NeedDecision(need_rev=base_rev, need_cont=max(base_cont, need), reason=reason)
    return NeedDecision(need_rev=max(base_rev, need), need_cont=max(base_cont, need), reason=reason)
