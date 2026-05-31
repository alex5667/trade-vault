from __future__ import annotations

from dataclasses import dataclass


def regime_group(regime: str) -> str:
    """
    Maps fine-grained regime to broad AB-test group.
    """
    rg = (regime or "na").strip().lower()
    return "thin" if rg in ("thin", "news", "illiquid") else "default"


def norm_arm(x: str | None) -> str:
    """
    Normalizes arm to A/B/C. Default is A.
    """
    a = (x or "").strip().upper()
    return a if a in ("A", "B", "C") else "A"


@dataclass
class ActiveArmDecision:
    apply: bool
    active_arm: str
    is_active: bool
    reason: str


def decide_active_arm(*, cand_arm: str, active_arm_value: str | None) -> ActiveArmDecision:
    """
    If active_arm_value is missing/unparseable -> fail-open (do not apply gate).
    Else enforce that only active arm can emit real entry.
    """
    ca = norm_arm(cand_arm)
    av = (active_arm_value or "").strip().upper()
    if av not in ("A", "B", "C"):
        # Fail-open: if config is missing or invalid, we assume GATE IS OPEN (or not applied)
        # But wait, if logic says "ACTIVE ARM ONLY", then missing config usually means "Default A"?
        # The user code says: return ActiveArmDecision(apply=False, ... reason="NO_ACTIVE_ARM_KEY")
        # apply=False implies "Don't gate, let it pass" or "Don't enforce"?
        # In smt_entry_policy_service usage:
        # active_arm = act.active_arm if act.apply else "NA"
        # shadow_due_to_inactive = bool(act.apply and (not act.is_active))
        # If apply=False, shadow_due_to_inactive is False. So it passes (unless other shadow logic exists).
        return ActiveArmDecision(apply=False, active_arm="NA", is_active=True, reason="NO_ACTIVE_ARM_KEY")

    aa = av
    return ActiveArmDecision(apply=True, active_arm=aa, is_active=(ca == aa), reason="OK" if (ca == aa) else "INACTIVE_ARM")
