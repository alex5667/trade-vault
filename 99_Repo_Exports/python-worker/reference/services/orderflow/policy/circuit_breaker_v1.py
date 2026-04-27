"""
P68 — Circuit Breaker Policy (v1)

Goal:
- Derive effective regime (ok|warn|block) from dq/drift states + quality KPIs.
- Apply safe overrides to the decision pipeline:
  - block -> rule-strong-only (no soft), ML enforce disabled (best-effort)
  - warn  -> keep ML, but allow stricter binding defaults (best-effort)
- Record policy fields into indicators (so DecisionRecord includes them).

This module MUST be:
- deterministic
- low-latency
- fail-open (never raise in hot path)
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Tuple


def _norm_state(v: Any) -> str:
    """
    Normalize state inputs to one of: ok|warn|block|unknown
    Accepts: str, int, bool
    """
    if v is None:
        return "unknown"
    if isinstance(v, bool):
        return "block" if v else "ok"
    if isinstance(v, (int, float)):
        # common encodings: 0=ok,1=warn,2=block
        if int(v) <= 0:
            return "ok"
        if int(v) == 1:
            return "warn"
        return "block"
    s = str(v).strip().lower()
    if s in ("ok", "good", "pass", "0"):
        return "ok"
    if s in ("warn", "warning", "soft", "1"):
        return "warn"
    if s in ("block", "bad", "fail", "deny", "2"):
        return "block"
    return "unknown"


def _f(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        v = cfg.get(key, default)
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _i(cfg: Dict[str, Any], key: str, default: int) -> int:
    try:
        v = cfg.get(key, default)
        if v is None:
            return int(default)
        return int(float(v))
    except Exception:
        return int(default)


def _b(cfg: Dict[str, Any], key: str, default: bool) -> bool:
    try:
        v = cfg.get(key, default)
        if v is None:
            return bool(default)
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return True
        if s in ("0", "false", "no", "n", "off"):
            return False
        return bool(default)
    except Exception:
        return bool(default)


@dataclass
class CircuitBreakerDecision:
    ver: str
    regime: str  # ok|warn|block|unknown
    reason: str
    # recommended overrides (applied by caller)
    force_rule_strong_only: bool
    disable_ml_enforce: bool
    # diagnostics
    dq_state: str
    drift_state: str
    ece_24h: float
    expectancy_r_24h: float
    precision_top5p_24h: float

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def decide_circuit_breaker(
    *,
    cfg: Dict[str, Any],
    dq_state: Any,
    drift_state: Any,
) -> CircuitBreakerDecision:
    """
    Decide effective regime and overrides.
    Inputs:
      - cfg: merged static+dynamic cfg (P67)
      - dq_state, drift_state: from indicators / dynamic cfg / engine evidence
    """
    # Feature flag
    if not _b(cfg, "cb_enable", True):
        return CircuitBreakerDecision(
            ver="v1",
            regime="ok",
            reason="disabled",
            force_rule_strong_only=False,
            disable_ml_enforce=False,
            dq_state=_norm_state(dq_state),
            drift_state=_norm_state(drift_state),
            ece_24h=_f(cfg, "signal_quality_ece_24h", 0.0),
            expectancy_r_24h=_f(cfg, "signal_quality_expectancy_r_24h", 0.0),
            precision_top5p_24h=_f(cfg, "signal_quality_precision_top5p_24h", 0.0),
        )

    dq = _norm_state(dq_state)
    dr = _norm_state(drift_state)

    # Base regime from dq/drift
    base = "ok"
    if dq == "block" or dr == "block":
        base = "block"
    elif dq == "warn" or dr == "warn":
        base = "warn"
    elif dq == "unknown" or dr == "unknown":
        base = "warn" if _b(cfg, "cb_unknown_as_warn", True) else "ok"

    # Quality escalation (uses P47/P64 KPIs in dynamic cfg)
    ece = _f(cfg, "signal_quality_ece_24h", 0.0)
    exp_r = _f(cfg, "signal_quality_expectancy_r_24h", 0.0)
    prec = _f(cfg, "signal_quality_precision_top5p_24h", 0.0)
    n = _i(cfg, "signal_quality_n_24h", 0)

    min_n = _i(cfg, "cb_quality_min_n_24h", 60)
    ece_warn = _f(cfg, "cb_ece_warn", 0.10)
    ece_block = _f(cfg, "cb_ece_block", 0.18)
    exp_warn = _f(cfg, "cb_expectancy_warn", -0.05)
    exp_block = _f(cfg, "cb_expectancy_block", -0.20)
    prec_warn = _f(cfg, "cb_precision_top5p_warn", 0.40)
    prec_block = _f(cfg, "cb_precision_top5p_block", 0.30)

    regime = base
    reason_parts = [f"base:{base}", f"dq:{dq}", f"drift:{dr}"]

    if n >= min_n:
        # Escalate based on quality
        if (ece >= ece_block) or (exp_r <= exp_block) or (prec > 0 and prec <= prec_block):
            if regime != "block":
                regime = "block"
            reason_parts.append("quality:block")
        elif (ece >= ece_warn) or (exp_r <= exp_warn) or (prec > 0 and prec <= prec_warn):
            if regime == "ok":
                regime = "warn"
            reason_parts.append("quality:warn")
    else:
        reason_parts.append(f"quality:skip(n<{min_n})")

    # Overrides
    force_strong = False
    disable_ml = False
    if regime == "block":
        force_strong = _b(cfg, "cb_block_force_rule_strong_only", True)
        disable_ml = _b(cfg, "cb_block_disable_ml_enforce", True)
    elif regime == "warn":
        force_strong = _b(cfg, "cb_warn_force_rule_strong_only", False)
        disable_ml = _b(cfg, "cb_warn_disable_ml_enforce", False)

    return CircuitBreakerDecision(
        ver="v1",
        regime=regime,
        reason="|".join(reason_parts),
        force_rule_strong_only=force_strong,
        disable_ml_enforce=disable_ml,
        dq_state=dq,
        drift_state=dr,
        ece_24h=ece,
        expectancy_r_24h=exp_r,
        precision_top5p_24h=prec,
    )


def apply_circuit_breaker_overrides(
    *,
    cfg: Dict[str, Any],
    decision: CircuitBreakerDecision,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Return (cfg_overrides, policy_fields_for_indicators).
    The caller should merge cfg_overrides into effective cfg.
    """
    overrides: Dict[str, Any] = {}

    # Strong-only mode is implemented via explicit flags consumed in TickProcessor and (optionally) engine.
    if decision.force_rule_strong_only:
        overrides["require_strong_confirmation"] = 1
        overrides["strong_gate_shadow"] = 0
        overrides["cb_effective_rule_strong_only"] = 1
    else:
        overrides["cb_effective_rule_strong_only"] = 0

    # Disable ML enforce (best-effort): some components use these keys.
    if decision.disable_ml_enforce:
        overrides["ml_confirm_rollout"] = "off"
        overrides["ml_enforce_disable"] = 1
    else:
        overrides["ml_enforce_disable"] = 0

    # Policy fields go to indicators and then DecisionRecord (P45/P62)
    policy_fields = {
        "policy_ver": decision.ver,
        "policy_regime": decision.regime,
        "policy_reason": decision.reason,
        "policy_force_rule_strong_only": int(decision.force_rule_strong_only),
        "policy_disable_ml_enforce": int(decision.disable_ml_enforce),
        "policy_dq_state": decision.dq_state,
        "policy_drift_state": decision.drift_state,
        "policy_ece_24h": decision.ece_24h,
        "policy_expectancy_r_24h": decision.expectancy_r_24h,
        "policy_precision_top5p_24h": decision.precision_top5p_24h,
    }

    return overrides, policy_fields


def enforce_circuit_breaker_regime(
    decision: CircuitBreakerDecision, 
    effective_regime: str, 
    cfg: Dict[str, Any]
) -> CircuitBreakerDecision:
    """
    Return a new decision with the effective regime applied (updating flags).
    """
    if decision.regime == effective_regime:
        return decision
        
    # Re-calc overrides
    force_strong = False
    disable_ml = False
    if effective_regime == "block":
        force_strong = _b(cfg, "cb_block_force_rule_strong_only", True)
        disable_ml = _b(cfg, "cb_block_disable_ml_enforce", True)
    elif effective_regime == "warn":
        force_strong = _b(cfg, "cb_warn_force_rule_strong_only", False)
        disable_ml = _b(cfg, "cb_warn_disable_ml_enforce", False)
        
    return CircuitBreakerDecision(
        ver=decision.ver,
        regime=effective_regime,
        reason=decision.reason + f"|hysteresis:{effective_regime}",
        force_rule_strong_only=force_strong,
        disable_ml_enforce=disable_ml,
        dq_state=decision.dq_state,
        drift_state=decision.drift_state,
        ece_24h=decision.ece_24h,
        expectancy_r_24h=decision.expectancy_r_24h,
        precision_top5p_24h=decision.precision_top5p_24h,
    )
