import logging
import os
import time
from typing import Any

from services.atr_invariant_remediation_executor import InvariantRemediationExecutor
from services.atr_invariant_remediation_registry import get_active_remediation_policies
from services.atr_invariants_registry import get_active_invariants

logger = logging.getLogger("atr_invariant_runtime_engine")

class InvariantRuntimeEngine:
    """
    Validates payload, gate, and execution invariants before orders:queue.
    """
    def __init__(self):
        self.invariants = []
        self.remediation_policies = {}
        self.executor = InvariantRemediationExecutor()
        self.last_sync = 0
        self.sync_interval = 60  # seconds
        self.advisory_only = os.getenv("ATR_INVARIANTS_ADVISORY_ONLY", "1") == "1"
        self.deny_critical = os.getenv("ATR_INVARIANTS_RUNTIME_DENY_CRITICAL", "0") == "1"

    def _sync_invariants(self) -> None:
        now = time.time()
        if now - self.last_sync > self.sync_interval or not self.invariants:
            self.invariants = get_active_invariants()
            self.remediation_policies = get_active_remediation_policies()
            self.last_sync = now
            logger.debug(f"Synced {len(self.invariants)} invariants and {len(self.remediation_policies)} remediation policies.")

    def _evaluate_rule(self, rule_json: dict[str, Any], signal: dict[str, Any]) -> bool:
        """
        Simple hardcoded rule evaluator based on the JSON contract.
        In a real scenario, this could use a rule engine like Rule Engine or simple eval (unsafe).
        For now, we implement hardcoded checks based on reason_code for safety and speed.
        """
        # We rely on the python logic matching the reason_codes defined in INITIAL_INVARIANTS.
        return True # Fallback for unknown ones

    def _fast_hardcoded_checks(self, signal: dict[str, Any]) -> list[dict[str, Any]]:
        """
        To maintain low-latency, we execute hardcoded checks that match the active invariants registry.
        If an invariant is disabled in the registry, we skip it.
        """
        violations = []

        # Build active reason_codes set to only check what's enabled
        active_codes = {inv["reason_code"]: inv for inv in self.invariants}

        side = str(signal.get("side") or signal.get("direction", "")).upper()
        sl_price = float(signal.get("sl_price") or signal.get("sl") or 0.0)
        entry_price = float(signal.get("entry_price") or signal.get("price") or signal.get("entry") or 0.0)
        tp1_price = float(signal.get("tp1_price") or 0.0)
        if not tp1_price:
            tps = signal.get("tp_levels")
            if tps and len(tps) > 0:
                tp1_price = float(tps[0])
        signal_id = str(signal.get("signal_id") or signal.get("sid") or "")

        tradeable = signal.get("tradeable") is True or signal.get("is_rejected_signal") == 0
        veto_reason = signal.get("veto_reason") or signal.get("rejection_reason")

        risk_pct = float(signal.get("risk_pct") or 0.0)
        effective_risk_pct = float(signal.get("effective_risk_pct") or 0.0)

        if "INV_PAYLOAD_BUY_ORDERING" in active_codes and side == "BUY":
            # sl_price < entry_price < tp1_price
            if not (0 < sl_price < entry_price and (tp1_price == 0 or entry_price < tp1_price)):
                violations.append({
                    "invariant_id": active_codes["INV_PAYLOAD_BUY_ORDERING"]["invariant_id"],
                    "reason_code": "INV_PAYLOAD_BUY_ORDERING",
                    "severity": active_codes["INV_PAYLOAD_BUY_ORDERING"]["severity"],
                    "enforcement_mode": active_codes["INV_PAYLOAD_BUY_ORDERING"]["enforcement_mode"],
                    "details": f"BUY ordering violated: sl={sl_price}, entry={entry_price}, tp1={tp1_price}"
                })

        if "INV_PAYLOAD_SELL_ORDERING" in active_codes and side == "SELL":
            # sl_price > entry_price > tp1_price
            if not (sl_price > entry_price and (tp1_price == 0 or entry_price > tp1_price)):
                violations.append({
                    "invariant_id": active_codes["INV_PAYLOAD_SELL_ORDERING"]["invariant_id"],
                    "reason_code": "INV_PAYLOAD_SELL_ORDERING",
                    "severity": active_codes["INV_PAYLOAD_SELL_ORDERING"]["severity"],
                    "enforcement_mode": active_codes["INV_PAYLOAD_SELL_ORDERING"]["enforcement_mode"],
                    "details": f"SELL ordering violated: sl={sl_price}, entry={entry_price}, tp1={tp1_price}"
                })

        if "INV_SIGNAL_ID_REQUIRED" in active_codes:
            if not signal_id:
                violations.append({
                    "invariant_id": active_codes["INV_SIGNAL_ID_REQUIRED"]["invariant_id"],
                    "reason_code": "INV_SIGNAL_ID_REQUIRED",
                    "severity": active_codes["INV_SIGNAL_ID_REQUIRED"]["severity"],
                    "enforcement_mode": active_codes["INV_SIGNAL_ID_REQUIRED"]["enforcement_mode"],
                    "details": "signal_id is missing or empty"
                })

        if "INV_TRADEABLE_REQUIRES_NO_HARD_VETO" in active_codes:
            if tradeable and veto_reason is not None and str(veto_reason).strip() != "":
                violations.append({
                    "invariant_id": active_codes["INV_TRADEABLE_REQUIRES_NO_HARD_VETO"]["invariant_id"],
                    "reason_code": "INV_TRADEABLE_REQUIRES_NO_HARD_VETO",
                    "severity": active_codes["INV_TRADEABLE_REQUIRES_NO_HARD_VETO"]["severity"],
                    "enforcement_mode": active_codes["INV_TRADEABLE_REQUIRES_NO_HARD_VETO"]["enforcement_mode"],
                    "details": f"Marked tradeable but has veto_reason: {veto_reason}"
                })

        if "INV_NO_ORDER_WITHOUT_RISK_PCT" in active_codes:
            # We assume order evaluation is happening if we are in validate_signal
            if risk_pct <= 0 and effective_risk_pct <= 0:
                violations.append({
                    "invariant_id": active_codes["INV_NO_ORDER_WITHOUT_RISK_PCT"]["invariant_id"],
                    "reason_code": "INV_NO_ORDER_WITHOUT_RISK_PCT",
                    "severity": active_codes["INV_NO_ORDER_WITHOUT_RISK_PCT"]["severity"],
                    "enforcement_mode": active_codes["INV_NO_ORDER_WITHOUT_RISK_PCT"]["enforcement_mode"],
                    "details": "risk_pct=0 and effective_risk_pct=0"
                })

        if "INV_NO_ORDER_WITHOUT_SL" in active_codes:
            if sl_price <= 0:
                violations.append({
                    "invariant_id": active_codes["INV_NO_ORDER_WITHOUT_SL"]["invariant_id"],
                    "reason_code": "INV_NO_ORDER_WITHOUT_SL",
                    "severity": active_codes["INV_NO_ORDER_WITHOUT_SL"]["severity"],
                    "enforcement_mode": active_codes["INV_NO_ORDER_WITHOUT_SL"]["enforcement_mode"],
                    "details": "sl_price is zero or missing"
                })

        return violations

    def validate_runtime_state(self, signal: dict[str, Any], ctx: dict[str, Any]) -> tuple[bool, list[dict[str, Any]]]:
        violations = []
        active_codes = {inv["reason_code"]: inv for inv in self.invariants}

        is_new_entry = (signal.get("action") or "OPEN") == "OPEN"
        # If it's OPEN, make sure side implies it's a new position

        degrade_state = (ctx.get("degrade_state") or "normal")
        allocator_state = (ctx.get("allocator_state") or "fresh")
        rollout_stage = (ctx.get("rollout_stage") or "shadow")
        portfolio_gate_allow = bool(ctx.get("portfolio_gate_allow", True))
        protective_exit_allowed = bool(ctx.get("protective_exit_allowed", True))

        if "INV_NO_NEW_RISK_UNDER_DEGRADE" in active_codes:
            if is_new_entry and degrade_state in {"reduce_only", "no_new_risk", "hard_freeze"}:
                violations.append({
                    "invariant_id": active_codes["INV_NO_NEW_RISK_UNDER_DEGRADE"]["invariant_id"],
                    "reason_code": "INV_NO_NEW_RISK_UNDER_DEGRADE",
                    "severity": active_codes["INV_NO_NEW_RISK_UNDER_DEGRADE"]["severity"],
                    "enforcement_mode": active_codes["INV_NO_NEW_RISK_UNDER_DEGRADE"]["enforcement_mode"],
                    "details": f"Cannot take new risk under degrade_state={degrade_state}"
                })

        if "INV_NO_ALLOCATOR_ON_STALE_STATE_FOR_LIVE_SCOPE" in active_codes:
            if rollout_stage == "live_100" and allocator_state != "fresh" and is_new_entry:
                violations.append({
                    "invariant_id": active_codes["INV_NO_ALLOCATOR_ON_STALE_STATE_FOR_LIVE_SCOPE"]["invariant_id"],
                    "reason_code": "INV_NO_ALLOCATOR_ON_STALE_STATE_FOR_LIVE_SCOPE",
                    "severity": active_codes["INV_NO_ALLOCATOR_ON_STALE_STATE_FOR_LIVE_SCOPE"]["severity"],
                    "enforcement_mode": active_codes["INV_NO_ALLOCATOR_ON_STALE_STATE_FOR_LIVE_SCOPE"]["enforcement_mode"],
                    "details": "Live scope cannot trade on stale allocator state"
                })

        if "INV_NO_PORTFOLIO_CAP_BYPASS" in active_codes:
            if is_new_entry and not portfolio_gate_allow:
                violations.append({
                    "invariant_id": active_codes["INV_NO_PORTFOLIO_CAP_BYPASS"]["invariant_id"],
                    "reason_code": "INV_NO_PORTFOLIO_CAP_BYPASS",
                    "severity": active_codes["INV_NO_PORTFOLIO_CAP_BYPASS"]["severity"],
                    "enforcement_mode": active_codes["INV_NO_PORTFOLIO_CAP_BYPASS"]["enforcement_mode"],
                    "details": "Order violates portfolio gate concentration/cluster caps"
                })

        if "INV_PROTECTIVE_EXITS_ALWAYS_ALLOWED_UNDER_DEGRADE" in active_codes:
            if (not is_new_entry) and not protective_exit_allowed:
                violations.append({
                    "invariant_id": active_codes["INV_PROTECTIVE_EXITS_ALWAYS_ALLOWED_UNDER_DEGRADE"]["invariant_id"],
                    "reason_code": "INV_PROTECTIVE_EXITS_ALWAYS_ALLOWED_UNDER_DEGRADE",
                    "severity": active_codes["INV_PROTECTIVE_EXITS_ALWAYS_ALLOWED_UNDER_DEGRADE"]["severity"],
                    "enforcement_mode": active_codes["INV_PROTECTIVE_EXITS_ALWAYS_ALLOWED_UNDER_DEGRADE"]["enforcement_mode"],
                    "details": "Protective exit explicitly blocked"
                })

        return (len(violations) == 0), violations
    def validate_signal(self, signal: dict[str, Any]) -> tuple[bool, list[dict[str, Any]]]:
        """
        Returns:
            allow: bool (whether to proceed to orders:queue)
            violations: list of dict details
        """
        self._sync_invariants()

        violations = self._fast_hardcoded_checks(signal)

        # Cross-layer logic
        meta = signal.get("meta", {}) if isinstance(signal.get("meta"), dict) else {}
        ctx = meta if meta else signal # Fallback if context is in top-level
        _, runtime_violations = self.validate_runtime_state(signal, ctx)
        violations.extend(runtime_violations)

        # Decide if deny
        allow = True
        has_critical_deny = any(v["enforcement_mode"] == "runtime_deny" and v["severity"] == "critical" for v in violations)

        # Remediation Execution
        remediation_actions = []
        for v in violations:
            inv_id = v.get("invariant_id", "UNKNOWN")
            policy = self.remediation_policies.get(inv_id)
            if policy:
                v_context = dict(v)
                # Ensure scope data for remediation string substitution
                v_context["scope_kind"] = "symbol"
                v_context["scope_value"] = (signal.get("symbol", "unknown"))

                action = self.executor.execute(v_context, policy)

                if action["status"] == "executed" and action["reason_code"] == "REMEDIATION_RUNTIME_CLIP":
                    clip_mult = action["action_json"].get("clip_mult", 1.0)
                    orig_eff = float(signal.get("effective_risk_pct") or 0.0)
                    signal["effective_risk_pct"] = orig_eff * clip_mult
                    signal["runtime_clip_applied"] = clip_mult
                    logger.warning(f"Runtime_clip applied: mult={clip_mult} to {v_context['scope_value']} due to {inv_id}")

                remediation_actions.append(action)

        if remediation_actions:
            if isinstance(signal.get("meta"), dict):
                signal["meta"]["remediation_actions"] = remediation_actions
            else:
                signal["remediation_actions"] = remediation_actions

        if violations:
            logger.warning(f"Invariant violations detected for {signal.get('symbol')}: {violations}")

            if has_critical_deny and self.deny_critical and not self.advisory_only:
                allow = False
                logger.error(f"Signal {signal.get('signal_id')} denied by InvariantRuntimeEngine.")

        return allow, violations

# Singleton instance
_engine = None
def get_runtime_engine() -> InvariantRuntimeEngine:
    global _engine
    if _engine is None:
        _engine = InvariantRuntimeEngine()
    return _engine
