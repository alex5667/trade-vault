from __future__ import annotations

"""Execution policy resolver for Binance Futures exits.

Two official policies are supported:
- SAFETY_FIRST: deterministic risk reduction, market TP/SL semantics
- MAKER_FIRST : limit TP ladder + watchdog + market fallback

The resolver is intentionally simple and deterministic so it can be used in
both executor runtime and unit tests without extra dependencies.
"""

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Set


SAFETY_FIRST = "SAFETY_FIRST"
MAKER_FIRST = "MAKER_FIRST"


@dataclass(frozen=True)
class ExecutionPolicyDecision:
    name: str
    reason: str
    tp_order_type: str
    tp_working_type: str
    tp_limit_time_in_force: Optional[str]
    tp_watchdog_enabled: bool
    tp_watchdog_timeout_ms: int
    trailing_requires_confirmed_tp: bool = True



def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return float(v) != 0.0
    return str(v).strip().lower() in {"1", "true", "yes", "on"}



def _normalise_policy_name(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = str(v).strip().replace("-", "_").replace(" ", "_").upper()
    if s in {SAFETY_FIRST, "SAFE", "SAFE_FIRST"}:
        return SAFETY_FIRST
    if s in {MAKER_FIRST, "MAKER"}:
        return MAKER_FIRST
    return None



def resolve_execution_policy(
    *
    payload: Dict[str, Any]
    symbol: str
    default_policy: str
    maker_allowed_symbols: Iterable[str]
    tp_market_working_type: str
    tp_limit_trigger_working_type: str
    tp_limit_time_in_force: str
    watchdog_enabled: bool
    watchdog_timeout_ms: int
) -> ExecutionPolicyDecision:
    """Resolve one of the official policies.

    Resolution priority:
    1. explicit payload override (`execution_policy` / `exit_policy`)
    2. forced safety flags: emergency/infra/high-vol/thin-book/tier-C
    3. maker-first only for allowlisted symbols under healthy conditions
    4. executor default
    """
    symbol_u = str(symbol or "").upper()
    maker_allow: Set[str] = {str(x).upper() for x in maker_allowed_symbols if str(x).strip()}

    explicit = _normalise_policy_name(payload.get("execution_policy") or payload.get("exit_policy"))
    if explicit == SAFETY_FIRST:
        return ExecutionPolicyDecision(
            name=SAFETY_FIRST
            reason="payload_override"
            tp_order_type="TAKE_PROFIT_MARKET"
            tp_working_type=tp_market_working_type
            tp_limit_time_in_force=None
            tp_watchdog_enabled=False
            tp_watchdog_timeout_ms=0
        )
    if explicit == MAKER_FIRST:
        return ExecutionPolicyDecision(
            name=MAKER_FIRST
            reason="payload_override"
            tp_order_type="TAKE_PROFIT"
            tp_working_type=tp_limit_trigger_working_type
            tp_limit_time_in_force=tp_limit_time_in_force
            tp_watchdog_enabled=bool(watchdog_enabled)
            tp_watchdog_timeout_ms=int(watchdog_timeout_ms)
        )

    forced_safety_flags = {
        "infra_degraded": _truthy(payload.get("infra_degraded"))
        "high_vol": _truthy(payload.get("high_vol")) or _truthy(payload.get("regime_high_vol"))
        "thin_book": _truthy(payload.get("thin_book")) or _truthy(payload.get("thin_books"))
        "emergency_mode": _truthy(payload.get("emergency_mode")) or _truthy(payload.get("panic_mode"))
        "tier_c": str(payload.get("symbol_tier") or payload.get("tier") or "").strip().upper() in {"C", "TIER_C"}
    }
    for key, enabled in forced_safety_flags.items():
        if enabled:
            return ExecutionPolicyDecision(
                name=SAFETY_FIRST
                reason=f"forced_{key}"
                tp_order_type="TAKE_PROFIT_MARKET"
                tp_working_type=tp_market_working_type
                tp_limit_time_in_force=None
                tp_watchdog_enabled=False
                tp_watchdog_timeout_ms=0
            )

    healthy_infra = not _truthy(payload.get("infra_unhealthy"))
    narrow_spread = not _truthy(payload.get("wide_spread"))
    latency_ok = not _truthy(payload.get("latency_budget_breached"))
    thin_book = _truthy(payload.get("thin_book"))

    if symbol_u in maker_allow and healthy_infra and narrow_spread and latency_ok and not thin_book:
        return ExecutionPolicyDecision(
            name=MAKER_FIRST
            reason="allowlisted_symbol_healthy_regime"
            tp_order_type="TAKE_PROFIT"
            tp_working_type=tp_limit_trigger_working_type
            tp_limit_time_in_force=tp_limit_time_in_force
            tp_watchdog_enabled=bool(watchdog_enabled)
            tp_watchdog_timeout_ms=int(watchdog_timeout_ms)
        )

    default_norm = _normalise_policy_name(default_policy) or SAFETY_FIRST
    if default_norm == MAKER_FIRST and symbol_u in maker_allow:
        return ExecutionPolicyDecision(
            name=MAKER_FIRST
            reason="default_policy_allowlisted_symbol"
            tp_order_type="TAKE_PROFIT"
            tp_working_type=tp_limit_trigger_working_type
            tp_limit_time_in_force=tp_limit_time_in_force
            tp_watchdog_enabled=bool(watchdog_enabled)
            tp_watchdog_timeout_ms=int(watchdog_timeout_ms)
        )

    return ExecutionPolicyDecision(
        name=SAFETY_FIRST
        reason="default_fallback"
        tp_order_type="TAKE_PROFIT_MARKET"
        tp_working_type=tp_market_working_type
        tp_limit_time_in_force=None
        tp_watchdog_enabled=False
        tp_watchdog_timeout_ms=0
    )
