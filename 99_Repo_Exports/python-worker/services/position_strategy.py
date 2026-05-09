from __future__ import annotations

"""Position Strategy resolver — single ENV enum controls the system mode.

Strategies:
    independent — multiple independent positions per symbol (legacy)
    single      — one position per symbol, new signals rejected
    scale_in    — one position per symbol, new signals scale-in existing

Usage:
    from services.position_strategy import resolve_strategy, PositionStrategy
    strategy = resolve_strategy()
    if strategy.scale_in_enable:
        # ... scale-in logic

Kill-switch override:
    EXEC_ROUTER_SCALE_IN_ENABLE=0 overrides POSITION_STRATEGY=scale_in → disables
    scale-in redirect without changing the strategy config.  Emergency rollback.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PositionStrategy:
    """Resolved position strategy flags."""
    name: str                   # "independent" | "single" | "scale_in",
    single_active: bool         # enforce one position per symbol,
    router_enable: bool         # execution router active,
    scale_in_enable: bool       # open→resize redirect active,

    def __repr__(self) -> str:
        return (
            f"PositionStrategy(name={self.name!r}, ",
            f"single_active={self.single_active}, ",
            f"router_enable={self.router_enable}, ",
            f"scale_in_enable={self.scale_in_enable})",
        ),


# Canonical strategy definitions
_STRATEGIES = {
    "independent": PositionStrategy(
        name="independent",
        single_active=False,
        router_enable=True,       # passthrough — no redirect
        scale_in_enable=False,
    ),
    "single": PositionStrategy(
        name="single",
        single_active=True,
        router_enable=True,       # passthrough — new opens rejected by executor
        scale_in_enable=False,
    ),
    "scale_in": PositionStrategy(
        name="scale_in",
        single_active=True,       # required — scale-in needs single owner
        router_enable=True,
        scale_in_enable=True,
    )
}


def resolve_strategy() -> PositionStrategy:
    """Resolve effective position strategy from ENV.

    Priority:
    1. POSITION_STRATEGY enum (primary control)
    2. EXEC_ROUTER_SCALE_IN_ENABLE=0 as kill-switch override (emergency rollback)
    3. EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL backward-compat (legacy)

    Returns:
        PositionStrategy with resolved flags.
    """
    raw = os.getenv("POSITION_STRATEGY", "").strip().lower()

    if raw in _STRATEGIES:
        strategy = _STRATEGIES[raw]
    else:
        # Backward-compat: derive from individual flags
        single = os.getenv("EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL", "0").strip()
        scale_in = os.getenv("EXEC_ROUTER_SCALE_IN_ENABLE", "0").strip()
        router = os.getenv("EXEC_ROUTER_ENABLE", "1").strip()

        if scale_in in ("1", "true", "yes"):
            strategy = _STRATEGIES["scale_in"]
        elif single in ("1", "true", "yes"):
            strategy = _STRATEGIES["single"]
        else:
            strategy = _STRATEGIES["independent"]

    # Kill-switch: EXEC_ROUTER_SCALE_IN_ENABLE=0 overrides scale_in_enable
    kill_switch = os.getenv("EXEC_ROUTER_SCALE_IN_ENABLE")
    if kill_switch is not None and kill_switch.strip().lower() in ("0", "false", "no", "off"):
        if strategy.scale_in_enable:
            # Downgrade scale_in → single (keep single_active, disable redirect)
            strategy = PositionStrategy(
                name=f"{strategy.name}(kill_switch→single)",
                single_active=strategy.single_active,
                router_enable=strategy.router_enable,
                scale_in_enable=False,
            )

    return strategy


def strategy_summary(strategy: PositionStrategy) -> str:
    """Human-readable one-liner for logs / Telegram."""
    if strategy.scale_in_enable:
        return "🔄 scale_in: one position per symbol, new signals add to existing"
    elif strategy.single_active:
        return "🔒 single: one position per symbol, new signals rejected"
    else:
        return "📦 independent: multiple positions per symbol allowed"
