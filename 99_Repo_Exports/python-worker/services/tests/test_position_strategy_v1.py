from __future__ import annotations
"""Unit tests for position_strategy — POSITION_STRATEGY enum resolver.

Tests:
1. POSITION_STRATEGY=single → single_active=True, scale_in=False
2. POSITION_STRATEGY=scale_in → single_active=True, scale_in=True
3. POSITION_STRATEGY=independent → single_active=False, scale_in=False
4. Kill-switch: POSITION_STRATEGY=scale_in + EXEC_ROUTER_SCALE_IN_ENABLE=0 → scale_in=False
5. Backward compat: no POSITION_STRATEGY, individual flags used
6. strategy_summary returns correct emoji/text
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest


# Helper to set env cleanly
def _with_env(env_dict, fn):
    """Run fn with a clean env subset."""
    # Save originals
    saved = {}
    keys_to_clear = [
        "POSITION_STRATEGY",
        "EXEC_ROUTER_ENABLE", "EXEC_ROUTER_SCALE_IN_ENABLE",
        "EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL",
    ]
    for k in keys_to_clear:
        saved[k] = os.environ.pop(k, None)
    # Set requested
    for k, v in env_dict.items():
        os.environ[k] = str(v)
    try:
        return fn()
    finally:
        # Restore
        for k in keys_to_clear:
            os.environ.pop(k, None)
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


# ===========================================================================
# Tests
# ===========================================================================

def test_strategy_single():
    from services.position_strategy import resolve_strategy
    s = _with_env({"POSITION_STRATEGY": "single"}, resolve_strategy)
    assert s.name == "single"
    assert s.single_active is True
    assert s.router_enable is True
    assert s.scale_in_enable is False


def test_strategy_scale_in():
    from services.position_strategy import resolve_strategy
    s = _with_env({"POSITION_STRATEGY": "scale_in", "EXEC_ROUTER_SCALE_IN_ENABLE": "1"}, resolve_strategy)
    assert s.name == "scale_in"
    assert s.single_active is True
    assert s.scale_in_enable is True


def test_strategy_independent():
    from services.position_strategy import resolve_strategy
    s = _with_env({"POSITION_STRATEGY": "independent"}, resolve_strategy)
    assert s.name == "independent"
    assert s.single_active is False
    assert s.scale_in_enable is False


def test_kill_switch_overrides_scale_in():
    """POSITION_STRATEGY=scale_in + EXEC_ROUTER_SCALE_IN_ENABLE=0 → scale_in disabled."""
    from services.position_strategy import resolve_strategy
    s = _with_env({
        "POSITION_STRATEGY": "scale_in",
        "EXEC_ROUTER_SCALE_IN_ENABLE": "0",
    }, resolve_strategy)
    assert s.scale_in_enable is False
    assert s.single_active is True  # kept from scale_in
    assert "kill_switch" in s.name


def test_backward_compat_no_strategy_env():
    """No POSITION_STRATEGY set → derive from individual flags."""
    from services.position_strategy import resolve_strategy
    # Legacy: EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL=1 → single
    s = _with_env({
        "EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL": "1",
    }, resolve_strategy)
    assert s.name == "single"
    assert s.single_active is True
    assert s.scale_in_enable is False


def test_backward_compat_independent_default():
    """No env vars at all → independent (legacy default)."""
    from services.position_strategy import resolve_strategy
    s = _with_env({}, resolve_strategy)
    assert s.name == "independent"
    assert s.single_active is False


def test_backward_compat_scale_in_flag():
    """No POSITION_STRATEGY but EXEC_ROUTER_SCALE_IN_ENABLE=1 → scale_in."""
    from services.position_strategy import resolve_strategy
    s = _with_env({
        "EXEC_ROUTER_SCALE_IN_ENABLE": "1",
    }, resolve_strategy)
    assert s.name == "scale_in"
    assert s.scale_in_enable is True


def test_strategy_summary():
    from services.position_strategy import strategy_summary, PositionStrategy
    s1 = PositionStrategy(name="scale_in", single_active=True, router_enable=True, scale_in_enable=True)
    assert "scale_in" in strategy_summary(s1)
    s2 = PositionStrategy(name="single", single_active=True, router_enable=True, scale_in_enable=False)
    assert "single" in strategy_summary(s2)
    s3 = PositionStrategy(name="independent", single_active=False, router_enable=True, scale_in_enable=False)
    assert "independent" in strategy_summary(s3)
