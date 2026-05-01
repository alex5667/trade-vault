from __future__ import annotations
"""P6 tests for ExecutionIntentValidator.

Tests cover:
  - Invalid workingType values (reject non-MARK_PRICE / CONTRACT_PRICE)
  - close_position=True combined with reduce_only=True (should pass: Binance allows either)
  - compute_trailing_activate_price edge cases
"""

import math
import pytest
import sys
import os

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

try:
    from services.execution_intent_validator import validate_exit_intent
    from services.binance_futures_client import AlgoOrderRef
    from services.binance_executor import compute_trailing_activate_price
except Exception:
    from execution_intent_validator import validate_exit_intent
    from binance_futures_client import AlgoOrderRef
    from binance_executor import compute_trailing_activate_price


def _make_ref(order_type: str, working_type: str = "MARK_PRICE") -> AlgoOrderRef:
    return AlgoOrderRef(
        algo_id=1,
        client_algo_id="test",
        type=order_type,
        working_type=working_type,
    )


def test_invalid_working_type_rejected():
    """Non-canonical workingType values should be rejected by validate_exit_intent."""
    result = validate_exit_intent(
        position_mode="oneway",
        position_side=None,
        exit_intent="reduce",
        reduce_only=True,
        close_position=False,
        quantity=0.01,
        order_type="STOP_MARKET",
        working_type="LAST_PRICE",
        is_algo=True,
    )
    assert result.is_valid_exit_contract is False
    assert "invalid_workingType" in result.reason


def test_close_position_true_with_reduce_only():
    """Binance accepts both closePosition and reduceOnly on the same SL order.

    For one-way mode SL, closePosition=True is preferred. This test just asserts
    the ref construction does not raise.
    """
    ref = AlgoOrderRef(
        algo_id=99,
        client_algo_id="sl-test",
        type="STOP_MARKET",
        working_type="MARK_PRICE",
        close_position=True,
        reduce_only=True,
    )
    assert ref.close_position is True
    assert ref.reduce_only is True


class TestComputeTrailingActivatePrice:
    def test_long_activation_above_latest(self):
        px = compute_trailing_activate_price(
            "LONG", latest_price=50000.0, tick_size=0.1, buffer_bps=5.0
        )
        assert px > 50000.0

    def test_short_activation_below_latest(self):
        px = compute_trailing_activate_price(
            "SHORT", latest_price=50000.0, tick_size=0.1, buffer_bps=5.0
        )
        assert px < 50000.0

    def test_zero_or_negative_latest_raises(self):
        with pytest.raises(ValueError):
            compute_trailing_activate_price("LONG", latest_price=0.0, tick_size=0.1, buffer_bps=5.0)

    def test_user_activate_price_long_valid(self):
        px = compute_trailing_activate_price(
            "LONG", latest_price=40000.0, tick_size=0.01, buffer_bps=1.0,
            user_activate_price=41000.0,
        )
        assert px > 40000.0

    def test_user_activate_price_short_valid(self):
        px = compute_trailing_activate_price(
            "SHORT", latest_price=40000.0, tick_size=0.01, buffer_bps=1.0,
            user_activate_price=39000.0,
        )
        assert px < 40000.0
