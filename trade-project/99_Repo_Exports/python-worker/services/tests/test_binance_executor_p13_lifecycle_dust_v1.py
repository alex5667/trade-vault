"""P13 — Lifecycle watchdog dust tolerance fix.

Tests that _monitor_trade_lifecycle_thread does NOT treat a position
with qty exactly equal to step_size as "closed" (which would cancel
all protection and leave a micro-position unprotected).
"""
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

mod_dir = Path(__file__).parent.parent
sys.path.insert(0, str(mod_dir))

mod_path = mod_dir / "binance_executor.py"
spec = importlib.util.spec_from_file_location("binance_executor", mod_path)
mod = importlib.util.module_from_spec(spec)  # type: ignore
sys.modules[spec.name] = mod  # type: ignore
assert spec.loader is not None  # type: ignore
spec.loader.exec_module(mod)  # type: ignore


def _make_exec():
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.position_mode = "oneway"
    ex._exec_event = MagicMock()
    ex._cancel_by_token = MagicMock(return_value=0)
    ex._cancel_all_symbol_orders_best_effort = MagicMock(return_value={
        "plain_seen": 3, "algo_seen": 0, "plain_canceled": 3, "algo_canceled": 0,
    })
    # step_size = 0.1 (e.g. SUI, XRP)
    ex._position_qty_tolerance = MagicMock(return_value=0.1)
    return ex


def test_lifecycle_does_not_cancel_when_qty_equals_step_size():
    """Position qty == step_size is dust but real exposure — do NOT clean up."""
    ex = _make_exec()
    client = MagicMock()

    # Position stays at exactly step_size (0.1) forever, until deadline
    client.get_position_risk.return_value = [
        {"symbol": "SUIUSDT", "positionAmt": "0.1"}
    ]

    # Deadline immediately exceeded to exit the loop fast
    with patch("time.sleep"), patch("time.time", side_effect=[100, 100 + 14401]):  # past deadline
        ex._monitor_trade_lifecycle_thread(
            sid="sid-dust",
            symbol="SUIUSDT",
            logical_side="LONG",
            client=client,
        )

    # Neither cancel method should be called — position is still open
    ex._cancel_all_symbol_orders_best_effort.assert_not_called()
    ex._cancel_by_token.assert_not_called()

    # Timeout event should be emitted
    args, _ = ex._exec_event.call_args
    event = args[0]
    assert event["action"] == "lifecycle_monitor_timeout"


def test_lifecycle_cancels_when_qty_below_step_size():
    """Position qty < step_size (truly zero) → clean up all orders."""
    ex = _make_exec()
    client = MagicMock()

    # Seq: 1.0 → 0.0 (below step_size 0.1)
    client.get_position_risk.side_effect = [
        [{"symbol": "SUIUSDT", "positionAmt": "1.0"}],
        [{"symbol": "SUIUSDT", "positionAmt": "0.0"}],
    ]

    with patch("time.sleep"), patch("time.time", side_effect=[100, 101, 102, 103]):
        ex._monitor_trade_lifecycle_thread(
            sid="sid-close",
            symbol="SUIUSDT",
            logical_side="LONG",
            client=client,
        )

    # Must call cancel_all for fully closed positions
    ex._cancel_all_symbol_orders_best_effort.assert_called_once_with(
        symbol="SUIUSDT", client=client,
    )
    ex._cancel_by_token.assert_not_called()

    args, _ = ex._exec_event.call_args
    event = args[0]
    assert event["action"] == "lifecycle_cleanup"
    assert event["reason"] == "closed"


def test_lifecycle_cancels_when_qty_is_half_step_size():
    """Position qty = 0.05 (half of step_size=0.1) → truly dust → clean up."""
    ex = _make_exec()
    client = MagicMock()

    client.get_position_risk.side_effect = [
        [{"symbol": "XRPUSDT", "positionAmt": "-0.05"}],
    ]

    with patch("time.sleep"), patch("time.time", side_effect=[100, 101, 102]):
        ex._monitor_trade_lifecycle_thread(
            sid="sid-half-dust",
            symbol="XRPUSDT",
            logical_side="SHORT",
            client=client,
        )

    ex._cancel_all_symbol_orders_best_effort.assert_called_once()
    args, _ = ex._exec_event.call_args
    event = args[0]
    assert event["reason"] == "closed"


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
