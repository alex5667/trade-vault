import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# Mock imports for binance_executor
mod_dir = Path(__file__).parent.parent
sys.path.insert(0, str(mod_dir))

mod_path = mod_dir / "binance_executor.py"
spec = importlib.util.spec_from_file_location("binance_executor", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

pol_path = mod_dir / "execution_policy.py"
pspec = importlib.util.spec_from_file_location("execution_policy", pol_path)
pol = importlib.util.module_from_spec(pspec)
sys.modules[pspec.name] = pol
assert pspec.loader is not None
pspec.loader.exec_module(pol)

def _make_exec():
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.position_mode = "oneway"
    ex._exec_event = MagicMock()
    ex._cancel_by_token = MagicMock()
    ex._position_qty_tolerance = MagicMock(return_value=0.0001)
    return ex

def test_monitor_trade_lifecycle_cleanup_on_zero_position():
    ex = _make_exec()
    client = MagicMock()

    # Seq: 1.0 -> 0.0
    client.get_position_risk.side_effect = [
        [{"symbol": "BTCUSDT", "positionAmt": "1.0"}],
        [{"symbol": "BTCUSDT", "positionAmt": "0.0"}]
    ]

    # We want to exit after the second iteration
    # time.sleep will be called once between iterations
    with patch("time.sleep") as mock_sleep:
        # Mock time.time to avoid infinite loop if logic fails
        with patch("time.time", side_effect=[100, 101, 102, 103]):
            ex._monitor_trade_lifecycle_thread(
                sid="sid-test",
                symbol="BTCUSDT",
                logical_side="LONG",
                client=client
            )

    # Verify _cancel_by_token was called
    ex._cancel_by_token.assert_called_once_with("BTCUSDT", "sid-test", client=client)
    # verify event was emitted
    args, _ = ex._exec_event.call_args
    event = args[0]
    assert event["action"] == "lifecycle_cleanup"
    assert event["sid"] == "sid-test"

def test_monitor_trade_lifecycle_cleanup_on_reversal():
    ex = _make_exec()
    client = MagicMock()

    # Seq: LONG -> SHORT (reversed)
    client.get_position_risk.side_effect = [
        [{"symbol": "BTCUSDT", "positionAmt": "1.0"}], # LONG
        [{"symbol": "BTCUSDT", "positionAmt": "-1.0"}] # SHORT (reversed for LONG sid)
    ]

    with patch("time.sleep"), patch("time.time", side_effect=[100, 101, 102, 103]):
        ex._monitor_trade_lifecycle_thread(
            sid="sid-rev",
            symbol="BTCUSDT",
            logical_side="LONG",
            client=client
        )

    ex._cancel_by_token.assert_called_once_with("BTCUSDT", "sid-rev", client=client)
    args, _ = ex._exec_event.call_args
    event = args[0]
    assert event["reason"] == "reversed"

if __name__ == "__main__":
    import pytest
    pytest.main([__file__])
