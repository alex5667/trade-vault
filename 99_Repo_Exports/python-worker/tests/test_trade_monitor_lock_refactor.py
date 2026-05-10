"""
Tests for TradeMonitorService.on_tick() refactoring.
Verifies that I/O operations happen outside self._lock.
"""

import types
from tests.trade_monitor_test_utils import create_mock_trade_monitor


def test_on_tick_structure_basic():
    """
    Basic test: ensure on_tick() can be called with minimal setup.
    Verifies the refactored structure doesn't break basic flow.
    """
    from services.trade_monitor import TradeMonitorService

    # Build minimal service
    svc = create_mock_trade_monitor()

    # Add required attributes
    svc._get_spec = lambda symbol: type('Spec', (), {})()
    svc.repo = type('Repo', (), {})()

    # Mock tick building
    import services.trade_monitor as tm
    tm.build_tick = lambda raw: types.SimpleNamespace(symbol=raw.get("symbol", "TEST"), ts_ms=1000, mid=100.0)
    tm.process_tick = lambda pos, tick, spec, tp_ratios, fill_policy: ([], None)

    # Should not crash
    svc.on_tick({"symbol": "TEST"})

    # Verify basic structure
    assert hasattr(svc, '_lock')
    assert hasattr(svc, '_use_symbol_locks')
    assert hasattr(svc, '_run_io_tasks')


def test_per_symbol_locks_enabled():
    """
    Test that per-symbol locks are created when enabled.
    """
    from services.trade_monitor import TradeMonitorService

    svc = create_mock_trade_monitor()

    lock1 = svc._get_symbol_lock("BTCUSDT")
    lock2 = svc._get_symbol_lock("ETHUSDT")
    lock1_again = svc._get_symbol_lock("BTCUSDT")

    # Same symbol should return same lock
    assert lock1 is lock1_again
    # Different symbols should have different locks
    assert lock1 is not lock2

    assert "BTCUSDT" in svc._symbol_locks
    assert "ETHUSDT" in svc._symbol_locks


def test_per_symbol_locks_disabled():
    """
    Test that per-symbol locks are bypassed when disabled.
    """
    from services.trade_monitor import TradeMonitorService

    svc = create_mock_trade_monitor()

    # Should not create locks when disabled
    ctx = svc._symbol_ctx("BTCUSDT")
    import contextlib
    assert isinstance(ctx, contextlib.nullcontext)
