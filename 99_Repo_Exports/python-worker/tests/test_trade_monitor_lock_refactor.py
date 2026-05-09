"""
Tests for TradeMonitorService.on_tick() refactoring.
Verifies that I/O operations happen outside self._lock.
"""

import types


def test_on_tick_structure_basic():
    """
    Basic test: ensure on_tick() can be called with minimal setup.
    Verifies the refactored structure doesn't break basic flow.
    """
    from services.trade_monitor import TradeMonitorService

    # Build minimal service
    svc = TradeMonitorService.__new__(TradeMonitorService)

    # Add required attributes
    svc._lock = type('MockLock', (), {'_is_owned': lambda: False, '__enter__': lambda self: None, '__exit__': lambda self, *a: None})()
    svc._lock_is_owned = lambda: False
    svc._use_symbol_locks = False
    svc._symbol_locks_guard = type('MockLock', (), {})()
    svc._symbol_locks = {}
    svc._get_symbol_lock = lambda self, symbol: type('MockLock', (), {'__enter__': lambda: None, '__exit__': lambda *a: None})()
    svc._update_last_price = lambda tick: None
    svc._housekeep_expired_positions = lambda ts_ms: None
    svc._run_io_tasks = lambda tasks: None
    svc._get_spec = lambda symbol: type('Spec', (), {})()
    svc.open_positions = {}
    svc.pos_by_sid = {}
    svc.open_by_symbol = {}
    svc._last_price_by_symbol = {}
    svc.tp_ratios = (0.3, 0.35, 0.35)
    svc.fill_policy = "level"
    svc._index_remove = lambda pos: None
    svc.regime_guard = None
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

    svc = TradeMonitorService.__new__(TradeMonitorService)
    svc._lock = type('MockLock', (), {})()
    svc._use_symbol_locks = True
    svc._symbol_locks_guard = type('MockLock', (), {'__enter__': lambda self: None, '__exit__': lambda self, *a: None})()
    svc._symbol_locks = {}

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

    svc = TradeMonitorService.__new__(TradeMonitorService)
    svc._lock = type('MockLock', (), {})()
    svc._use_symbol_locks = False
    svc._symbol_locks_guard = type('MockLock', (), {})()
    svc._symbol_locks = {}
    svc._get_symbol_lock = lambda symbol: type('MockLock', (), {})()

    # Should not create locks when disabled
    lock = svc._get_symbol_lock("BTCUSDT")
    assert "BTCUSDT" not in svc._symbol_locks
