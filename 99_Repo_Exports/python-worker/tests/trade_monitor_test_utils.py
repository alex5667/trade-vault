import types
import contextlib

def create_mock_trade_monitor():
    from services.trade_monitor._monolith import TradeMonitorService
    svc = TradeMonitorService.__new__(TradeMonitorService)

    # Core locks
    class MockLock:
        def __enter__(self): return None
        def __exit__(self, exc_type, exc_val, exc_tb): return None
        def acquire(self, blocking=True): return True
        def release(self): pass
        def _is_owned(self): return False

    import threading
    svc._lock = threading.RLock()
    svc._lock_is_owned = lambda: False
    svc._use_symbol_locks = False
    svc._symbol_locks_guard = threading.RLock()
    svc._symbol_locks = {}


    # Core state
    svc.open_positions = {}
    svc.pos_by_sid = {}
    svc.open_by_symbol = {}
    svc._last_price_by_symbol = {}
    svc._fsm_map = {}
    svc.shards = {}

    # Metrics state (added during decomposition)
    svc._metrics_update_interval_ms = 1000
    svc._last_metrics_update_by_sym = {}
    svc._last_metrics_update_ms = 0
    svc._metrics_batch_size = 50

    svc._trail_tp_activate_level = 1
    svc._simulated_slippage_bps = 0
    svc.trailing_tp1_offset_default = 0.0
    svc.tp_ratios = (0.3, 0.35, 0.35)
    svc.fill_policy = "level"
    
    # Executors
    class DummyExecutor:
        def submit(self, fn, *args, **kwargs):
            fn(*args, **kwargs)
    svc._db_executor = DummyExecutor()
    svc._worker_pool = DummyExecutor()

    svc.regime_guard = None
    svc._attach_health_on_close = False
    svc.logger = types.SimpleNamespace(info=lambda *a: None, warning=lambda *a: None, error=lambda *a: None, debug=lambda *a: None)

    # I/O Handlers
    svc._update_last_price = lambda tick: None
    svc._housekeep_expired_positions = lambda ts_ms, **kwargs: None
    svc._run_io_tasks = lambda tasks: [t.fn() for t in tasks]
    svc._IOTask = lambda fn, desc: types.SimpleNamespace(fn=fn, desc=desc)
    svc._dedup_acquire = lambda key, event_id: True  # no-op dedup
    svc._index_remove = lambda pos: None
    
    from services.trade_monitor._monolith import TM_OPEN_POSITIONS, TM_TICK_LATENCY_US
    svc.tm_open_positions = TM_OPEN_POSITIONS
    svc.tm_tick_latency_us = TM_TICK_LATENCY_US

    return svc
