"""
Tests for TradeMonitorService: no I/O under global _lock.
"""
import types


class DummyRepo:
    def __init__(self, svc):
        self.svc = svc
        self.calls = []
    def load_open_positions(self, limit=5000):
        return []
    def append_event(self, ev):
        assert self.svc._lock_is_owned() is False
        self.calls.append(("append_event", getattr(ev, "event_type", "?")))
    def save_tp_hit(self, *args, **kwargs):
        assert self.svc._lock_is_owned() is False
        self.calls.append(("save_tp_hit",))
    def save_trailing_move(self, *args, **kwargs):
        assert self.svc._lock_is_owned() is False
        self.calls.append(("save_trailing_move",))
    def save_trailing_sync(self, *args, **kwargs):
        assert self.svc._lock_is_owned() is False
        self.calls.append(("save_trailing_sync",))
    def save_closed(self, *args, **kwargs):
        assert self.svc._lock_is_owned() is False
        self.calls.append(("save_closed",))
    def save_tp_hit_fast(self, **kwargs):
        assert self.svc._lock_is_owned() is False
        self.calls.append(("save_tp_hit_fast",))
    def save_trailing_move_fast(self, **kwargs):
        assert self.svc._lock_is_owned() is False
        self.calls.append(("save_trailing_move_fast",))
    def save_trailing_sync_fast(self, **kwargs):
        assert self.svc._lock_is_owned() is False
        self.calls.append(("save_trailing_sync_fast",))


def test_on_tick_does_not_do_io_under_global_lock(monkeypatch):
    from services.trade_monitor import TradeMonitorService
    import services.trade_monitor as tm

    # Prevent real RedisTradeRepository init
    monkeypatch.setattr(tm, "RedisTradeRepository", lambda redis, health_provider=None: types.SimpleNamespace(load_open_positions=lambda limit=5000: []))

    # Fake analytics DB
    monkeypatch.setattr(tm.analytics_db, "save_trade_closed", lambda closed: None)

    svc = TradeMonitorService.__new__(TradeMonitorService)
    svc._lock = type('MockLock', (), {'_is_owned': lambda: False, '__enter__': lambda self: None, '__exit__': lambda self, *a: None})()
    svc._lock_is_owned = lambda: False
    svc._use_symbol_locks = False
    svc._symbol_locks_guard = type('MockLock', (), {})()
    svc._symbol_locks = {}
    svc._get_symbol_lock = lambda self, symbol: type('MockLock', (), {'__enter__': lambda: None, '__exit__': lambda *a: None})()
    svc._update_last_price = lambda tick: None
    svc._housekeep_expired_positions = lambda ts_ms, **kwargs: None
    svc._run_io_tasks = lambda tasks: [t.fn() for t in tasks]
    svc.open_positions = {}
    svc.pos_by_sid = {}
    svc.open_by_symbol = {}
    svc.shards = {}
    svc._last_price_by_symbol = {}
    svc.tp_ratios = (0.3, 0.35, 0.35)
    svc.fill_policy = "level"
    svc._index_remove = lambda pos: None
    svc.regime_guard = None
    svc._attach_health_on_close = False
    svc._IOTask = lambda fn, desc: types.SimpleNamespace(fn=fn, desc=desc)
    svc._dedup_acquire = lambda key, event_id: True  # no-op dedup
    svc.repo = DummyRepo(svc)
    
    # Mock prometheus metrics
    svc.tm_open_positions = types.SimpleNamespace(labels=lambda **kw: types.SimpleNamespace(set=lambda v: None))
    svc.tm_tick_latency_us = types.SimpleNamespace(labels=lambda **kw: types.SimpleNamespace(observe=lambda v: None))
    svc.tm_orphans_force_closed = types.SimpleNamespace(labels=lambda **kw: types.SimpleNamespace(inc=lambda: None))

    # Stub spec
    monkeypatch.setattr(TradeMonitorService, "_get_spec", lambda self, symbol: types.SimpleNamespace())

    # Stub build_tick
    tick = types.SimpleNamespace(symbol="BTCUSDT", ts_ms=1_700_000_000_000, mid=100.0)
    monkeypatch.setattr(tm, "build_tick", lambda raw: tick)

    # Create a position
    pos = types.SimpleNamespace(
        id="p1",
        sid="s1",
        strategy="st",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        sl=90.0,
        tp_levels=[110.0, 120.0, 130.0],
        remaining_qty=1.0,
        realized_pnl_gross=0.0,
        closed=False,
    )

    with svc._lock:
        svc.open_positions[pos.id] = pos
        svc.pos_by_sid[pos.sid] = pos.id
        svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)
        svc.shards.setdefault(pos.symbol, {})[pos.id] = pos

    # process_tick returns one event and no close
    ev = types.SimpleNamespace(event_type="TRAILING_SYNC", payload={})
    monkeypatch.setattr(tm, "process_tick", lambda p, t, sp, **kwargs: ([ev], None))

    svc.on_tick({"symbol": "BTCUSDT"})
    assert ("append_event", "TRAILING_SYNC") in svc.repo.calls
