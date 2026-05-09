import types

import pytest


class FakeRepo:
    def __init__(self, fail_on_save_closed=False):
        self.fail_on_save_closed = fail_on_save_closed
        self.events = []
        self.closed = []
        self.tp_hits = []
        self.trailing_moves = []
        self.trailing_syncs = []

    def append_event(self, ev):
        self.events.append(ev)

    def save_tp_hit_fast(self, **kw):
        self.tp_hits.append(kw)

    def save_trailing_move_fast(self, **kw):
        self.trailing_moves.append(kw)

    def save_trailing_sync_fast(self, **kw):
        self.trailing_syncs.append(kw)

    def save_closed(self, closed, health_snapshot=None):
        if self.fail_on_save_closed:
            raise RuntimeError("boom save_closed")
        self.closed.append((closed, health_snapshot))


def test_closing_flag_reverted_on_io_failure(monkeypatch):
    import services.trade_monitor as tm
    from services.trade_monitor import TradeMonitorService

    # Prevent real RedisTradeRepository init
    monkeypatch.setattr(tm, "RedisTradeRepository", lambda redis, health_provider=None: types.SimpleNamespace(load_open_positions=lambda limit=5000: []))

    svc = TradeMonitorService.__new__(TradeMonitorService)
    svc._lock = type('MockLock', (), {'_is_owned': lambda: False, '__enter__': lambda self: None, '__exit__': lambda self, *a: None})()
    svc._lock_is_owned = lambda: False
    svc._use_symbol_locks = False
    svc._symbol_locks_guard = type('MockLock', (), {})()
    svc._symbol_locks = {}
    svc._get_symbol_lock = lambda self, symbol: type('MockLock', (), {'__enter__': lambda: None, '__exit__': lambda *a: None})()
    svc._update_last_price = lambda tick: None
    svc._housekeep_expired_positions = lambda ts_ms: None
    svc._run_io_tasks = lambda tasks: [t.fn() for t in tasks]
    svc.open_positions = {}
    svc.pos_by_sid = {}
    svc.open_by_symbol = {}
    svc._last_price_by_symbol = {}
    svc.tp_ratios = (0.3, 0.35, 0.35)
    svc.fill_policy = "level"
    svc._index_remove = lambda pos: None
    svc.regime_guard = None
    svc._attach_health_on_close = False
    svc._IOTask = lambda fn, desc: types.SimpleNamespace(fn=fn, desc=desc)
    svc._dedup_acquire = lambda key, event_id: True  # no-op dedup
    svc.repo = FakeRepo(fail_on_save_closed=True)

    # Create fake open position
    pos = types.SimpleNamespace(
        id="p1",
        sid="sid1",
        strategy="s",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=1700000000000,
        lot=1.0,
        remaining_qty=1.0,
        sl=90.0,
        tp_levels=[110.0, 120.0, 130.0],
        closed=False,
        trailing_started=False,
        trailing_active=False,
    )

    with svc._lock:
        svc.open_positions[pos.id] = pos
        svc.pos_by_sid[pos.sid] = pos.id
        svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    # Monkeypatch process_tick to force "closed" outcome
    class DummyClosed:
        def __init__(self):
            self.symbol = "BTCUSDT"
            self.pnl_net = 0.1
            self.close_reason = "SL"
            self.close_reason_raw = "SL"
            self.__dict__ = {"symbol": "BTCUSDT", "pnl_net": 0.1}

    def fake_process_tick(p, tick, spec, **kw):
        return ([], DummyClosed())

    monkeypatch.setattr(tm, "process_tick", fake_process_tick)

    # Also patch build_tick
    class DummyTick:
        def __init__(self):
            self.symbol = "BTCUSDT"
            self.ts_ms = 1700000001000

    monkeypatch.setattr(tm, "build_tick", lambda raw: DummyTick())

    with pytest.raises(RuntimeError):
        svc.on_tick({"symbol": "BTCUSDT", "ts_ms": 1700000001000, "price": 101.0})

    # After failure, position must still exist and _closing must be reverted to allow retry
    with svc._lock:
        p = svc.open_positions.get("p1")
        assert p is not None
        assert bool(getattr(p, "_closing", False)) is False
