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
        assert not self.svc._lock_is_owned()
        self.calls.append(("append_event", getattr(ev, "event_type", "?")))
    def save_tp_hit(self, pos, tp_level, fill_price, closed_qty, pnl_part, ts_ms):
        assert not self.svc._lock_is_owned()
        self.calls.append(("save_tp_hit", tp_level))
    def save_tp_hit_fast(self, **kwargs):
        assert not self.svc._lock_is_owned()
        self.calls.append(("save_tp_hit_fast", kwargs.get("tp_level")))
    def save_trailing_move(self, pos, prev_sl, new_sl, ts_ms):
        assert not self.svc._lock_is_owned()
        self.calls.append(("save_trailing_move", prev_sl, new_sl))
    def save_trailing_move_fast(self, **kwargs):
        assert not self.svc._lock_is_owned()
        self.calls.append(("save_trailing_move_fast", kwargs.get("previous_sl"), kwargs.get("new_sl")))
    def save_trailing_sync(self, pos, ts_ms):
        assert not self.svc._lock_is_owned()
        self.calls.append(("save_trailing_sync", ts_ms))
    def save_trailing_sync_fast(self, **kwargs):
        assert not self.svc._lock_is_owned()
        self.calls.append(("save_trailing_sync_fast", kwargs.get("ts_ms")))
    def save_closed(self, closed, health_snapshot=None):
        assert not self.svc._lock_is_owned()
        self.calls.append(("save_closed", getattr(closed, "symbol", "?")))


def test_on_tick_repo_io_outside_global_lock(monkeypatch):
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
    svc.repo = DummyRepo(svc)

    # Stub spec
    monkeypatch.setattr(tm.TradeMonitorService, "_get_spec", lambda self, symbol: types.SimpleNamespace())

    # Stub build_tick
    tick = types.SimpleNamespace(symbol="BTCUSDT", ts_ms=1_700_000_000_000, mid=100.0)
    monkeypatch.setattr(tm, "build_tick", lambda raw: tick)

    # Create a position
    pos = types.SimpleNamespace(
        id="p1",
        sid="sid1",
        strategy="s",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        sl=90.0,
        tp_levels=[110.0, 120.0, 130.0],
        remaining_qty=1.0,
        tp_hits=0,
        closed=False,
        trailing_started=False,
        trailing_active=False,
        signal_payload={"atr": 1.0},
        is_long=lambda: True,
    )

    with svc._lock:
        svc.open_positions[pos.id] = pos
        svc.pos_by_sid[pos.sid] = pos.id
        svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    # process_tick returns one TP_HIT event + closed trade
    ev_tp = types.SimpleNamespace(event_type="TP_HIT", payload={"tp_level": 1, "fill_price": 110.0, "closed_qty": 0.3, "pnl_part_gross": 1.0})
    closed = types.SimpleNamespace(symbol="BTCUSDT", close_reason_raw="TP1", close_reason="TP1", pnl_net=1.0)

    monkeypatch.setattr(tm, "process_tick", lambda pos, tick, spec, tp_ratios, fill_policy: ([ev_tp], closed))

    svc.on_tick({"symbol": "BTCUSDT", "ts_ms": tick.ts_ms, "mid": 100.0})

    # Ensure I/O happened and did not assert
    assert ("append_event", "TP_HIT") in svc.repo.calls
    assert ("save_tp_hit_fast", 1) in svc.repo.calls
    assert ("save_closed", "BTCUSDT") in svc.repo.calls


def test_external_sl_hit_repo_io_outside_global_lock(monkeypatch):
    from services.trade_monitor import TradeMonitorService
    import services.trade_monitor as tm

    monkeypatch.setattr(tm, "RedisTradeRepository", lambda redis, health_provider=None: types.SimpleNamespace(load_open_positions=lambda limit=5000: []))
    monkeypatch.setattr(tm.analytics_db, "save_trade_closed", lambda closed: None)

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
    svc.repo = DummyRepo(svc)

    # stub spec and finalize_trade
    class Spec:
        def pnl_money(self, entry, exit, qty, direction, symbol=None):
            return 1.0
    monkeypatch.setattr(tm.TradeMonitorService, "_get_spec", lambda self, symbol: Spec())
    monkeypatch.setattr(tm, "finalize_trade", lambda pos, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios: types.SimpleNamespace(
        symbol=pos.symbol, close_reason_raw=close_reason_raw, close_reason="SL", pnl_net=0.1
    ))

    pos = types.SimpleNamespace(
        id="p_sl",
        sid="sidSL",
        strategy="s",
        source="CryptoOrderFlow",
        symbol="ETHUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        sl=90.0,
        remaining_qty=1.0,
        realized_pnl_gross=0.0,
        tp_hits=0,
        trailing_active=False,
        trailing_started=False,
        closed=False,
    )
    with svc._lock:
        svc.open_positions[pos.id] = pos
        svc.pos_by_sid[pos.sid] = pos.id
        svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    ok = svc.apply_external_sl_hit(signal_id="sidSL", price=95.0, timestamp=1_700_000_000_000, event_id="e1")
    assert ok is True
    assert ("save_closed", "ETHUSDT") in svc.repo.calls
