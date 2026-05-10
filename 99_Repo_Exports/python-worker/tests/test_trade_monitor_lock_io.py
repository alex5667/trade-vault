"""
Tests for TradeMonitorService: no I/O under global _lock.
"""
import types
from tests.trade_monitor_test_utils import create_mock_trade_monitor


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
    import services.trade_monitor as tm
    from services.trade_monitor import TradeMonitorService

    # Prevent real RedisTradeRepository init
    monkeypatch.setattr("services.trade_monitor._monolith.RedisTradeRepository", lambda redis, health_provider=None: types.SimpleNamespace(load_open_positions=lambda limit=5000: []))

    # Fake analytics DB
    monkeypatch.setattr("services.trade_monitor._monolith.analytics_db.save_trade_closed", lambda closed: None)

    svc = create_mock_trade_monitor()
    svc.repo = DummyRepo(svc)

    # Mock prometheus metrics
    svc.tm_open_positions = types.SimpleNamespace(labels=lambda **kw: types.SimpleNamespace(set=lambda v: None))
    svc.tm_tick_latency_us = types.SimpleNamespace(labels=lambda **kw: types.SimpleNamespace(observe=lambda v: None))
    svc.tm_orphans_force_closed = types.SimpleNamespace(labels=lambda **kw: types.SimpleNamespace(inc=lambda: None))

    # Stub spec
    monkeypatch.setattr(TradeMonitorService, "_get_spec", lambda self, symbol: types.SimpleNamespace())

    # Stub build_tick
    tick = types.SimpleNamespace(symbol="BTCUSDT", ts_ms=1_700_000_000_000, mid=100.0)
    monkeypatch.setattr("services.trade_monitor._monolith.build_tick", lambda raw: tick)

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
        svc.shards.setdefault(pos.symbol, {})[pos.id] = pos

    # process_tick returns one event and no close
    ev = types.SimpleNamespace(event_type="TRAILING_SYNC", payload={})
    monkeypatch.setattr("services.trade_monitor._monolith.process_tick", lambda p, t, sp, **kwargs: ([ev], None))

    svc.on_tick({"symbol": "BTCUSDT"})
    assert ("append_event", "TRAILING_SYNC") in svc.repo.calls
