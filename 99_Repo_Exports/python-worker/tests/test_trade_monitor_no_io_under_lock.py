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
    import services.trade_monitor as tm
    from services.trade_monitor import TradeMonitorService

    # Prevent real RedisTradeRepository init
    monkeypatch.setattr("services.trade_monitor._monolith.RedisTradeRepository", lambda redis, health_provider=None: types.SimpleNamespace(load_open_positions=lambda limit=5000: []))

    # Fake analytics DB
    monkeypatch.setattr("services.trade_monitor._monolith.analytics_db.save_trade_closed", lambda closed: None)

    svc = create_mock_trade_monitor()
    svc.repo = DummyRepo(svc)

    # Stub spec
    monkeypatch.setattr(tm.TradeMonitorService, "_get_spec", lambda self, symbol: types.SimpleNamespace())

    # Stub build_tick
    tick = types.SimpleNamespace(symbol="BTCUSDT", ts_ms=1_700_000_000_000, mid=100.0)
    monkeypatch.setattr("services.trade_monitor._monolith.build_tick", lambda raw: tick)

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
        svc.shards.setdefault(pos.symbol, {})[pos.id] = pos

    # process_tick returns one TP_HIT event + closed trade
    ev_tp = types.SimpleNamespace(event_type="TP_HIT", payload={"tp_level": 1, "fill_price": 110.0, "closed_qty": 0.3, "pnl_part_gross": 1.0})
    closed = types.SimpleNamespace(symbol="BTCUSDT", close_reason_raw="TP1", close_reason="TP1", pnl_net=1.0)

    monkeypatch.setattr("services.trade_monitor._monolith.process_tick", lambda pos, tick, spec, tp_ratios, fill_policy: ([ev_tp], closed))

    svc.on_tick({"symbol": "BTCUSDT", "ts_ms": tick.ts_ms, "mid": 100.0})

    # Ensure I/O happened and did not assert
    assert ("append_event", "TP_HIT") in svc.repo.calls
    assert ("save_tp_hit", 1) in svc.repo.calls
    assert ("save_closed", "BTCUSDT") in svc.repo.calls


def test_external_sl_hit_repo_io_outside_global_lock(monkeypatch):
    import services.trade_monitor as tm
    from services.trade_monitor import TradeMonitorService

    monkeypatch.setattr("services.trade_monitor._monolith.RedisTradeRepository", lambda redis, health_provider=None: types.SimpleNamespace(load_open_positions=lambda limit=5000: []))
    monkeypatch.setattr("services.trade_monitor._monolith.analytics_db.save_trade_closed", lambda closed: None)

    svc = create_mock_trade_monitor()
    svc.repo = DummyRepo(svc)

    # stub spec and finalize_trade
    class Spec:
        def pnl_money(self, entry, exit, qty, direction, symbol=None):
            return 1.0
    monkeypatch.setattr(tm.TradeMonitorService, "_get_spec", lambda self, symbol: Spec())
    monkeypatch.setattr("services.trade_monitor._monolith.finalize_trade", lambda pos, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios: types.SimpleNamespace(
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
        svc.shards.setdefault(pos.symbol, {})[pos.id] = pos

    ok = svc.apply_external_sl_hit(signal_id="sidSL", price=95.0, timestamp=1_700_000_000_000, event_id="e1")
    assert ok is True
    assert ("save_closed", "ETHUSDT") in svc.repo.calls
