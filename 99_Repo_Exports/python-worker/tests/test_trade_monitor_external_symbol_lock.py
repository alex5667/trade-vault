"""
Tests for external methods using symbol-lock + I/O outside global lock.
"""
import types
from tests.trade_monitor_test_utils import create_mock_trade_monitor


def test_external_sl_hit_repo_io_outside_global_lock(monkeypatch):
    import services.trade_monitor as tm
    from services.trade_monitor import TradeMonitorService

    # stub finalize_trade (pure)
    monkeypatch.setattr("services.trade_monitor._monolith.finalize_trade", lambda pos, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios: types.SimpleNamespace(
        symbol=pos.symbol, close_reason="SL", close_reason_raw=close_reason_raw, pnl_net=0.1
    ))

    # stub spec
    class Spec:
        def pnl_money(self, entry, exit, qty, direction, symbol=None):
            return 1.0
    monkeypatch.setattr(TradeMonitorService, "_get_spec", lambda self, symbol: Spec())

    # no-op analytics + health
    monkeypatch.setattr("services.trade_monitor._monolith.analytics_db.save_trade_closed", lambda c: None)
    monkeypatch.setattr(TradeMonitorService, "_get_health_snapshot_for_trade", lambda self, sym: {})
    monkeypatch.setattr(TradeMonitorService, "_get_health_snapshot_prefixed", lambda self, sym, now_ms: {})

    svc = create_mock_trade_monitor()

    # fake repo asserts: no global lock held
    class FakeRepo:
        def __init__(self, svc):
            self.svc = svc
            self.calls = []

        def append_event(self, ev):
            assert not self.svc._lock_is_owned()
            self.calls.append(("append", ev.event_type))

        def save_closed(self, closed, health_snapshot=None):
            assert not self.svc._lock_is_owned()
            self.calls.append(("save_closed", getattr(closed, "symbol", "")))

    svc.repo = FakeRepo(svc)

    # stub stats (also must be outside lock)
    monkeypatch.setattr(tm.TradeMonitorService, "_update_stats_from_dicts",
                        lambda self, p, c: (assert_not_owned(self)))

    def assert_not_owned(service):
        assert not service._lock_is_owned()

    # create open pos
    pos = types.SimpleNamespace(
        id="p_sl",
        sid="sid1",
        symbol="BTCUSDT",
        source="CryptoOrderFlow",
        strategy="s",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        sl=90.0,
        remaining_qty=0.5,
        realized_pnl_gross=0.0,
        tp_hits=0,
        trailing_active=False,
        closed=False,
    )

    with svc._lock:
        svc.open_positions[pos.id] = pos
        svc.pos_by_sid[pos.sid] = pos.id
        svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)
        svc.shards.setdefault(pos.symbol, {})[pos.id] = pos

    ok = svc.apply_external_sl_hit(signal_id="sid1", price=95.0, timestamp=1_700_000_000_000, event_id="e1")
    assert ok is True
    assert ("save_closed", "BTCUSDT") in svc.repo.calls


def test_update_trailing_sl_repo_io_outside_global_lock(monkeypatch):
    import services.trade_monitor as tm
    from services.trade_monitor import TradeMonitorService

    svc = create_mock_trade_monitor()

    # stub apply_trailing_update -> returns dummy event
    ev = types.SimpleNamespace(event_type="TRAILING_SYNC", payload={"new_sl": 101.0})
    monkeypatch.setattr("services.trade_monitor._monolith.apply_trailing_update", lambda pos, new_sl, ts_ms, trailing_distance, point_size, clear_future_tp_levels: ev)

    class FakeRepo:
            def __init__(self, svc):
                self.svc = svc
                self.calls = []

            def append_event(self, evv):
                assert not self.svc._lock_is_owned()
                self.calls.append(("append", evv.event_type))

            def save_trailing_sync(self, pos, ts):
                assert not self.svc._lock_is_owned()
                self.calls.append(("sync", pos.id))

            def save_trailing_sync_fast(self, **kwargs):
                assert not self.svc._lock_is_owned()
                self.calls.append(("sync_fast", kwargs.get("order_id")))

    svc.repo = FakeRepo(svc)

    pos = types.SimpleNamespace(
        id="p_tr",
        sid="sid2",
        symbol="ETHUSDT",
        source="CryptoOrderFlow",
        strategy="s",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        sl=90.0,
        closed=False,
    )
    with svc._lock:
        svc.open_positions[pos.id] = pos
        svc.pos_by_sid[pos.sid] = pos.id
        svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)
        svc.shards.setdefault(pos.symbol, {})[pos.id] = pos

    ok = svc.update_trailing_sl(signal_id="sid2", new_sl=101.0, event_id="ev2")
    assert ok is True
    assert ("append", "TRAILING_SYNC") in svc.repo.calls
    # The actual implementation uses save_trailing_sync, not save_trailing_sync_fast
    assert ("sync", "p_tr") in svc.repo.calls

