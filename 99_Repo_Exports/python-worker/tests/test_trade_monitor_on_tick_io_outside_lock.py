import types
import threading
import pytest


class TrackingLock:
    def __init__(self):
        self._lock = threading.RLock()
        self._owner = None
        self._count = 0

    def acquire(self, *a, **kw):
        ok = self._lock.acquire(*a, **kw)
        if ok:
            tid = threading.get_ident()
            if self._owner is None:
                self._owner = tid
            self._count += 1
        return ok

    def release(self):
        self._count -= 1
        if self._count <= 0:
            self._owner = None
            self._count = 0
        return self._lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()

    def held_by_me(self) -> bool:
        return self._owner == threading.get_ident()


class FakeRepo:
    def __init__(self, lock: TrackingLock):
        self.lock = lock
        self.calls = []

    def _assert_no_lock(self):
        assert not self.lock.held_by_me(), "repo IO called under TradeMonitorService._lock"

    def append_event(self, ev):
        self._assert_no_lock()
        self.calls.append(("append_event", ev.event_type))

    def save_tp_hit(self, *a, **kw):
        self._assert_no_lock()
        self.calls.append(("save_tp_hit", kw.get("tp_level")))

    def save_trailing_move(self, *a, **kw):
        self._assert_no_lock()
        self.calls.append(("save_trailing_move", None))

    def save_trailing_sync(self, *a, **kw):
        self._assert_no_lock()
        self.calls.append(("save_trailing_sync", None))

    def save_closed(self, *a, **kw):
        self._assert_no_lock()
        self.calls.append(("save_closed", None))


def test_on_tick_does_not_do_repo_io_under_lock(monkeypatch):
    # Import module
    from services import trade_monitor as tm

    # stub build_tick
    tick = types.SimpleNamespace(symbol="BTCUSDT", ts_ms=1700000000000, mid=100.0, last=100.0, price=100.0)
    monkeypatch.setattr(tm, "build_tick", lambda raw: tick)

    # stub process_tick (pure)
    class Ev:
        def __init__(self, event_type, payload):
            self.event_type = event_type
            self.payload = payload

    closed = types.SimpleNamespace(symbol="BTCUSDT", pnl_net=1.0)

    def fake_process_tick(pos, tick, spec, **kw):
        evs = [Ev("TP_HIT", {"tp_level": 1, "fill_price": 101.0, "closed_qty": 0.1, "pnl_part_gross": 0.5})]
        return evs, closed

    monkeypatch.setattr(tm, "process_tick", fake_process_tick)
    monkeypatch.setattr(tm, "analytics_db", types.SimpleNamespace(save_trade_closed=lambda c: None))

    # Construct service with injected repo
    lock = TrackingLock()
    repo = FakeRepo(lock)
    s = tm.TradeMonitorService(redis_url=None, config={"monitor": {}}, redis_client=types.SimpleNamespace(), repo=repo)
    s._lock = lock
    s._attach_health_on_close = False

    # minimal position object
    pos = types.SimpleNamespace(
        id="oid1",
        sid="sid1",
        symbol="BTCUSDT",
        source="CryptoOrderFlow",
        strategy="x",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        sl=99.0,
        closed=False,
        tp_hits=0,
        trailing_started=False,
        trailing_active=False,
        signal_payload={},
        is_long=lambda: True,
    )
    s.open_positions["oid1"] = pos
    s.pos_by_sid["sid1"] = "oid1"
    s.open_by_symbol["BTCUSDT"] = {"oid1"}
    s.shards = {"BTCUSDT": {"oid1": pos}}

    # avoid report import
    monkeypatch.setitem(__import__("sys").modules, "services.periodic_reporter", types.SimpleNamespace(check_and_trigger_report=lambda *a, **kw: None))

    # run
    s.on_tick({"symbol": "BTCUSDT", "ts_ms": tick.ts_ms, "price": 100.0})

    # ensure IO happened and position cleaned
    assert ("append_event", "TP_HIT") in repo.calls
    assert any(c[0] == "save_tp_hit" for c in repo.calls)
    assert any(c[0] == "save_closed" for c in repo.calls)
    assert "oid1" not in s.open_positions
