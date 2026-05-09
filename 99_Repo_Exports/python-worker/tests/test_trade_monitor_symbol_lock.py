import threading
import time
from types import SimpleNamespace

from utils.time_utils import get_ny_time_millis


class FakeRepo:
    def __init__(self):
        self.events = []
        self.trailing_sync = 0

    def append_event(self, ev):
        self.events.append(ev)

    def save_trailing_sync(self, pos, ts_ms):
        self.trailing_sync += 1


class FakeRedis:
    def __init__(self):
        self.kv = {}

    def set(self, key, value, nx=False, ex=None, xx=False):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    def delete(self, key):
        self.kv.pop(key, None)
        return 1


def test_symbol_lock_serializes_tick_and_external(monkeypatch):
    """
    Ensures: while on_tick(symbol) is inside process_tick(), external update for same symbol
    cannot enter apply_trailing_update (blocked by symbol lock).
    """
    from domain.models import PositionState
    from services.trade_monitor import TradeMonitorService

    repo = FakeRepo()
    r = FakeRedis()

    svc = TradeMonitorService(redis_client=r, repo=repo)
    svc._use_symbol_locks = True
    svc.tp_ratios = (0.3, 0.35, 0.35)
    svc.fill_policy = "level"
    svc._update_last_price = lambda tick: None
    svc._housekeep_expired_positions = lambda ts_ms: None

    # Minimal open position registered
    pos = PositionState(
        id="oid1",
        sid="sid1",
        strategy="s",
        source="CryptoOrderFlow",
        symbol="ETHUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=get_ny_time_millis(),
        lot=1.0,
        remaining_qty=1.0,
        sl=99.0,
        tp_levels=[101.0, 102.0, 103.0],
    )

    with svc._lock:
        svc.open_positions[pos.id] = pos
        svc.pos_by_sid[pos.sid] = pos.id
        svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    started = threading.Event()
    allow_finish = threading.Event()
    external_entered = threading.Event()

    def fake_build_tick(_raw):
        return SimpleNamespace(symbol="ETHUSDT", ts_ms=get_ny_time_millis(), mid=100.0, last=100.0, price=100.0)

    def fake_process_tick(_pos, _tick, _spec, **kwargs):
        started.set()
        # Hold symbol-lock for a moment to simulate long compute
        assert allow_finish.wait(timeout=1.5), "timeout waiting allow_finish"
        return ([], None)

    def fake_apply_trailing_update(_pos, **kwargs):
        external_entered.set()
        return None

    monkeypatch.setattr("services.trade_monitor.build_tick", fake_build_tick)
    monkeypatch.setattr("services.trade_monitor.process_tick", fake_process_tick)
    monkeypatch.setattr("services.trade_monitor.apply_trailing_update", fake_apply_trailing_update)
    monkeypatch.setattr("services.trade_monitor.get_symbol_info", lambda *a, **k: {})
    monkeypatch.setattr("services.trade_monitor.spec_from_symbol_info", lambda *a, **k: SimpleNamespace())

    t_tick = threading.Thread(target=lambda: svc.on_tick({"symbol": "ETHUSDT"}), daemon=True)
    t_tick.start()

    assert started.wait(timeout=1.0), "process_tick did not start"

    # External thread should block on symbol-lock until allow_finish is set
    t_ext = threading.Thread(target=lambda: svc.update_trailing_sl("sid1", new_sl=100.5, event_id="e1"), daemon=True)
    t_ext.start()

    time.sleep(0.15)
    assert not external_entered.is_set(), "external entered while tick was running (symbol lock broken)"

    allow_finish.set()
    t_tick.join(timeout=2.0)
    t_ext.join(timeout=2.0)

    assert external_entered.is_set(), "external never entered apply_trailing_update"
