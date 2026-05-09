import threading
from types import SimpleNamespace

from utils.time_utils import get_ny_time_millis


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.lock = threading.Lock()

    def get(self, k):
        with self.lock:
            return self.kv.get(k)

    def set(self, k, v, nx=False, ex=None, xx=False):
        with self.lock:
            if nx and k in self.kv:
                return None
            if xx and k not in self.kv:
                return None
            self.kv[k] = v
            return True

    def delete(self, k):
        with self.lock:
            self.kv.pop(k, None)
        return 1

    def hgetall(self, k):
        return {}


class FakeRepo:
    def __init__(self):
        self.events = []
        self.closed = []

    def append_event(self, ev):
        self.events.append(ev)

    def save_closed(self, closed, health_snapshot=None):
        self.closed.append((closed, health_snapshot or {}))


def test_external_sl_hit_returns_true_if_sid_already_closed(monkeypatch):
    from services.trade_monitor import TradeMonitorService

    r = FakeRedis()
    repo = FakeRepo()

    svc = TradeMonitorService(redis_client=r, repo=repo)  # requires your __init__ to accept injections

    # mark sid as closed (simulating previous run)
    svc.redis.set("closed_sid_done:sid123", "1", ex=7 * 24 * 3600)

    ok = svc.apply_external_sl_hit(signal_id="sid123", price=100.0, timestamp=get_ny_time_millis(), event_id=None)
    assert ok is True
    assert repo.events == []
    assert repo.closed == []


def test_external_sl_hit_closes_and_marks_sid_closed(monkeypatch):
    from domain.models import PositionState
    from services.trade_monitor import TradeMonitorService

    r = FakeRedis()
    repo = FakeRepo()
    svc = TradeMonitorService(redis_client=r, repo=repo)

    # Minimal spec
    class Spec:
        def pnl_money(self, entry, exit, qty, direction, symbol=None):
            # LONG: pnl = (exit-entry)*qty
            return (exit - entry) * qty
    svc._get_spec = lambda symbol: Spec()

    # finalize_trade stub
    import services.trade_monitor as tm
    def _finalize_stub(pos, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios):
        return SimpleNamespace(
            symbol=pos.symbol,
            pnl_net=getattr(pos, "realized_pnl_gross", 0.0),
            close_reason="CLOSE",
            close_reason_raw=str(close_reason_raw),
        )
    monkeypatch.setattr(tm, "finalize_trade", _finalize_stub)

    # analytics no-op
    import services.analytics_db as adb
    monkeypatch.setattr(adb, "save_trade_closed", lambda *a, **k: None)

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
    pos.realized_pnl_gross = 0.0

    with svc._lock:
        svc.open_positions[pos.id] = pos
        svc.pos_by_sid[pos.sid] = pos.id
        svc.open_by_symbol.setdefault(pos.symbol, set()).add(pos.id)

    ok = svc.apply_external_sl_hit(signal_id="sid1", price=95.0, timestamp=get_ny_time_millis(), event_id="e1")
    assert ok is True
    # in-memory removed
    with svc._lock:
        assert "oid1" not in svc.open_positions
        assert "sid1" not in svc.pos_by_sid
    # repo got close
    assert len(repo.closed) == 1
    # sid marked closed
    assert svc.redis.get("closed_sid_done:sid1") is not None
