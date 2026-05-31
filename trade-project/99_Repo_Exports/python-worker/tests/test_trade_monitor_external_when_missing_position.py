import threading


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.lock = threading.Lock()

    def get(self, k):
        with self.lock:
            return self.kv.get(k)

    def set(self, k, v, ex=None, nx=False):
        with self.lock:
            self.kv[k] = v
            return True


def test_external_sl_hit_true_if_sid_done(monkeypatch):
    from services.trade_monitor import TradeMonitorService

    r = FakeRedis()
    svc = TradeMonitorService(redis_client=r)  # если ваш __init__ другой — подстройте фабрику

    r.set("closed_sid_done:sidX", "1", ex=7 * 24 * 3600)

    ok = svc.apply_external_sl_hit(signal_id="sidX", price=100.0, timestamp=0, event_id=None)
    assert ok is True
