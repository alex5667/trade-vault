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


class FakeRepo:
    # В этом тесте repo не используется (позиции нет), но DI требует объект.
    pass


def test_external_sl_hit_true_when_sid_done_and_no_position():
    from services.trade_monitor import TradeMonitorService

    r = FakeRedis()
    r.set("closed_sid_done:sidX", "1", ex=7 * 24 * 3600)

    svc = TradeMonitorService(redis_client=r, repo=FakeRepo())

    # Нет позиций в памяти/индексах -> _peek_pos_and_symbol_by_sid должен вернуть (None, ..)
    ok = svc.apply_external_sl_hit(signal_id="sidX", price=100.0, timestamp=0, event_id=None)
    assert ok is True
