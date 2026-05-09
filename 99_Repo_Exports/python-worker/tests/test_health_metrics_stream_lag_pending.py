from health_metrics import HealthMetrics


class FakePipe:
    def __init__(self):
        self.ops = []
    def set(self, k, v, ex=None):
        self.ops.append(("set", k, v, ex))
        return self
    def hset(self, k, mapping=None, **kwargs):
        self.ops.append(("hset", k, mapping))
        return self
    def expire(self, k, time):
        self.ops.append(("expire", k, time))
        return self
    def execute(self):
        return True


class FakeRedis:
    def __init__(self):
        self.pipe = FakePipe()
    def pipeline(self):
        return self.pipe


def test_health_metrics_flush_includes_stream_metrics(monkeypatch):
    hm = HealthMetrics.__new__(HealthMetrics)
    hm._window_sec = 5
    hm._buckets = {}
    hm._lock = __import__("threading").Lock()
    hm._stop = __import__("threading").Event()
    hm._redis = FakeRedis()

    hm.on_stream_lag("BTCUSDT", "book", 100)
    hm.on_stream_lag("BTCUSDT", "book", 300)
    hm.on_stream_lag("BTCUSDT", "ticks", 50)
    hm.on_pending_len("BTCUSDT", "book", 10)
    hm.on_pending_len("BTCUSDT", "book", 30)
    hm.on_pending_len("BTCUSDT", "ticks", 7)

    hm._flush_snapshot()

    sets = [op for op in hm._redis.pipe.ops if op[0] == "set"]
    assert any(k.endswith(":book_lag_ms") for (_, k, *_rest) in sets)
    assert any(k.endswith(":ticks_lag_ms") for (_, k, *_rest) in sets)
    assert any(k.endswith(":book_pending_avg") for (_, k, *_rest) in sets)
    assert any(k.endswith(":ticks_pending_avg") for (_, k, *_rest) in sets)

    hsets = [op for op in hm._redis.pipe.ops if op[0] == "hset"]
    assert hsets, "expected health_snapshot hset"
    snap = hsets[0][2]
    assert "avg_book_lag_ms" in snap
    assert "avg_ticks_lag_ms" in snap
    assert "avg_book_pending" in snap
    assert "max_book_pending" in snap
