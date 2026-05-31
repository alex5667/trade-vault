# python-worker/tests/test_health_metrics_streams.py


from health_metrics import HealthMetrics, SymbolBucket


class DummyPipe:
    def __init__(self):
        self.set_calls = []
        self.hset_calls = []
        self.expire_calls = []

    def set(self, key, value, ex=None):
        self.set_calls.append((key, value, ex))
        return self

    def hset(self, key, mapping=None, **kwargs):
        self.hset_calls.append((key, dict(mapping or {})))
        return self

    def expire(self, key, time):
        self.expire_calls.append((key, time))
        return self

    def execute(self):
        return True


class DummyRedis:
    def __init__(self):
        self.pipe = DummyPipe()

    def pipeline(self):
        return self.pipe


def test_flush_snapshot_writes_stream_metrics():
    hm = HealthMetrics(redis_url="redis://unused", window_sec=5)
    hm._redis = DummyRedis()  # inject stub

    # inject bucket
    b = SymbolBucket()
    b.ticks_total = 10

    b.sum_book_lag_ms = 300
    b.cnt_book_lag = 3   # avg 100
    b.sum_ticks_lag_ms = 40
    b.cnt_ticks_lag = 2  # avg 20
    b.sum_l3_lag_ms = 0
    b.cnt_l3_lag = 0     # avg 0

    # new pending fields with aggregation
    b.sum_book_pending = 33  # 11+11+11
    b.cnt_book_pending = 3   # avg 11
    b.max_book_pending = 11

    b.sum_ticks_pending = 66  # 22+22+22
    b.cnt_ticks_pending = 3   # avg 22
    b.max_ticks_pending = 22

    b.sum_l3_pending = 99  # 33+33+33
    b.cnt_l3_pending = 3    # avg 33
    b.max_l3_pending = 33

    hm._buckets["BTCUSDT"] = b

    hm._flush_snapshot()

    pipe = hm._redis.pipe
    keys = [k for (k, v, ex) in pipe.set_calls]

    assert "orderflow:BTCUSDT:book_lag_ms" in keys
    assert "orderflow:BTCUSDT:ticks_lag_ms" in keys
    assert "orderflow:BTCUSDT:l3_lag_ms" in keys

    assert "orderflow:BTCUSDT:book_pending_avg" in keys
    assert "orderflow:BTCUSDT:ticks_pending_avg" in keys
    assert "orderflow:BTCUSDT:l3_pending_avg" in keys

    # check values
    kv = {k: v for (k, v, ex) in pipe.set_calls}
    assert abs(float(kv["orderflow:BTCUSDT:book_lag_ms"]) - 100.0) < 1e-6
    assert abs(float(kv["orderflow:BTCUSDT:ticks_lag_ms"]) - 20.0) < 1e-6
    assert abs(float(kv["orderflow:BTCUSDT:book_pending_avg"]) - 11.0) < 1e-6
    assert abs(float(kv["orderflow:BTCUSDT:ticks_pending_avg"]) - 22.0) < 1e-6

    # check snapshot contains fields
    assert pipe.hset_calls, "health_snapshot hset should be called"
    snap_key, mapping = pipe.hset_calls[-1]
    assert snap_key == "orderflow:BTCUSDT:health_snapshot"
    assert "avg_book_lag_ms" in mapping
    assert "avg_book_pending" in mapping
    assert "max_book_pending" in mapping
