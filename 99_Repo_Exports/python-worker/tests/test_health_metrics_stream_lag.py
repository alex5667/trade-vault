import time

from health_metrics import HealthMetrics, SymbolBucket


class FakePipe:
    def __init__(self):
        self.ops = []

    def set(self, key, val, ex=None):
        self.ops.append(("set", key, val, ex))
        return self

    def hset(self, key, mapping=None, **kwargs):
        self.ops.append(("hset", key, mapping))
        return self

    def expire(self, key, time):
        self.ops.append(("expire", key, time))
        return self

    def execute(self):
        return True


class FakeRedis:
    def __init__(self):
        self.pipe = FakePipe()

    def pipeline(self):
        return self.pipe


def test_flush_snapshot_includes_stream_lags_and_pending():
    from health_metrics import SymbolBucket

    hm = HealthMetrics.__new__(HealthMetrics)
    hm._window_sec = 5
    hm._buckets = {}

    def mock_get_bucket(symbol):
        if symbol not in hm._buckets:
            hm._buckets[symbol] = SymbolBucket()
        return hm._buckets[symbol]

    hm._get_bucket = mock_get_bucket
    hm._lock = type('MockLock', (), {'__enter__': lambda self: None, '__exit__': lambda self, *args: None})()
    hm._redis = FakeRedis()

    sym = "BTCUSDT"
    hm.on_stream_lag(sym, "ticks", 100)
    hm.on_stream_lag(sym, "ticks", 300)
    hm.on_stream_lag(sym, "book", 50)
    hm.on_stream_lag(sym, "l3", 200)
    hm.set_pending_len(sym, "ticks", 7)
    hm.set_pending_len(sym, "book", 2)
    hm.set_pending_len(sym, "l3", 0)

    # also add minimal activity so snapshot is not skipped
    hm.on_signal_emit(sym)

    hm._flush_snapshot()

    # assert keys were written
    ops = hm._redis.pipe.ops
    set_keys = [op[1] for op in ops if op[0] == "set"]
    assert f"orderflow:{sym}:ticks_lag_ms_avg" in set_keys
    assert f"orderflow:{sym}:book_lag_ms_avg" in set_keys
    assert f"orderflow:{sym}:l3_lag_ms_avg" in set_keys
    assert f"orderflow:{sym}:pending_len_ticks" in set_keys
    assert f"orderflow:{sym}:pending_len_book" in set_keys
    assert f"orderflow:{sym}:pending_len_l3" in set_keys

    # assert health_snapshot hash includes fields
    hsets = [op for op in ops if op[0] == "hset"]
    assert hsets, "expected hset to be called"
    mapping = hsets[0][2]
    assert "avg_ticks_lag_ms" in mapping
    assert "avg_book_lag_ms" in mapping
    assert "avg_l3_lag_ms" in mapping
    assert mapping["pending_len_ticks"] == "7"
    assert mapping["pending_len_book"] == "2"
    assert mapping["pending_len_l3"] == "0"
