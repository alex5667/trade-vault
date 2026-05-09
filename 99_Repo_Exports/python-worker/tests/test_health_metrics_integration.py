#!/usr/bin/env python3
"""
Тесты для интеграции метрик lag и pending в HealthMetrics.
"""


from health_metrics import HealthMetrics


class FakeRedis:
    """Фейковый Redis для тестирования."""
    def __init__(self):
        self.kv = {}
        self.hashes = {}

    def set(self, k, v, **kwargs):
        self.kv[k] = v

    def hset(self, k, mapping=None, **kwargs):
        if mapping is None:
            mapping = kwargs
        self.hashes.setdefault(k, {}).update(mapping)

    def pipeline(self):
        return FakePipeline(self)


class FakePipeline:
    """Фейковый pipeline для тестирования."""
    def __init__(self, redis):
        self.redis = redis
        self.operations = []

    def set(self, k, v, **kwargs):
        self.operations.append(('set', k, v, kwargs))
        return self

    def hset(self, k, mapping=None, **kwargs):
        self.operations.append(('hset', k, mapping, kwargs))
        return self

    def expire(self, k, ttl):
        self.operations.append(('expire', k, ttl))
        return self

    def execute(self):
        for op, *args in self.operations:
            if op == 'set':
                k, v, kwargs = args
                self.redis.set(k, v, **kwargs)
            elif op == 'hset':
                k, mapping, kwargs = args
                self.redis.hset(k, mapping, **kwargs)
            elif op == 'expire':
                k, ttl = args
                # Для тестов просто игнорируем expire
                pass
        self.operations = []


def test_healthmetrics_stream_lag_and_pending_flush():
    """Тест агрегации lag/pending + flush в Redis."""
    r = FakeRedis()
    hm = HealthMetrics(redis_url="redis://unused")
    hm._redis = r  # injection для теста

    # simulate events inside window
    hm.on_stream_lag("BTCUSDT", "ticks", 100)
    hm.on_stream_lag("BTCUSDT", "ticks", 300)
    hm.on_stream_lag("BTCUSDT", "book", 50)
    hm.on_stream_lag("BTCUSDT", "l3", 20)

    hm.on_pending_len("BTCUSDT", "ticks", 7)
    hm.on_pending_len("BTCUSDT", "book", 2)
    hm.on_pending_len("BTCUSDT", "l3", 1)

    hm._flush_snapshot()

    # Проверяем Redis keys
    assert r.kv["orderflow:BTCUSDT:ticks_lag_ms"] == (100+300)/2
    assert r.kv["orderflow:BTCUSDT:book_lag_ms"] == 50.0
    assert r.kv["orderflow:BTCUSDT:l3_lag_ms"] == 20.0

    assert r.kv["orderflow:BTCUSDT:pending_len_ticks"] == 7
    assert r.kv["orderflow:BTCUSDT:pending_len_book"] == 2
    assert r.kv["orderflow:BTCUSDT:pending_len_l3"] == 1

    # Проверяем max значения
    assert r.kv["orderflow:BTCUSDT:ticks_lag_ms_max"] == 300
    assert r.kv["orderflow:BTCUSDT:book_lag_ms_max"] == 50
    assert r.kv["orderflow:BTCUSDT:l3_lag_ms_max"] == 20

    assert r.kv["orderflow:BTCUSDT:pending_len_ticks_max"] == 7
    assert r.kv["orderflow:BTCUSDT:pending_len_book_max"] == 2
    assert r.kv["orderflow:BTCUSDT:pending_len_l3_max"] == 1

    # Проверяем health_snapshot hash
    snap = r.hashes["orderflow:BTCUSDT:health_snapshot"]
    assert "ticks_lag_ms_avg" in snap
    assert "book_lag_ms_avg" in snap
    assert "l3_lag_ms_avg" in snap
    assert "pending_len_ticks" in snap
    assert "pending_len_book" in snap
    assert "pending_len_l3" in snap

    # Проверяем значения в snapshot
    assert snap["ticks_lag_ms_avg"] == f"{(100+300)/2:.2f}"
    assert snap["book_lag_ms_avg"] == f"{50.0:.2f}"
    assert snap["l3_lag_ms_avg"] == f"{20.0:.2f}"
    assert snap["pending_len_ticks"] == "7"
    assert snap["pending_len_book"] == "2"
    assert snap["pending_len_l3"] == "1"


def test_on_stream_lag_invalid_values():
    """Тест обработки некорректных значений lag."""
    hm = HealthMetrics(redis_url="redis://unused")

    # Отрицательные значения должны стать 0
    hm.on_stream_lag("BTCUSDT", "ticks", -100)
    bucket = hm._buckets["BTCUSDT"]
    assert bucket.sum_ticks_lag_ms == 0
    assert bucket.max_ticks_lag_ms == 0

    # Положительные значения должны сохраняться
    hm.on_stream_lag("BTCUSDT", "ticks", 150)
    assert bucket.sum_ticks_lag_ms == 150
    assert bucket.max_ticks_lag_ms == 150


def test_on_pending_len_invalid_values():
    """Тест обработки некорректных значений pending."""
    hm = HealthMetrics(redis_url="redis://unused")

    # Отрицательные значения должны стать 0
    hm.on_pending_len("BTCUSDT", "ticks", -5)
    bucket = hm._buckets["BTCUSDT"]
    assert bucket.pending_ticks == 0

    # Положительные значения должны сохраняться
    hm.on_pending_len("BTCUSDT", "ticks", 10)
    assert bucket.pending_ticks == 10
    assert bucket.max_pending_ticks == 10


def test_safe_avg():
    """Тест _safe_avg функции."""
    hm = HealthMetrics(redis_url="redis://unused")

    # Деление на 0
    assert hm._safe_avg(100, 0) == 0.0

    # Нормальное деление
    assert hm._safe_avg(100, 2) == 50.0
    assert hm._safe_avg(150, 3) == 50.0


def test_empty_flush():
    """Тест flush с пустыми buckets."""
    r = FakeRedis()
    hm = HealthMetrics(redis_url="redis://unused")
    hm._redis = r

    # Flush без данных
    hm._flush_snapshot()

    # Redis должен остаться пустым
    assert len(r.kv) == 0
    assert len(r.hashes) == 0


def test_multiple_symbols():
    """Тест работы с несколькими символами."""
    r = FakeRedis()
    hm = HealthMetrics(redis_url="redis://unused")
    hm._redis = r

    # Данные для разных символов
    hm.on_stream_lag("BTCUSDT", "ticks", 100)
    hm.on_pending_len("BTCUSDT", "ticks", 5)

    hm.on_stream_lag("ETHUSDT", "book", 200)
    hm.on_pending_len("ETHUSDT", "book", 3)

    hm._flush_snapshot()

    # Проверяем что данные для обоих символов сохранены
    assert "orderflow:BTCUSDT:ticks_lag_ms" in r.kv
    assert "orderflow:BTCUSDT:pending_len_ticks" in r.kv
    assert "orderflow:ETHUSDT:book_lag_ms" in r.kv
    assert "orderflow:ETHUSDT:pending_len_book" in r.kv

    assert "orderflow:BTCUSDT:health_snapshot" in r.hashes
    assert "orderflow:ETHUSDT:health_snapshot" in r.hashes
