from utils.time_utils import get_ny_time_millis
"""
Integration tests for SignalOutboxPublisher deduplication with real Redis.
Tests the most critical exactly-once invariants.
"""
import json
import time
import pytest
from core.signal_outbox import SignalOutboxPublisher, OutboxSettings, _LUA_DEDUP_AND_OUTBOX


class TestOutboxDeduplication:
    """Тесты дедупликации в outbox - основа exactly-once."""

    def test_dedup_same_bucket_blocks_second_publish(self, r):
        """Дедуп в одном бакете блокирует вторую публикацию."""
        settings = OutboxSettings(
            outbox_stream="stream:signals:outbox",
            outbox_maxlen=20000,
            dedup_ttl_ms=60000,
            dedup_bucket_ms=60000,
        )
        outbox = SignalOutboxPublisher(redis_client=r, settings=settings)

        ts_ms = get_ny_time_millis()
        env = {"sid": "signal_1", "ts_ms": ts_ms, "symbol": "BTCUSDT"}

        # Первая публикация должна пройти
        id1 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env,
        )
        assert id1 is not None

        # Вторая публикация в том же бакете должна быть заблокирована дедупом
        id2 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env,
        )
        assert id2 is None  # dedup сработал

        # В outbox должен быть только один сигнал
        assert r.xlen("stream:signals:outbox") == 1

    def test_dedup_different_bucket_allows_publish(self, r):
        """Разные бакеты позволяют публикацию (дедуп не срабатывает)."""
        settings = OutboxSettings(
            outbox_stream="stream:signals:outbox",
            dedup_bucket_ms=60000,  # 1 минута
        )
        outbox = SignalOutboxPublisher(redis_client=r, settings=settings)

        ts1 = 1700000000000  # bucket 1
        ts2 = 1700000600000  # bucket 2 (через 1 минуту)

        env1 = {"sid": "signal_1", "ts_ms": ts1, "symbol": "BTCUSDT"}
        env2 = {"sid": "signal_2", "ts_ms": ts2, "symbol": "BTCUSDT"}

        id1 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts1, envelope=env1,
        )
        id2 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts2, envelope=env2,
        )

        assert id1 is not None
        assert id2 is not None  # разные бакеты - дедуп не сработал
        assert r.xlen("stream:signals:outbox") == 2

    def test_lua_rollback_on_xadd_error(self, r):
        """Критический тест: Lua должен откатывать дедуп-ключ при ошибке XADD."""
        settings = OutboxSettings(outbox_stream="stream:signals:outbox")
        outbox = SignalOutboxPublisher(redis_client=r, settings=settings)

        ts_ms = get_ny_time_millis()
        # Пользуемся методом класса для получения актуального формата ключа
        dedup_key = outbox.build_dedup_key(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="", reason="",
            ts_ms=ts_ms, bucket_ms=60000
        )
        outbox_stream = settings.outbox_stream

        envelope_json = json.dumps({"sid": "signal_rollback", "ts_ms": ts_ms}, ensure_ascii=False, separators=(",", ":"))

        sha = r.script_load(_LUA_DEDUP_AND_OUTBOX)

        # Намеренно ломает XADD - maxlen не число (NaN)
        # Это вызывает ResponseError в redis-py, а не возврат значения
        with pytest.raises(Exception) as excinfo:
            r.evalsha(sha, 2, dedup_key, outbox_stream, "60000", "NaN", envelope_json)

        # КРИТИЧНО: дедуп-ключ не должен быть создан (или должен быть удален, если Lua упадет позже)
        assert r.exists(dedup_key) == 0

        # И в outbox ничего не должно быть записано
        assert r.xlen(outbox_stream) == 0

    def test_publish_returns_correct_result_flags(self, r):
        """publish() должен возвращать правильные флаги sent/dedup."""
        settings = OutboxSettings(outbox_stream="stream:signals:outbox")
        outbox = SignalOutboxPublisher(redis_client=r, settings=settings)

        ts_ms = get_ny_time_millis()
        env = {"sid": "signal_test", "ts_ms": ts_ms, "symbol": "BTCUSDT"}

        # Первый паблиш - должен быть sent, не dedup
        result1 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env,
        )
        # publish возвращает msg_id или None
        # В коде: если msg_id None -> dedup=True, sent=False
        # если msg_id есть -> sent=True, dedup=False

        # Для первого паблиш - должен быть msg_id
        assert result1 is not None

        # Второй паблиш того же - dedup
        result2 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env,
        )
        assert result2 is None  # dedup сработал

    def test_dedup_ttl_expiration(self, r):
        """Дедуп ключ должен истекать по TTL."""
        settings = OutboxSettings(
            outbox_stream="stream:signals:outbox",
            dedup_ttl_ms=1000,  # короткий TTL для теста
        )
        outbox = SignalOutboxPublisher(redis_client=r, settings=settings)

        ts_ms = get_ny_time_millis()
        env = {"sid": "signal_ttl", "ts_ms": ts_ms, "symbol": "BTCUSDT"}

        # Первый паблиш
        id1 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env,
        )
        assert id1 is not None

        # Второй сразу - dedup
        id2 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env,
        )
        assert id2 is None

        # Ждем истечения TTL
        time.sleep(1.1)

        # Теперь дедуп должен позволить публикацию
        id3 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env,
        )
        assert id3 is not None

        # Должно быть 2 сообщения в outbox
        assert r.xlen("stream:signals:outbox") == 2

    def test_different_level_keys_allow_publish(self, r):
        """Разные level_key позволяют публикацию (даже в одном бакете)."""
        settings = OutboxSettings(outbox_stream="stream:signals:outbox")
        outbox = SignalOutboxPublisher(redis_client=r, settings=settings)

        ts_ms = get_ny_time_millis()
        env1 = {"sid": "signal_pp", "ts_ms": ts_ms, "symbol": "BTCUSDT", "level_key": "PP"}
        env2 = {"sid": "signal_r1", "ts_ms": ts_ms, "symbol": "BTCUSDT", "level_key": "R1"}

        id1 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="PP",
            ts_ms=ts_ms, envelope=env1,
        )
        id2 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="R1",
            ts_ms=ts_ms, envelope=env2,
        )

        assert id1 is not None
        assert id2 is not None  # разные level_key - дедуп не сработал
        assert r.xlen("stream:signals:outbox") == 2
    def test_different_detection_reasons_are_deduplicated(self, r):
        """Разные detection_reason теперь дедуплицируются, чтобы не было дублей при изменении причины."""
        settings = OutboxSettings(outbox_stream="stream:signals:outbox")
        outbox = SignalOutboxPublisher(redis_client=r, settings=settings)

        ts_ms = get_ny_time_millis()
        env1 = {"sid": "s1", "ts_ms": ts_ms, "symbol": "BTCUSDT", "detection_reason": "RSI"}
        env2 = {"sid": "s2", "ts_ms": ts_ms, "symbol": "BTCUSDT", "detection_reason": "MACD"}

        id1 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env1,
        )
        id2 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env2,
        )

        assert id1 is not None
        assert id2 is None  # разные причины больше не влияют на дедуп (теперь дедуплицируется)
        assert r.xlen("stream:signals:outbox") == 1

    def test_different_fingerprints_allow_publish(self, r):
        """Разные fingerprints позволяют публикацию (даже в одном бакете)."""
        settings = OutboxSettings(outbox_stream="stream:signals:outbox")
        outbox = SignalOutboxPublisher(redis_client=r, settings=settings)

        ts_ms = get_ny_time_millis()
        env1 = {"sid": "s1", "ts_ms": ts_ms, "symbol": "BTCUSDT", "fingerprint": "fp1"}
        env2 = {"sid": "s2", "ts_ms": ts_ms, "symbol": "BTCUSDT", "fingerprint": "fp2"}

        id1 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env1,
        )
        id2 = outbox.publish(
            source="test", strategy="test", symbol="BTCUSDT",
            side="buy", kind="test", level_key="",
            ts_ms=ts_ms, envelope=env2,
        )

        assert id1 is not None
        assert id2 is not None  # разные фингерпринты - дедуп не сработал
        assert r.xlen("stream:signals:outbox") == 2
