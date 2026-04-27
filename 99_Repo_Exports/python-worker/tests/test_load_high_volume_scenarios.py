"""
Load tests for high-volume scenarios.
Tests that the system maintains exactly-once semantics under load.
"""
import json
import time
import pytest
from core.signal_outbox import SignalOutboxPublisher, OutboxSettings
from services.signal_dispatcher import SignalDispatcher


class TestLoadHighVolume:
    """Тесты производительности и корректности под нагрузкой."""

    def test_outbox_load_no_duplicates(self, r):
        """Load test: много сигналов в outbox, проверка отсутствия дубликатов."""
        settings = OutboxSettings(
            outbox_stream="stream:signals:outbox",
            dedup_bucket_ms=60000,
        )
        outbox = SignalOutboxPublisher(redis_client=r, settings=settings)

        n = 100  # уменьшенное количество для быстрого теста
        base_ts = 1700000000000

        published_ids = []
        for i in range(n):
            sid = "03d"
            env = {
                "sid": sid,
                "ts_ms": base_ts + i * 1000,  # разные timestamps
                "symbol": "BTCUSDT"
            }

            msg_id = outbox.publish(
                source="load_test", strategy="test", symbol="BTCUSDT",
                side="LONG", kind="ENTRY", level_key="",
                ts_ms=base_ts + i * 1000, envelope=env,
            )

            if msg_id:  # не dedup
                published_ids.append(msg_id)

        # Все должны быть опубликованы (разные timestamps = разные buckets)
        assert len(published_ids) == n
        assert r.xlen("stream:signals:outbox") == n

    def test_outbox_dedup_load_same_bucket(self, r):
        """Load test: дедуп в одном бакете под нагрузкой."""
        settings = OutboxSettings(
            outbox_stream="stream:signals:outbox",
            dedup_bucket_ms=60000,
        )
        outbox = SignalOutboxPublisher(redis_client=r, settings=settings)

        n = 50
        ts_ms = 1700000000000  # фиксированный timestamp = один bucket
        env = {"sid": "dedup_load", "ts_ms": ts_ms, "symbol": "BTCUSDT"}

        # Публикуем много раз тот же сигнал
        published_count = 0
        dedup_count = 0

        for i in range(n):
            msg_id = outbox.publish(
                source="load_test", strategy="test", symbol="BTCUSDT",
                side="LONG", kind="ENTRY", level_key="",
                ts_ms=ts_ms, envelope=env,
            )

            if msg_id:
                published_count += 1
            else:
                dedup_count += 1

        # Только первый должен опубликоваться, остальные dedup
        assert published_count == 1
        assert dedup_count == n - 1
        assert r.xlen("stream:signals:outbox") == 1

    def test_dispatcher_load_idempotent(self, r):
        """Load test: dispatcher должен оставаться идемпотентным под нагрузкой."""
        dispatcher = SignalDispatcher(
            redis_client=r,
            outbox_stream="stream:signals:outbox",
            group="load-test-group",
        )

        n = 20
        base_sid = "load_dispatch_"

        # Создаем и "обрабатываем" много сигналов
        for i in range(n):
            sid = "02d"
            env = {
                "sid": sid,
                "targets": {
                    "notify": {"text": f"load test {i}"},
                    "stream": {"key": "signals:load:BTCUSDT"},
                }
            }

            # Первая обработка
            ok1 = dispatcher._handle_one(f"{i}-0", {"data": json.dumps(env, ensure_ascii=False)})
            assert ok1 is True

            # Повторная обработка того же
            ok2 = dispatcher._handle_one(f"{i}-0", {"data": json.dumps(env, ensure_ascii=False)})
            assert ok2 is True

        # Проверяем отсутствие дубликатов
        stream_len = r.xlen("signals:load:BTCUSDT")
        assert stream_len == n  # по одному сообщению на сигнал

        # Проверяем маркеры доставки
        notify_markers = 0
        stream_markers = 0

        for i in range(n):
            sid = "02d"
            if r.exists(f"deliver:notify:{sid}"):
                notify_markers += 1
            if r.exists(f"deliver:stream:{sid}"):
                stream_markers += 1

        assert notify_markers == n
        assert stream_markers == n

    def test_end_to_end_load_pipeline(self, r):
        """End-to-end load test: outbox -> dispatcher -> targets."""
        # Настройка компонентов
        outbox_settings = OutboxSettings(
            outbox_stream="stream:signals:outbox",
            dedup_bucket_ms=60000,
        )
        outbox = SignalOutboxPublisher(redis_client=r, settings=outbox_settings)

        dispatcher = SignalDispatcher(
            redis_client=r,
            outbox_stream="stream:signals:outbox",
            group="e2e-test-group",
        )

        n = 30
        published_signals = 0
        dispatched_signals = 0

        # 1. Публикуем сигналы в outbox
        for i in range(n):
            sid = "02d"
            env = {
                "sid": sid,
                "ts_ms": 1700000000000 + i * 60000,  # разные минуты = разные buckets
                "symbol": "BTCUSDT",
                "targets": {
                    "stream": {"key": "signals:e2e:BTCUSDT"},
                    "snap": {"key": f"signal:snap:{sid}"},
                }
            }

            msg_id = outbox.publish(
                source="e2e_test", strategy="load", symbol="BTCUSDT",
                side="LONG", kind="ENTRY", level_key="",
                ts_ms=1700000000000 + i * 60000, envelope=env,
            )

            if msg_id:
                published_signals += 1

        assert published_signals == n
        assert r.xlen("stream:signals:outbox") == n

        # 2. "Обрабатываем" все сообщения из outbox
        messages = r.xrange("stream:signals:outbox")
        for msg_id, fields in messages:
            ok = dispatcher._handle_one(msg_id, fields)
            assert ok is True
            dispatched_signals += 1

        assert dispatched_signals == n

        # 3. Проверяем финальный результат
        final_stream_len = r.xlen("signals:e2e:BTCUSDT")
        assert final_stream_len == n

        # Проверяем что все snapshots созданы
        snap_count = 0
        for i in range(n):
            sid = "02d"
            if r.exists(f"signal:snap:{sid}"):
                snap_count += 1

        assert snap_count == n

    def test_concurrent_dedup_simulation(self, r):
        """Симуляция конкурентного доступа к дедуп ключам."""
        settings = OutboxSettings(
            outbox_stream="stream:signals:outbox",
            dedup_bucket_ms=60000,
        )

        # Создаем несколько outbox инстансов (симулируем конкурентных publisher'ов)
        outbox1 = SignalOutboxPublisher(redis_client=r, settings=settings)
        outbox2 = SignalOutboxPublisher(redis_client=r, settings=settings)
        outbox3 = SignalOutboxPublisher(redis_client=r, settings=settings)

        outboxes = [outbox1, outbox2, outbox3]
        ts_ms = 1700000000000
        env = {"sid": "concurrent_test", "ts_ms": ts_ms, "symbol": "BTCUSDT"}

        # Все пытаются опубликовать один и тот же сигнал
        results = []
        for i, outbox in enumerate(outboxes):
            msg_id = outbox.publish(
                source="concurrent_test", strategy="test", symbol="BTCUSDT",
                side="LONG", kind="ENTRY", level_key="",
                ts_ms=ts_ms, envelope=env,
            )
            results.append(msg_id)

        # Только один должен опубликоваться
        published_count = sum(1 for r in results if r is not None)
        dedup_count = sum(1 for r in results if r is None)

        assert published_count == 1
        assert dedup_count == 2
        assert r.xlen("stream:signals:outbox") == 1

    @pytest.mark.slow
    def test_load_heavy_volume(self, r, redis_url):
        """Тяжелый load test (отмечен как slow)."""

        # Этот тест можно включать отдельно для серьезного нагрузочного тестирования
        n = 1000

        import uuid
        run_id = uuid.uuid4().hex[:8]

        settings = OutboxSettings(outbox_stream=f"stream:signals:outbox:heavy_{run_id}")
        outbox = SignalOutboxPublisher(redis_url=redis_url, settings=settings)

        start_time = time.time()
        try:
            outbox._redis.flushdb()
        except Exception:
            pass

        # Публикуем 1000 уникальных сигналов и парные SRE метрики
        for i in range(n):
            sid = f"04d_{i}"
            env = {
                "sid": sid,
                "ts_ms": 1700000000000 + i,
                "symbol": "BTCUSDT",
                # Эмуляция "здоровых" данных
                "book_age_ms": 15,
                "tick_gap_ms": 50,
                "data_health": 1.0,
                "book_health_ok": 1,
                "source_consistency_ok": 1
            }

            msg_id = outbox.publish(
                source=f"heavy_load_{run_id}", strategy="test", symbol="BTCUSDT",
                side="LONG", kind="ENTRY", level_key=str(i),
                ts_ms=1700000000000 + i, envelope=env,
            )
            assert msg_id, f"Publish failed for i={i}, maybe deduped?"
            
            # Эмулируем P4 публикацию в SRE метрики (для bench tool)
            start_us = time.perf_counter_ns() // 1000
            time.sleep(0.0001)  # Искусственная задержка (100us)
            now_us = time.perf_counter_ns() // 1000
            lat_us = now_us - start_us
            
            r.xadd(
                f"metrics:of_gate:heavy_{run_id}",
                {
                    "type": "of_gate",
                    "ts_ms": str(1700000000000 + i),
                    "symbol": "BTCUSDT",
                    "latency_us": str(lat_us),
                    "exec_risk_norm": "0.1",
                    "ok": "1",
                    "ok_soft": "0",
                    "meta_veto": "0",
                    "data_health": "1.0",
                    "book_health_ok": "1",
                    "source_consistency_ok": "1"
                }
            )

        publish_time = time.time() - start_time

        # Проверяем что все опубликовано
        assert r.xlen(f"stream:signals:outbox:heavy_{run_id}") == n

        # Выводим метрики производительности
        print(f"Publish time: {publish_time:.2f}s")
        if publish_time > 0:
            print(f"Publish QPS: {n/publish_time:.1f}")

        # Проверяем отсутствие потерь
        dispatcher = SignalDispatcher()
        dispatcher.simple_redis = r
        dispatcher.redis = r
        dispatcher.outbox_stream = f"stream:signals:outbox:heavy_{run_id}"
        
        dispatched = 0

        messages = r.xrange(f"stream:signals:outbox:heavy_{run_id}")
        for msg_id, fields in messages:
            if dispatcher._handle_one(msg_id, fields):
                dispatched += 1

        assert dispatched == n

        dispatch_time = time.time() - start_time - publish_time
        print(".2f")
