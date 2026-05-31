from utils.time_utils import get_ny_time_millis

"""
Integration-тесты для проверки race condition между сервисами.

Симулируют реальный сценарий, когда scanner-trade-monitor и scanner-signal-tracker
одновременно получают один и тот же сигнал из Redis streams.

Без namespace изоляции: один сервис занимает SID, другой получает reject.
С namespace изоляцией: оба сервиса успешно обрабатывают сигнал независимо.
"""
import os
import threading
from unittest.mock import patch

import pytest

from services.trade_monitor import TradeMonitorService


class TestTradeMonitorRaceCondition:
    """
    Интеграционные тесты для race condition между сервисами.
    
    Сценарий из реального инцидента BTCUSDT 16:17 UTC:
    1. Сигнал с conf=78% публикуется в Redis stream
    2. scanner-signal-tracker и scanner-trade-monitor читают одновременно
    3. Оба пытаются занять SID claim в Redis
    4. БЕЗ namespace: один получает ключ, другой reject → пропуск сигнала
    5. С namespace: оба получают свои независимые ключи → оба обрабатывают сигнал
    """

    @pytest.fixture
    def real_redis(self):
        """
        Используем реальный Redis для интеграционных тестов.
        
        ВАЖНО: тесты изолированы через уникальные namespace и очистку после теста.
        """
        import redis
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/15")  # DB 15 для тестов
        client = redis.from_url(redis_url, decode_responses=True)

        # Проверяем доступность Redis
        try:
            client.ping()
        except Exception as e:
            pytest.skip(f"Redis недоступен для integration тестов: {e}")

        yield client

        # Очистка после теста
        # Удаляем все тестовые ключи
        for key in client.scan_iter("dedup:trade_monitor:test-*"):
            client.delete(key)

    def test_race_condition_without_namespace(self, real_redis):
        """
        Симулируем race condition БЕЗ namespace изоляции (старое поведение).
        
        Ожидаемый результат: второй сервис получает reject на claim.
        """
        signal_id = f"test-race-no-ns-{get_ny_time_millis()}"

        # Оба сервиса используют один namespace (имитация старого кода)
        with patch.dict(os.environ, {"TM_NAMESPACE": "default"}, clear=True):
            monitor1 = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            monitor2 = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )

            # Симулируем одновременный claim (race)
            claim1 = monitor1._sid_claim(signal_id, ttl_sec=5)
            claim2 = monitor2._sid_claim(signal_id, ttl_sec=5)

            # Первый сервис успешно занимает ключ
            assert claim1 is True

            # Второй сервис получает reject (ключ уже занят)
            assert claim2 is False  # ← ПРОБЛЕМА: пропуск сигнала!

        # Очистка
        real_redis.delete(monitor1._sid_dedup_key(signal_id))

    def test_race_condition_with_namespace(self, real_redis):
        """
        Симулируем race condition С namespace изоляцией (новое поведение).
        
        Ожидаемый результат: оба сервиса успешно обрабатывают сигнал.
        """
        signal_id = f"test-race-with-ns-{get_ny_time_millis()}"

        # Сервис 1: trade-monitor
        with patch.dict(os.environ, {"TM_NAMESPACE": "test-trade-monitor"}, clear=True):
            monitor_tm = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            claim_tm = monitor_tm._sid_claim(signal_id, ttl_sec=5)

        # Сервис 2: signal-tracker
        with patch.dict(os.environ, {"TM_NAMESPACE": "test-signal-tracker"}, clear=True):
            monitor_st = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            claim_st = monitor_st._sid_claim(signal_id, ttl_sec=5)

        # ✅ Оба сервиса успешно занимают свои ключи
        assert claim_tm is True
        assert claim_st is True

        # Проверяем, что в Redis созданы 2 разных ключа
        key_tm = monitor_tm._sid_dedup_key(signal_id)
        key_st = monitor_st._sid_dedup_key(signal_id)

        assert real_redis.exists(key_tm) == 1
        assert real_redis.exists(key_st) == 1
        assert key_tm != key_st

        # Очистка
        real_redis.delete(key_tm)
        real_redis.delete(key_st)

    def test_concurrent_claim_same_namespace(self, real_redis):
        """
        Проверяем, что в рамках одного namespace race condition корректно обрабатывается.
        
        Если два экземпляра одного сервиса пытаются занять один SID,
        только один должен получить claim (это нормальное поведение дедупликации).
        """
        signal_id = f"test-concurrent-same-ns-{get_ny_time_millis()}"

        # Создаем мониторы ДО запуска потоков (threading-safe)
        old_val = os.environ.get("TM_NAMESPACE")
        try:
            os.environ["TM_NAMESPACE"] = "test-same-ns"
            monitor1 = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            monitor2 = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
        finally:
            if old_val is None:
                os.environ.pop("TM_NAMESPACE", None)
            else:
                os.environ["TM_NAMESPACE"] = old_val

        results = []
        lock = threading.Lock()

        def try_claim(monitor):
            result = monitor._sid_claim(signal_id, ttl_sec=5)
            with lock:
                results.append(result)

        # Запускаем 2 потока с одинаковыми namespace
        threads = [
            threading.Thread(target=try_claim, args=(monitor1,)),
            threading.Thread(target=try_claim, args=(monitor2,)),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Только один поток должен получить True
        assert results.count(True) == 1, f"Expected 1 True claim, got: {results}"
        assert results.count(False) == 1

        # Очистка
        real_redis.delete(monitor1._sid_dedup_key(signal_id))

    def test_concurrent_claim_different_namespaces(self, real_redis):
        """
        Проверяем, что при разных namespace оба потока успешно занимают claim.
        
        Это ключевой тест для решения проблемы race condition между сервисами.
        """
        signal_id = f"test-concurrent-diff-ns-{get_ny_time_millis()}"

        # Создаем мониторы ДО запуска потоков (threading-safe)
        old_val = os.environ.get("TM_NAMESPACE")
        try:
            os.environ["TM_NAMESPACE"] = "test-ns-a"
            monitor_a = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )

            os.environ["TM_NAMESPACE"] = "test-ns-b"
            monitor_b = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
        finally:
            if old_val is None:
                os.environ.pop("TM_NAMESPACE", None)
            else:
                os.environ["TM_NAMESPACE"] = old_val

        results = []
        lock = threading.Lock()

        def try_claim(monitor):
            result = monitor._sid_claim(signal_id, ttl_sec=5)
            with lock:
                results.append(result)

        # Запускаем 2 потока с разными namespace
        threads = [
            threading.Thread(target=try_claim, args=(monitor_a,)),
            threading.Thread(target=try_claim, args=(monitor_b,)),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # ✅ Оба потока должны получить True
        assert results.count(True) == 2, f"Expected 2 True claims, got: {results}"
        assert results.count(False) == 0

        # Очистка
        real_redis.delete(monitor_a._sid_dedup_key(signal_id))
        real_redis.delete(monitor_b._sid_dedup_key(signal_id))

    def test_dedup_acquire_isolation(self, real_redis):
        """
        Проверяем изоляцию _dedup_acquire между namespace.
        """
        event_id = f"test-event-{get_ny_time_millis()}"
        kind = "tp_hit"

        # Сервис 1: trade-monitor
        with patch.dict(os.environ, {"TM_NAMESPACE": "test-tm"}, clear=True):
            monitor_tm = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            dedup1 = monitor_tm._dedup_acquire(kind, event_id)

        # Сервис 2: signal-tracker
        with patch.dict(os.environ, {"TM_NAMESPACE": "test-st"}, clear=True):
            monitor_st = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            dedup2 = monitor_st._dedup_acquire(kind, event_id)

        # Оба сервиса должны успешно обработать событие
        assert dedup1 is True
        assert dedup2 is True

        # Проверяем, что в Redis созданы 2 разных ключа
        key1 = monitor_tm._dedup_key(kind, event_id)
        key2 = monitor_st._dedup_key(kind, event_id)

        assert real_redis.exists(key1) == 1
        assert real_redis.exists(key2) == 1
        assert key1 != key2

        # Очистка
        real_redis.delete(key1)
        real_redis.delete(key2)

    def test_sid_finalize_isolation(self, real_redis):
        """
        Проверяем, что finalize для одного namespace не влияет на другой.
        """
        signal_id = f"test-finalize-{get_ny_time_millis()}"

        # Сервис 1: занимает claim и финализирует
        with patch.dict(os.environ, {"TM_NAMESPACE": "test-service-1"}, clear=True):
            monitor1 = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            claim1 = monitor1._sid_claim(signal_id, ttl_sec=5)
            assert claim1 is True
            monitor1._sid_finalize(signal_id, ttl_days=1)

            # После финализации claim должен превратиться в "done"
            key1 = monitor1._sid_dedup_key(signal_id)
            assert real_redis.get(key1) == "done"

        # Сервис 2: независимо занимает свой claim
        with patch.dict(os.environ, {"TM_NAMESPACE": "test-service-2"}, clear=True):
            monitor2 = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            claim2 = monitor2._sid_claim(signal_id, ttl_sec=5)
            # ✅ Сервис 2 не заблокирован финализацией сервиса 1
            assert claim2 is True

            key2 = monitor2._sid_dedup_key(signal_id)
            assert real_redis.get(key2) == "processing"

        # Очистка
        real_redis.delete(key1)
        real_redis.delete(key2)

    def test_btcusdt_incident_simulation(self, real_redis):
        """
        Симуляция реального инцидента BTCUSDT 16:17 UTC.
        
        Сценарий:
        1. Сигнал BTCUSDT conf=78% публикуется в Redis
        2. scanner-signal-tracker получает сигнал первым (на 10ms быстрее)
        3. scanner-trade-monitor получает тот же сигнал
        4. БЕЗ namespace: trade-monitor получает reject → позиция не открыта
        5. С namespace: оба обрабатывают независимо → позиция открыта
        """
        signal_id = "crypto-btcusdt-1737997029123-conf-78"

        # Симулируем быстрый scanner-signal-tracker
        with patch.dict(os.environ, {"TM_NAMESPACE": "signal-tracker"}, clear=True):
            tracker = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            tracker_claim = tracker._sid_claim(signal_id, ttl_sec=30)
            assert tracker_claim is True  # Трекер успешно занимает свой ключ

        # Симулируем scanner-trade-monitor (чуть медленнее)
        with patch.dict(os.environ, {"TM_NAMESPACE": "trade-monitor"}, clear=True):
            monitor = TradeMonitorService(
                redis_client=real_redis,
                config={},
                regime_guard=None,
                health_metrics=None
            )
            monitor_claim = monitor._sid_claim(signal_id, ttl_sec=30)

            # ✅ С namespace изоляцией: монитор также успешно занимает свой ключ
            assert monitor_claim is True

            # Проверяем, что в Redis два независимых ключа
            tracker_key = tracker._sid_dedup_key(signal_id)
            monitor_key = monitor._sid_dedup_key(signal_id)

            assert tracker_key != monitor_key
            assert real_redis.exists(tracker_key) == 1
            assert real_redis.exists(monitor_key) == 1

        # Очистка
        real_redis.delete(tracker_key)
        real_redis.delete(monitor_key)

    def test_high_load_race_condition(self, real_redis):
        """
        Стресс-тест: симулируем высокую нагрузку с множественными сигналами.
        
        Проверяем, что namespace изоляция работает корректно при высоком QPS.
        """
        base_time = get_ny_time_millis()
        num_signals = 50

        results_tm = []
        results_st = []
        lock_tm = threading.Lock()
        lock_st = threading.Lock()

        def process_signals_tm():
            old_val = os.environ.get("TM_NAMESPACE")
            try:
                os.environ["TM_NAMESPACE"] = "test-tm-load"
                monitor = TradeMonitorService(
                    redis_client=real_redis,
                    config={},
                    regime_guard=None,
                    health_metrics=None
                )
                for i in range(num_signals):
                    sid = f"load-test-signal-{base_time}-{i}"
                    result = monitor._sid_claim(sid, ttl_sec=5)
                    with lock_tm:
                        results_tm.append(result)
            finally:
                if old_val is None:
                    os.environ.pop("TM_NAMESPACE", None)
                else:
                    os.environ["TM_NAMESPACE"] = old_val

        def process_signals_st():
            old_val = os.environ.get("TM_NAMESPACE")
            try:
                os.environ["TM_NAMESPACE"] = "test-st-load"
                monitor = TradeMonitorService(
                    redis_client=real_redis,
                    config={},
                    regime_guard=None,
                    health_metrics=None
                )
                for i in range(num_signals):
                    sid = f"load-test-signal-{base_time}-{i}"
                    result = monitor._sid_claim(sid, ttl_sec=5)
                    with lock_st:
                        results_st.append(result)
            finally:
                if old_val is None:
                    os.environ.pop("TM_NAMESPACE", None)
                else:
                    os.environ["TM_NAMESPACE"] = old_val

        # Запускаем оба сервиса параллельно
        threads = [
            threading.Thread(target=process_signals_tm),
            threading.Thread(target=process_signals_st),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # ✅ Все claims для обоих сервисов должны быть успешны
        assert len(results_tm) == num_signals
        assert len(results_st) == num_signals
        assert all(results_tm), f"trade-monitor failed claims: {results_tm.count(False)}/{num_signals}"
        assert all(results_st), f"signal-tracker failed claims: {results_st.count(False)}/{num_signals}"

        # Очистка
        for i in range(num_signals):
            for ns in ["test-tm-load", "test-st-load"]:
                old_val = os.environ.get("TM_NAMESPACE")
                try:
                    os.environ["TM_NAMESPACE"] = ns
                    monitor = TradeMonitorService(
                        redis_client=real_redis,
                        config={},
                        regime_guard=None,
                        health_metrics=None
                    )
                    sid = f"load-test-signal-{base_time}-{i}"
                    real_redis.delete(monitor._sid_dedup_key(sid))
                finally:
                    if old_val is None:
                        os.environ.pop("TM_NAMESPACE", None)
                    else:
                        os.environ["TM_NAMESPACE"] = old_val


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

