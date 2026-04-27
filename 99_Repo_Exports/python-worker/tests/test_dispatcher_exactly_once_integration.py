from utils.time_utils import get_ny_time_millis
"""
Integration tests for SignalDispatcher exactly-once delivery semantics.
Tests the critical invariants for delivery markers and idempotent operations.
"""
import json
import time
import pytest
from services.signal_dispatcher import SignalDispatcher


class TestDispatcherExactlyOnce:
    """Тесты exactly-once семантики dispatcher - маркеры доставки."""

    def test_dispatcher_idempotent_delivery_same_envelope(self, r):
        """Повторная обработка того же envelope должна быть идемпотентной."""
        # Создаем dispatcher (адаптируйте под вашу реализацию)
        dispatcher = SignalDispatcher(
            redis_client=r,
            outbox_stream="stream:signals:outbox",
            group="test-group",
            delivery_marker_ttl_sec=3600,
        )

        sid = "signal_123"
        env = {
            "sid": sid,
            "ts_ms": get_ny_time_millis(),
            "symbol": "BTCUSDT",
            "targets": {
                "notify": {"text": "test notify"},
                "stream": {"key": "signals:test:BTCUSDT"},
                "snap": {"key": f"signal:snap:{sid}"},
            },
        }

        msg_id = "1690000000000-0"  # условный outbox msg_id

        # Первая обработка
        ok1 = dispatcher._handle_one(msg_id, {"data": json.dumps(env, ensure_ascii=False)})
        assert ok1 is True

        # Проверяем что доставка произошла
        assert r.exists(f"deliver:notify:{sid}") == 1
        assert r.xlen("signals:test:BTCUSDT") == 1
        assert r.exists(f"signal:snap:{sid}") == 1

        # Вторая обработка того же envelope
        ok2 = dispatcher._handle_one(msg_id, {"data": json.dumps(env, ensure_ascii=False)})
        assert ok2 is True

        # Результат должен быть тем же (идемпотентность)
        assert r.exists(f"deliver:notify:{sid}") == 1  # маркер не дублируется
        assert r.xlen("signals:test:BTCUSDT") == 1  # только одно сообщение
        assert r.exists(f"signal:snap:{sid}") == 1

    def test_partial_failure_does_not_poison_delivery_markers(self, r, monkeypatch):
        """Partial failure одного target не должен портить маркеры других."""
        dispatcher = SignalDispatcher(
            redis_client=r,
            outbox_stream="stream:signals:outbox",
            group="test-group",
        )

        sid = "signal_fail_1"
        env = {
            "sid": sid,
            "symbol": "BTCUSDT",
            "targets": {
                "notify": {"text": "test notify"},
                "stream": {"key": "signals:test:BTCUSDT"},
            }
        }

        # Симулируем падение доставки в stream target
        orig_xadd = r.xadd
        def boom_xadd(name, *args, **kwargs):
            if name == "signals:test:BTCUSDT":
                raise Exception("simulated target stream error")
            return orig_xadd(name, *args, **kwargs)

        monkeypatch.setattr(r, "xadd", boom_xadd)

        # Обработка должна вернуть False (не готова к ACK)
        ok = dispatcher._handle_one("1-0", {"data": json.dumps(env, ensure_ascii=False)})
        assert ok is False  # не готово к ACK из-за ошибки

        # Критично: маркер доставки НЕ должен быть поставлен
        assert r.exists(f"deliver:stream:{sid}") == 0

        # Но notify мог доставиться (зависит от порядка)
        # assert r.exists(f"deliver:notify:{sid}") == 1  # если доставился

    def test_delivery_markers_expire(self, r):
        """Delivery маркеры должны истекать по TTL."""
        dispatcher = SignalDispatcher(
            redis_client=r,
            delivery_marker_ttl_sec=1,  # короткий TTL для теста
        )

        sid = "signal_ttl_1"
        env = {
            "sid": sid,
            "targets": {"notify": {"text": "test"}},
        }

        # Первая доставка
        dispatcher._deliver_all(env)
        assert r.exists(f"deliver:notify:{sid}") == 1

        # Ждем истечения TTL
        time.sleep(1.1)

        # Маркер должен истечь
        assert r.exists(f"deliver:notify:{sid}") == 0

        # Повторная доставка должна сработать
        dispatcher._deliver_all(env)
        assert r.exists(f"deliver:notify:{sid}") == 1

    def test_mark_if_new_atomicity(self, r):
        """_mark_if_new должен быть атомарным (SET NX)."""
        dispatcher = SignalDispatcher(redis_client=r)

        target = "test_target"
        sid = "signal_atomic_1"

        # Первый вызов должен поставить маркер
        marked1 = dispatcher._mark_if_new(target, sid)
        assert marked1 is True
        assert r.exists(f"deliver:{target}:{sid}") == 1

        # Второй вызов должен вернуть False
        marked2 = dispatcher._mark_if_new(target, sid)
        assert marked2 is False
        assert r.exists(f"deliver:{target}:{sid}") == 1  # маркер не изменился

    def test_different_targets_independent_markers(self, r):
        """Разные targets должны иметь независимые маркеры."""
        dispatcher = SignalDispatcher(redis_client=r)

        sid = "signal_multi_1"
        env = {
            "sid": sid,
            "targets": {
                "notify": {"text": "notify payload"},
                "stream": {"key": "signals:test:BTCUSDT"},
                "snap": {"key": f"signal:snap:{sid}"},
            }
        }

        # Доставляем все targets
        dispatcher._deliver_all(env)

        # Проверяем что все маркеры поставлены
        assert r.exists(f"deliver:notify:{sid}") == 1
        assert r.exists(f"deliver:stream:{sid}") == 1
        assert r.exists(f"deliver:snap:{sid}") == 1

        # Повторная доставка не должна добавить новых сообщений
        initial_notify_count = r.xlen("telegram:notify") if r.exists("telegram:notify") else 0
        initial_stream_count = r.xlen("signals:test:BTCUSDT")
        initial_snap_count = 1 if r.exists(f"signal:snap:{sid}") else 0

        dispatcher._deliver_all(env)

        # Считаем что доставка idempotentна (маркеры не дают дубликатов)
        final_stream_count = r.xlen("signals:test:BTCUSDT")
        assert final_stream_count == initial_stream_count

    def test_delivery_marker_key_format(self, r):
        """Формат ключей delivery маркеров должен быть consistent."""
        dispatcher = SignalDispatcher(redis_client=r)

        # Проверяем что _delivery_key генерирует ожидаемый формат
        key = dispatcher._delivery_key("notify", "signal_123")
        expected = "deliver:notify:signal_123"
        assert key == expected

        # И что маркер ставится под этим ключом
        dispatcher._mark_if_new("notify", "signal_123")
        assert r.exists("deliver:notify:signal_123") == 1
