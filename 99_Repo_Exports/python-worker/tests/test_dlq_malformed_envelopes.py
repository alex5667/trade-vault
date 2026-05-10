from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""
Tests for DLQ (Dead Letter Queue) handling of malformed envelopes.
Tests that bad envelopes are properly quarantined without breaking the pipeline.
"""
import json

from services.dispatch.dispatcher_app import SignalDispatcher


class TestDLQMalformedEnvelopes:
    """Тесты обработки malformed envelopes - должны идти в DLQ."""

    def test_missing_sid_goes_to_dlq(self, r):
        """Envelope без sid должен пойти в DLQ и быть ACK."""
        dispatcher = SignalDispatcher(
            redis_client=r,
            outbox_stream=RS.SIGNAL_OUTBOX,
            group="test-group",
            dlq_stream=RS.SIGNAL_DLQ,
        )

        # Malformed envelope без sid
        bad_env = {
            "ts_ms": get_ny_time_millis(),
            "symbol": "BTCUSDT"
        }

        msg_id = "bad_msg_1"

        # Обработка должна вернуть True (ACK ok) и отправить в DLQ
        ok = dispatcher._handle_one(msg_id, {"data": json.dumps(bad_env, ensure_ascii=False)})
        assert ok is True

        # Проверяем что в DLQ есть сообщение
        dlq_len = r.xlen(RS.SIGNAL_DLQ)
        assert dlq_len >= 1

        # Проверяем содержание DLQ сообщения
        dlq_messages = r.xrange(RS.SIGNAL_DLQ)
        found = False
        for msg_id_dlq, fields in dlq_messages:
            if "data" in fields:
                dlq_data = json.loads(fields["data"])
                if dlq_data.get("reason") == "missing_sid":
                    found = True
                    assert "original_envelope" in dlq_data
                    break
        assert found, "DLQ должно содержать сообщение с reason='missing_sid'"

    def test_empty_envelope_goes_to_dlq(self, r):
        """Пустой envelope должен пойти в DLQ."""
        dispatcher = SignalDispatcher(
            redis_client=r,
            outbox_stream=RS.SIGNAL_OUTBOX,
            group="test-group",
            dlq_stream=RS.SIGNAL_DLQ,
        )

        empty_env = {}
        msg_id = "empty_msg_1"

        ok = dispatcher._handle_one(msg_id, {"data": json.dumps(empty_env, ensure_ascii=False)})
        assert ok is True

        # DLQ должен содержать сообщение
        dlq_len = r.xlen(RS.SIGNAL_DLQ)
        assert dlq_len >= 1

    def test_invalid_json_goes_to_dlq(self, r):
        """Invalid JSON в data поле должен пойти в DLQ."""
        dispatcher = SignalDispatcher(
            redis_client=r,
            outbox_stream=RS.SIGNAL_OUTBOX,
            group="test-group",
            dlq_stream=RS.SIGNAL_DLQ,
        )

        # Invalid JSON - незакрытая скобка
        invalid_json = '{"sid": "test", "ts_ms": 123456'

        msg_id = "invalid_json_1"

        # _parse_envelope должен вернуть None для invalid JSON
        env = dispatcher._parse_envelope({"data": invalid_json})
        assert env is None

        ok = dispatcher._handle_one(msg_id, {"data": invalid_json})
        assert ok is True  # ACK ok, отправлено в DLQ

        dlq_len = r.xlen(RS.SIGNAL_DLQ)
        assert dlq_len >= 1

    def test_missing_data_field_goes_to_dlq(self, r):
        """Отсутствие data поля должно пойти в DLQ."""
        dispatcher = SignalDispatcher(
            redis_client=r,
            outbox_stream=RS.SIGNAL_OUTBOX,
            group="test-group",
            dlq_stream=RS.SIGNAL_DLQ,
        )

        # Поля без data
        fields_without_data = {"some_field": "value"}

        msg_id = "no_data_1"

        env = dispatcher._parse_envelope(fields_without_data)
        assert env is None

        ok = dispatcher._handle_one(msg_id, fields_without_data)
        assert ok is True

        dlq_len = r.xlen(RS.SIGNAL_DLQ)
        assert dlq_len >= 1

    def test_max_attempts_exceeded_goes_to_dlq(self, r, monkeypatch):
        """После max_attempts неудачных попыток - DLQ."""
        dispatcher = SignalDispatcher(
            redis_client=r,
            outbox_stream=RS.SIGNAL_OUTBOX,
            group="test-group",
            dlq_stream=RS.SIGNAL_DLQ,
            max_attempts=2,  # маленький лимит для теста
        )

        sid = "fail_signal_1"
        env = {
            "sid": sid,
            "targets": {"notify": {"text": "test"}},
        }

        # Симулируем постоянную ошибку доставки
        def always_fail(*args, **kwargs):
            raise Exception("simulated delivery failure")

        monkeypatch.setattr(dispatcher, "_deliver_all", always_fail)

        msg_id = "fail_msg_1"

        # Первая попытка - должна re-enqueue
        ok1 = dispatcher._handle_one(msg_id, {"data": json.dumps(env, ensure_ascii=False)})
        assert ok1 is True  # re-enqueued

        # Должно появиться новое сообщение в outbox с attempt=1
        outbox_len = r.xlen(RS.SIGNAL_OUTBOX)
        assert outbox_len >= 1

        # Вторая попытка - должна отправить в DLQ
        # (нужно найти новое сообщение и обработать его)
        messages = r.xrange(RS.SIGNAL_OUTBOX)
        for re_msg_id, re_fields in messages:
            if "data" in re_fields:
                re_env = json.loads(re_fields["data"])
                if re_env.get("sid") == sid and re_env.get("attempt") == 1:
                    ok2 = dispatcher._handle_one(re_msg_id, re_fields)
                    assert ok2 is True  # DLQ
                    break

        # DLQ должно содержать сообщение
        dlq_len = r.xlen(RS.SIGNAL_DLQ)
        assert dlq_len >= 1

    def test_dlq_contains_original_message_and_reason(self, r):
        """DLQ сообщения должны содержать оригинал и причину."""
        dispatcher = SignalDispatcher(
            redis_client=r,
            dlq_stream=RS.SIGNAL_DLQ,
        )

        original_fields = {"data": '{"symbol": "BTCUSDT"}'}  # без sid
        reason = "test_reason"

        msg_id = "test_dlq_1"
        dispatcher._send_dlq(msg_id, original_fields, reason=reason)

        # Проверяем DLQ
        dlq_messages = r.xrange(RS.SIGNAL_DLQ)
        found = False
        for dlq_id, dlq_fields in dlq_messages:
            if "data" in dlq_fields:
                dlq_data = json.loads(dlq_fields["data"])
                if dlq_data.get("reason") == reason:
                    found = True
                    assert "original_msg_id" in dlq_data
                    assert "original_fields" in dlq_data
                    assert dlq_data["original_msg_id"] == msg_id
                    assert dlq_data["original_fields"] == original_fields
                    break

        assert found, f"DLQ должно содержать сообщение с reason='{reason}'"

    def test_ack_after_dlq(self, r):
        """После отправки в DLQ оригинальное сообщение должно быть ACK."""
        dispatcher = SignalDispatcher(
            redis_client=r,
            outbox_stream=RS.SIGNAL_OUTBOX,
            group="test-group",
            dlq_stream=RS.SIGNAL_DLQ,
        )

        # Добавим тестовое сообщение в outbox
        msg_id = r.xadd(RS.SIGNAL_OUTBOX, {"data": '{"symbol": "BTCUSDT"}'})

        # Обработаем его (должно пойти в DLQ)
        ok = dispatcher._handle_one(msg_id, {"data": '{"symbol": "BTCUSDT"}'})
        assert ok is True

        # Проверяем что сообщение ACK в consumer group
        # (это сложно проверить напрямую без mock, но можем проверить что оно не в pending)
        pending = r.xpending(RS.SIGNAL_OUTBOX, "test-group")
        msg_ids = [p["message_id"] for p in pending]
        assert msg_id not in msg_ids  # должно быть ACK
