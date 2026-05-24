"""
Тесты для services/tp_event_listener.py.

Покрывает:
- TestTpHitNormalization: нормализация event_type=TP_HIT+tp_level → TPn_HIT
- TestListenerDlqAckBehaviour: поведение DLQ/XACK в _process_one_message
- TestPoisonCapBehaviour: poison-message guard (_check_poison_cap)
"""

import sys
import os
from pathlib import Path

# Гарантируем путь к python-worker (conftest делает это автоматически, но
# явный insert защищает от прямого запуска).
_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import json
import time
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

# ── Вспомогательная фабрика слушателя без реального Redis/gateway ─────────────

def _make_listener(fake_r: fakeredis.FakeRedis | None = None) -> "TPEventListener":
    """Создаёт TPEventListener через __new__ + ручная инициализация атрибутов
    (обход реального Redis-соединения и сигнала SIGINT/SIGTERM в __init__).
    """
    from services.tp_event_listener import TPEventListener

    listener = TPEventListener.__new__(TPEventListener)
    listener.r = fake_r or fakeredis.FakeRedis(decode_responses=True)
    listener.events_stream = "events:trades"
    listener.consumer_group = "tp1-trailing-group"
    listener.consumer_name = "test-consumer"
    listener.running = False
    listener.stats = {
        "messages_read": 0,
        "messages_processed": 0,
        "messages_acked": 0,
        "errors": 0,
        "last_message_ts": 0,
    }
    # Оркестратор — заглушка; конкретные тесты подменяют его
    listener.orchestrator = MagicMock()
    return listener


# ─────────────────────────────────────────────────────────────────────────────
# TestTpHitNormalization
# ─────────────────────────────────────────────────────────────────────────────

class TestTpHitNormalization:
    """_parse_event нормализует TP_HIT + tp_level → TPn_HIT."""

    def setup_method(self):
        self.listener = _make_listener()

    def test_tp_hit_with_level_1_normalized(self):
        """event_type=TP_HIT + tp_level=1 → TP1_HIT."""
        fields = {"event_type": "TP_HIT", "tp_level": "1", "sid": "sig-abc"}
        event = self.listener._parse_event(fields)
        assert event is not None
        assert event["event_type"] == "TP1_HIT"

    def test_tp_hit_with_level_2_normalized(self):
        """event_type=TP_HIT + tp_level=2 → TP2_HIT."""
        fields = {"event_type": "TP_HIT", "tp_level": "2", "sid": "sig-abc"}
        event = self.listener._parse_event(fields)
        assert event is not None
        assert event["event_type"] == "TP2_HIT"

    def test_tp1_hit_unchanged(self):
        """event_type=TP1_HIT уже нормализован — не превращается в TP11_HIT."""
        fields = {"event_type": "TP1_HIT", "sid": "sig-abc"}
        event = self.listener._parse_event(fields)
        assert event is not None
        assert event["event_type"] == "TP1_HIT"

    def test_tp_hit_without_level_stays_bare(self):
        """event_type=TP_HIT без tp_level (или tp_level=0) остаётся TP_HIT."""
        fields = {"event_type": "TP_HIT", "sid": "sig-abc"}
        event = self.listener._parse_event(fields)
        assert event is not None
        # tp_level отсутствует → lvl=0 → нет нормализации
        assert event["event_type"] == "TP_HIT"


# ─────────────────────────────────────────────────────────────────────────────
# TestListenerDlqAckBehaviour
# ─────────────────────────────────────────────────────────────────────────────

class TestListenerDlqAckBehaviour:
    """_process_one_message: правильное DLQ/XACK поведение при ошибках."""

    def setup_method(self):
        self.fake_r = fakeredis.FakeRedis(decode_responses=True)
        self.listener = _make_listener(self.fake_r)
        # _check_poison_cap возвращает False → не poison (счётчик < 5)
        self.listener._check_poison_cap = MagicMock(return_value=False)

    # ── parse error + DLQ ok → XACK ──────────────────────────────────────────

    def test_parse_error_dlq_ok_then_ack(self):
        """Пустые fields → _parse_event=None → DLQ=True → XACK вызван."""
        self.listener._parse_event = MagicMock(return_value=None)
        self.listener._push_listener_dlq = MagicMock(return_value=True)
        self.listener._xack = MagicMock()

        self.listener._process_one_message("1-0", {})

        self.listener._push_listener_dlq.assert_called_once()
        call_kwargs = self.listener._push_listener_dlq.call_args
        assert "parse_error" in call_kwargs[0]  # reason positional arg
        self.listener._xack.assert_called_once_with("1-0")

    # ── parse error + DLQ fail → NO XACK ─────────────────────────────────────

    def test_parse_error_dlq_fail_no_ack(self):
        """Parse fails → DLQ=False → XACK НЕ вызван (остаётся в PEL)."""
        self.listener._parse_event = MagicMock(return_value=None)
        self.listener._push_listener_dlq = MagicMock(return_value=False)
        self.listener._xack = MagicMock()

        self.listener._process_one_message("1-0", {})

        self.listener._push_listener_dlq.assert_called_once()
        self.listener._xack.assert_not_called()

    # ── orchestrator error + DLQ ok → XACK ───────────────────────────────────

    def test_orchestrator_error_dlq_and_ack(self):
        """orchestrator возвращает error → DLQ ok → XACK вызван."""
        from services.tp_hit_trailing_orchestrator import TrailingResult

        self.listener._parse_event = MagicMock(return_value={"event_type": "TP1_HIT", "sid": "s1"})
        self.listener.orchestrator.handle_event = MagicMock(
            return_value=TrailingResult(success=False, skipped=False, error="boom")
        )
        self.listener._push_listener_dlq = MagicMock(return_value=True)
        self.listener._xack = MagicMock()

        self.listener._process_one_message("2-0", {"event_type": "TP1_HIT", "sid": "s1"})

        self.listener._push_listener_dlq.assert_called_once()
        self.listener._xack.assert_called_once_with("2-0")

    # ── orchestrator success → NO DLQ → XACK ─────────────────────────────────

    def test_orchestrator_success_no_dlq(self):
        """orchestrator success → DLQ не вызывается → XACK вызван."""
        from services.tp_hit_trailing_orchestrator import TrailingResult

        self.listener._parse_event = MagicMock(return_value={"event_type": "TP1_HIT", "sid": "s1"})
        self.listener.orchestrator.handle_event = MagicMock(
            return_value=TrailingResult(success=True)
        )
        self.listener._push_listener_dlq = MagicMock(return_value=True)
        self.listener._xack = MagicMock()

        self.listener._process_one_message("3-0", {"event_type": "TP1_HIT", "sid": "s1"})

        self.listener._push_listener_dlq.assert_not_called()
        self.listener._xack.assert_called_once_with("3-0")


# ─────────────────────────────────────────────────────────────────────────────
# TestPoisonCapBehaviour
# ─────────────────────────────────────────────────────────────────────────────

class TestPoisonCapBehaviour:
    """_check_poison_cap: Redis-счётчик + force-ACK при превышении лимита."""

    def setup_method(self):
        self.fake_r = fakeredis.FakeRedis(decode_responses=True)
        self.listener = _make_listener(self.fake_r)
        # Сбрасываем _MAX_RETRIES к дефолту (5) на случай env override
        self.listener._MAX_RETRIES = 5

    def test_below_cap_no_force_ack(self):
        """Счётчик=4 < 5 → _check_poison_cap возвращает False (нормальный путь)."""
        msg_id = "test-msg-1"
        key = f"tp_listener:retries:{msg_id}"
        self.fake_r.set(key, "3")  # следующий incr даст 4

        result = self.listener._check_poison_cap(msg_id)
        assert result is False
        # Убедимся, что счётчик поднялся
        assert int(self.fake_r.get(key)) == 4

    def test_at_cap_force_ack_regardless(self):
        """Счётчик=6 > 5 → _check_poison_cap=True → force-ACK даже при DLQ fail."""
        msg_id = "test-msg-poison"
        key = f"tp_listener:retries:{msg_id}"
        self.fake_r.set(key, "5")  # следующий incr даст 6 > 5

        # Poison → метод вернёт True
        result = self.listener._check_poison_cap(msg_id)
        assert result is True

        # Проверяем, что _process_one_message force-ACK-ает даже при DLQ fail
        self.fake_r.set(key, "5")  # reset для _process_one_message
        self.listener._push_listener_dlq = MagicMock(return_value=False)  # DLQ fail
        self.listener._xack = MagicMock()
        # Восстановим реальный _check_poison_cap (счётчик снова =6)
        self.listener._check_poison_cap = self.listener.__class__._check_poison_cap.__get__(self.listener)

        self.listener._process_one_message(msg_id, {"event_type": "TP1_HIT"})

        # Force-ACK должен быть вызван несмотря на DLQ fail
        self.listener._xack.assert_called_once_with(msg_id)

    def test_cap_increments_counter(self):
        """Каждый вызов _check_poison_cap инкрементирует Redis-счётчик."""
        msg_id = "test-msg-inc"
        key = f"tp_listener:retries:{msg_id}"

        assert self.fake_r.get(key) is None

        self.listener._check_poison_cap(msg_id)
        assert int(self.fake_r.get(key)) == 1

        self.listener._check_poison_cap(msg_id)
        assert int(self.fake_r.get(key)) == 2

        self.listener._check_poison_cap(msg_id)
        assert int(self.fake_r.get(key)) == 3
