"""P0.3 — Telegram notification contract: text field + legacy message mapping."""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from common.contracts.registry import TelegramNotificationV1
from pydantic import ValidationError


def test_text_field_accepted():
    n = TelegramNotificationV1(ts_ms=1000, text="hello")
    assert n.text == "hello"


def test_legacy_message_mapped_to_text():
    """Publisher sends message=...; validator must accept and expose as text."""
    n = TelegramNotificationV1(ts_ms=1000, message="legacy msg")
    assert n.text == "legacy msg"
    assert n.message == "legacy msg"


def test_text_takes_priority_over_message():
    n = TelegramNotificationV1(ts_ms=1000, text="canonical", message="legacy")
    assert n.text == "canonical"


def test_missing_both_text_and_message_raises():
    with pytest.raises(ValidationError):
        TelegramNotificationV1(ts_ms=1000)


def test_full_payload():
    n = TelegramNotificationV1(
        ts_ms=1714000000000,
        level="INFO",
        topic="trade_signal",
        text="BTCUSDT LONG entry 99000",
        meta={"signal_id": "abc123"},
    )
    assert n.level == "INFO"
    assert n.meta["signal_id"] == "abc123"
