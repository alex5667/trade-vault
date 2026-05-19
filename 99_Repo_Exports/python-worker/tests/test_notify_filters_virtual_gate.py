"""Tests for core.notify_filters.should_skip_telegram_virtual.

Pins CRYPTO_NOTIFY_SKIP_VIRTUAL=1 default + dual-key (virtual / is_virtual)
read so external producers that inject only is_virtual=1 still get blocked
from Telegram. Regression for v14_of canary 2026-05-18 — legacy OFConfirm
shadowed signals were leaking to operator chat with validation_status="passed".
"""
from __future__ import annotations

from core.notify_filters import should_skip_telegram_virtual


def test_blocks_when_virtual_bool_true_default_env():
    assert should_skip_telegram_virtual({"virtual": True}, env_value=None) is True


def test_blocks_when_is_virtual_int_1_default_env():
    # External producers (binance_iceberg_detector, outbox replay) inject
    # is_virtual=1 only — must still be blocked.
    assert should_skip_telegram_virtual({"is_virtual": 1}, env_value=None) is True


def test_blocks_when_is_virtual_str_1():
    # Wire-level dicts can carry strings after JSON roundtrip.
    assert should_skip_telegram_virtual({"is_virtual": "1"}, env_value=None) is True


def test_passes_when_not_virtual_default_env():
    assert should_skip_telegram_virtual({"virtual": False, "is_virtual": 0}) is False


def test_passes_when_no_virtual_keys_default_env():
    assert should_skip_telegram_virtual({}, env_value=None) is False


def test_env_override_zero_restores_legacy_pass_through():
    # Operator escape hatch: CRYPTO_NOTIFY_SKIP_VIRTUAL=0 → legacy behaviour.
    assert should_skip_telegram_virtual({"virtual": True}, env_value="0") is False
    assert should_skip_telegram_virtual({"is_virtual": 1}, env_value="0") is False


def test_env_truthy_variants():
    for v in ("1", "true", "TRUE", "yes", "on", "True"):
        assert should_skip_telegram_virtual({"is_virtual": 1}, env_value=v) is True, v


def test_env_falsy_variants():
    for v in ("0", "false", "no", "off", "", "  "):
        assert should_skip_telegram_virtual({"is_virtual": 1}, env_value=v) is False, v


def test_garbage_is_virtual_value_does_not_raise():
    # Malformed payloads must not crash the publish path.
    assert should_skip_telegram_virtual({"is_virtual": "garbage"}, env_value=None) is False
    assert should_skip_telegram_virtual({"is_virtual": None}, env_value=None) is False
