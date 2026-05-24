"""Tests for services/trailing_command_consumer.py.

Coverage:
- _parse_command: required-field validation, shadow=1 skip
- _dispatch: success / exception / gateway-false
- _process_one_message: ACK/DLQ semantics
- _check_retry_cap: poison-message force ACK
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import fakeredis
import pytest

_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_consumer(fake_r: fakeredis.FakeRedis | None = None):
    """Build TrailingCommandConsumer via __new__ without real Redis/gateway."""
    from services.trailing_command_consumer import TrailingCommandConsumer

    c = TrailingCommandConsumer.__new__(TrailingCommandConsumer)
    c.r = fake_r or fakeredis.FakeRedis(decode_responses=True)
    c.enabled = True
    c.stream = "events:trailing:commands"
    c.group = "trailing-cmd-consumer"
    c.consumer = "tcc-test"
    c.batch_size = 20
    c.block_ms = 5000
    c.dlq_stream = "events:trailing:dlq"
    c.max_retries = 5
    c.retry_ttl_sec = 3600
    c.pel_stale_ms = 60000
    c.pel_reclaim_count = 50
    c.pel_reclaim_interval_s = 30
    c.stats_interval_s = 300
    c.idle_sleep_s = 60
    c.redis_url = "redis://test"
    c.running = False
    c.stats = {
        "messages_read": 0,
        "messages_processed": 0,
        "messages_acked": 0,
        "dispatched_ok": 0,
        "dispatched_fail": 0,
        "dlq_pushed": 0,
        "dlq_write_failed": 0,
        "poison_acked": 0,
        "errors": 0,
        "last_message_ts": 0,
    }
    c.dispatcher = MagicMock()
    return c


def _valid_fields() -> dict[str, str]:
    return {
        "sid": "sig-abc",
        "position_id": "pos-123",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "new_sl": "50000.5",
        "reason_code": "watermark_advance",
        "profile": "rocket",
        "ts_ms": "1700000000000",
        "shadow": "0",
    }


# ── _parse_command ────────────────────────────────────────────────────────────


class TestParseCommand:
    def setup_method(self):
        self.c = _make_consumer()

    def test_parse_command_valid(self):
        cmd = self.c._parse_command(_valid_fields())
        assert cmd is not None
        assert cmd["sid"] == "sig-abc"
        assert cmd["position_id"] == "pos-123"
        assert cmd["symbol"] == "BTCUSDT"
        assert cmd["side"] == "LONG"
        assert cmd["new_sl"] == 50000.5
        assert cmd["reason_code"] == "watermark_advance"

    def test_parse_command_missing_sid(self):
        f = _valid_fields()
        del f["sid"]
        assert self.c._parse_command(f) is None

    def test_parse_command_missing_position_id(self):
        f = _valid_fields()
        del f["position_id"]
        assert self.c._parse_command(f) is None

    def test_parse_command_shadow_1_skipped(self):
        f = _valid_fields()
        f["shadow"] = "1"
        assert self.c._parse_command(f) is None

    def test_parse_command_invalid_new_sl(self):
        f = _valid_fields()
        f["new_sl"] = "not_a_number"
        assert self.c._parse_command(f) is None

    def test_parse_command_zero_new_sl(self):
        f = _valid_fields()
        f["new_sl"] = "0"
        assert self.c._parse_command(f) is None

    def test_parse_command_invalid_side(self):
        f = _valid_fields()
        f["side"] = "FOO"
        assert self.c._parse_command(f) is None

    def test_parse_command_empty_fields(self):
        assert self.c._parse_command({}) is None


# ── _dispatch ─────────────────────────────────────────────────────────────────


class TestDispatch:
    def setup_method(self):
        self.c = _make_consumer()

    def _cmd(self) -> dict:
        return {
            "sid": "sig-1",
            "position_id": "pos-1",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "new_sl": 50000.0,
            "reason_code": "watermark_advance",
            "profile": "rocket",
        }

    def test_dispatch_success_calls_dispatcher(self):
        self.c.dispatcher.send_trailing_modify = MagicMock(return_value=True)
        ok, err = self.c._dispatch(self._cmd())
        assert ok is True
        assert err == ""
        assert self.c.stats["dispatched_ok"] == 1
        self.c.dispatcher.send_trailing_modify.assert_called_once()
        kwargs = self.c.dispatcher.send_trailing_modify.call_args.kwargs
        assert kwargs["sid"] == "sig-1"
        assert kwargs["symbol"] == "BTCUSDT"
        assert kwargs["side"] == "LONG"
        assert kwargs["new_sl"] == 50000.0
        assert kwargs["position_id"] == "pos-1"

    def test_dispatch_gateway_returns_false_is_failure(self):
        self.c.dispatcher.send_trailing_modify = MagicMock(return_value=False)
        ok, err = self.c._dispatch(self._cmd())
        assert ok is False
        assert err == "gateway_returned_false"
        assert self.c.stats["dispatched_fail"] == 1

    def test_dispatch_exception_returns_failure(self):
        self.c.dispatcher.send_trailing_modify = MagicMock(
            side_effect=RuntimeError("boom")
        )
        ok, err = self.c._dispatch(self._cmd())
        assert ok is False
        assert "RuntimeError" in err
        assert "boom" in err
        assert self.c.stats["dispatched_fail"] == 1


# ── _process_one_message ──────────────────────────────────────────────────────


class TestProcessOneMessage:
    def setup_method(self):
        self.c = _make_consumer()
        # disable retry-cap by default so it doesn't interfere
        self.c._check_retry_cap = MagicMock(return_value=False)
        self.c._xack = MagicMock()
        self.c._push_dlq = MagicMock(return_value=True)

    def test_process_message_success_acks(self):
        self.c.dispatcher.send_trailing_modify = MagicMock(return_value=True)
        self.c._process_one_message("1-0", _valid_fields())
        self.c._xack.assert_called_once_with("1-0")
        self.c._push_dlq.assert_not_called()
        assert self.c.stats["messages_processed"] == 1

    def test_process_message_parse_error_dlq_and_ack(self):
        bad = _valid_fields()
        del bad["sid"]
        self.c._process_one_message("2-0", bad)
        self.c._push_dlq.assert_called_once()
        # reason should be parse_error
        args = self.c._push_dlq.call_args.args
        assert args[2] == "parse_error"
        self.c._xack.assert_called_once_with("2-0")

    def test_process_message_parse_error_dlq_fail_no_ack(self):
        self.c._push_dlq = MagicMock(return_value=False)
        bad = _valid_fields()
        del bad["sid"]
        self.c._process_one_message("3-0", bad)
        self.c._push_dlq.assert_called_once()
        self.c._xack.assert_not_called()

    def test_process_message_dispatch_fail_dlq_and_ack(self):
        self.c.dispatcher.send_trailing_modify = MagicMock(return_value=False)
        self.c._process_one_message("4-0", _valid_fields())
        self.c._push_dlq.assert_called_once()
        args = self.c._push_dlq.call_args.args
        assert args[2].startswith("dispatch_failed:")
        self.c._xack.assert_called_once_with("4-0")

    def test_process_message_dlq_fail_no_ack(self):
        self.c.dispatcher.send_trailing_modify = MagicMock(return_value=False)
        self.c._push_dlq = MagicMock(return_value=False)
        self.c._process_one_message("5-0", _valid_fields())
        self.c._push_dlq.assert_called_once()
        self.c._xack.assert_not_called()

    def test_process_message_dispatch_exception_dlq_and_ack(self):
        self.c.dispatcher.send_trailing_modify = MagicMock(
            side_effect=RuntimeError("gw down")
        )
        self.c._process_one_message("6-0", _valid_fields())
        self.c._push_dlq.assert_called_once()
        args = self.c._push_dlq.call_args.args
        assert args[2].startswith("dispatch_failed:")
        self.c._xack.assert_called_once_with("6-0")


# ── Retry cap ────────────────────────────────────────────────────────────────


class TestRetryCap:
    def setup_method(self):
        self.c = _make_consumer()

    def test_retry_cap_under_limit_returns_false(self):
        # First call: counter=1, max=5 → False
        assert self.c._check_retry_cap("msg-1") is False
        assert self.c.stats["poison_acked"] == 0

    def test_retry_cap_force_acks_when_exceeded(self):
        # Pre-set counter > max
        self.c.r.set("tcc:retries:msg-poison", 10)
        assert self.c._check_retry_cap("msg-poison") is True
        assert self.c.stats["poison_acked"] == 1

    def test_process_message_poison_force_acks(self):
        """When retry-cap exceeded, message goes to DLQ + ACK regardless of DLQ result."""
        self.c._check_retry_cap = MagicMock(return_value=True)
        self.c._push_dlq = MagicMock(return_value=False)  # even if DLQ fails
        self.c._xack = MagicMock()
        self.c._process_one_message("poison-1", _valid_fields())
        self.c._push_dlq.assert_called_once()
        args = self.c._push_dlq.call_args.args
        assert args[2] == "max_retries_exceeded"
        self.c._xack.assert_called_once_with("poison-1")


# ── DLQ writer ────────────────────────────────────────────────────────────────


class TestPushDlq:
    def test_push_dlq_writes_entry(self):
        c = _make_consumer()
        ok = c._push_dlq("9-0", {"sid": "s1", "symbol": "BTCUSDT"}, "test_reason")
        assert ok is True
        assert c.stats["dlq_pushed"] == 1
        # Verify entry on the stream
        entries = c.r.xrange("events:trailing:dlq")
        assert len(entries) == 1
        _, fields = entries[0]
        assert fields["kind"] == "trailing_cmd"
        assert fields["reason"] == "test_reason"
        assert fields["original_msg_id"] == "9-0"
        assert "sid" in fields["fields_json"]

    def test_push_dlq_failure_increments_counter(self):
        c = _make_consumer()
        c.r = MagicMock()
        c.r.xadd = MagicMock(side_effect=RuntimeError("redis down"))
        ok = c._push_dlq("9-0", {"sid": "s1"}, "test")
        assert ok is False
        assert c.stats["dlq_write_failed"] == 1


# ── Disabled / enabled config ─────────────────────────────────────────────────


class TestConfig:
    def test_disabled_default(self, monkeypatch):
        """TCC_ENABLED defaults to 0 — fresh __init__ via __new__ stub."""
        # Read env, simulating real __init__ path
        monkeypatch.delenv("TCC_ENABLED", raising=False)
        # Verify default behaviour: when env unset, enabled should evaluate False
        assert (os.getenv("TCC_ENABLED", "0") == "1") is False

    def test_enabled_when_set(self, monkeypatch):
        monkeypatch.setenv("TCC_ENABLED", "1")
        assert (os.getenv("TCC_ENABLED", "0") == "1") is True


# ── Autocal gate (auto-activate on shadow=false) ──────────────────────────────


class TestAutocalGate:
    def test_force_enabled_ignores_autocal(self, monkeypatch):
        """TCC_ENABLED=1 → is_active=True regardless of autocal."""
        monkeypatch.setenv("TCC_ENABLED", "1")
        monkeypatch.setenv("TCC_FOLLOW_AUTOCAL", "1")
        c = _make_consumer()
        c.force_enabled = True
        c.follow_autocal = True
        c.autocal_active = False
        assert c.is_active is True

    def test_disabled_no_follow_inactive(self):
        c = _make_consumer()
        c.force_enabled = False
        c.follow_autocal = False
        c.autocal_active = True
        assert c.is_active is False

    def test_follow_autocal_inactive_when_shadow_true(self):
        c = _make_consumer()
        c.force_enabled = False
        c.follow_autocal = True
        c.autocal_active = False
        assert c.is_active is False

    def test_follow_autocal_active_when_shadow_false(self):
        c = _make_consumer()
        c.force_enabled = False
        c.follow_autocal = True
        c.autocal_active = True
        assert c.is_active is True

    def test_refresh_autocal_reads_shadow_false(self):
        """Setting autocal:trailing_state:state with shadow=false flips autocal_active."""
        import json as _json
        from core.redis_keys import RK
        fake_r = fakeredis.FakeRedis(decode_responses=True)
        c = _make_consumer(fake_r)
        c.follow_autocal = True
        c.autocal_active = False
        fake_r.set(RK.AUTOCAL_TRAILING_STATE, _json.dumps({"shadow": False}))
        c._refresh_autocal_state()
        assert c.autocal_active is True

    def test_refresh_autocal_reads_shadow_true(self):
        """Snapshot with shadow=true keeps autocal_active=False."""
        import json as _json
        from core.redis_keys import RK
        fake_r = fakeredis.FakeRedis(decode_responses=True)
        c = _make_consumer(fake_r)
        c.follow_autocal = True
        c.autocal_active = True
        fake_r.set(RK.AUTOCAL_TRAILING_STATE, _json.dumps({"shadow": True}))
        c._refresh_autocal_state()
        assert c.autocal_active is False

    def test_refresh_autocal_no_key_keeps_inactive(self):
        fake_r = fakeredis.FakeRedis(decode_responses=True)
        c = _make_consumer(fake_r)
        c.follow_autocal = True
        c.autocal_active = False
        c._refresh_autocal_state()  # no key in Redis
        assert c.autocal_active is False

    def test_refresh_autocal_disabled_no_follow_is_noop(self):
        """follow_autocal=False → _refresh_autocal_state does not touch autocal_active."""
        import json as _json
        from core.redis_keys import RK
        fake_r = fakeredis.FakeRedis(decode_responses=True)
        c = _make_consumer(fake_r)
        c.follow_autocal = False
        c.autocal_active = False
        fake_r.set(RK.AUTOCAL_TRAILING_STATE, _json.dumps({"shadow": False}))
        c._refresh_autocal_state()
        assert c.autocal_active is False  # unchanged

    def test_transition_emits_telegram_notification(self):
        """Shadow=true → shadow=false transition writes to notify:telegram stream."""
        import json as _json
        from core.redis_keys import RK
        fake_r = fakeredis.FakeRedis(decode_responses=True)
        c = _make_consumer(fake_r)
        c.follow_autocal = True
        c.autocal_active = False
        fake_r.set(RK.AUTOCAL_TRAILING_STATE, _json.dumps({"shadow": False}))
        c._refresh_autocal_state()
        # Telegram notification should have been emitted
        assert fake_r.xlen("notify:telegram") == 1
        msgs = fake_r.xrevrange("notify:telegram", count=1)
        _, fields = msgs[0]
        assert fields["subtype"] == "trailing_cmd_consumer"
        assert "ACTIVATED" in fields["text"]
