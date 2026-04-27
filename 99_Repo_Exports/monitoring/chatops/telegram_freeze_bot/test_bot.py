import json
import sys
from unittest.mock import patch, MagicMock
import pytest

sys.modules['tick_flow_full'] = MagicMock()
sys.modules['tick_flow_full.core'] = MagicMock()
sys.modules['tick_flow_full.core.promote_freeze'] = MagicMock()

from bot import _parse_freeze_cmd, _handle_cmd, _format_status, HELP, _is_admin, _chat_allowed

@pytest.fixture
def mock_redis():
    with patch("bot._redis") as mock:
        yield mock

@pytest.fixture
def mock_read_freeze():
    with patch("bot.read_freeze") as mock:
        mock.return_value = MagicMock(active=False, until_ts_ms=0, reason="", source="")
        yield mock

@pytest.fixture
def mock_set_freeze():
    with patch("bot.set_freeze") as mock:
        mock.return_value = True
        yield mock

@pytest.fixture
def mock_clear_freeze():
    with patch("bot.clear_freeze") as mock:
        mock.return_value = True
        yield mock


def test_parse_freeze_cmd():
    assert _parse_freeze_cmd("/freeze status") == ("status", [])
    assert _parse_freeze_cmd("freeze set 3600 manual") == ("set", ["3600", "manual"])
    assert _parse_freeze_cmd("/freeze clear") == ("clear", [])
    assert _parse_freeze_cmd("freeze") == ("help", [])
    assert _parse_freeze_cmd("/freeze") == ("help", [])
    assert _parse_freeze_cmd("hello") is None


def test_handle_cmd_help(mock_redis):
    ok, resp = _handle_cmd("help", [], {})
    assert ok is True
    assert HELP in resp


def test_handle_cmd_status(mock_redis, mock_read_freeze):
    ok, resp = _handle_cmd("status", [], {})
    assert ok is True
    data = json.loads(resp)
    assert not data["active"]


def test_handle_cmd_set(mock_redis, mock_set_freeze, mock_read_freeze):
    actor = {"actor": "12345", "username": "testuser"}
    ok, resp = _handle_cmd("set", ["3600", "manual", "investigation"], actor)
    assert ok is True
    data = json.loads(resp)
    assert data["ok"] is True
    assert data["duration_s"] == 3600
    mock_set_freeze.assert_called_once()
    
def test_handle_cmd_set_invalid(mock_redis):
    ok, resp = _handle_cmd("set", ["not_an_int", "reason"], {})
    assert ok is False
    assert "must be integer" in resp

    ok, resp = _handle_cmd("set", ["-10", "reason"], {})
    assert ok is False
    assert "must be > 0" in resp

    ok, resp = _handle_cmd("set", ["3600"], {})
    assert ok is False
    assert "Usage:" in resp


def test_handle_cmd_clear(mock_redis, mock_clear_freeze):
    actor = {"actor": "12345"}
    ok, resp = _handle_cmd("clear", [], actor)
    assert ok is True
    data = json.loads(resp)
    assert data["ok"] is True
    mock_clear_freeze.assert_called_once()


@patch("bot.ADMIN_USER_IDS", ["123", "456"])
def test_is_admin():
    assert _is_admin(123) is True
    assert _is_admin(456) is True
    assert _is_admin(789) is False
    assert _is_admin(None) is False


@patch("bot.ALLOWED_CHAT_ID", "-100500")
def test_chat_allowed():
    assert _chat_allowed(-100500) is True
    assert _chat_allowed(123) is False
    assert _chat_allowed(None) is False
