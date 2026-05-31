"""Unit tests for telegram_task_bot/bot.py."""

import json
from unittest.mock import MagicMock, patch

import pytest

import sys
import os

# Add bot module path
sys.path.insert(0, os.path.dirname(__file__))

from bot import (
    _parse_command,
    cmd_task,
    cmd_tasks,
    cmd_done,
    cmd_clear,
    _chat_allowed,
    _is_admin,
    HELP_TEXT,
)


@pytest.fixture
def mock_redis():
    r = MagicMock()
    r.llen.return_value = 0
    r.lrange.return_value = []
    return r


class TestParseCommand:
    def test_task_command(self):
        assert _parse_command("/task fix the bug") == ("/task", "fix the bug")

    def test_tasks_no_args(self):
        assert _parse_command("/tasks") == ("/tasks", "")

    def test_done_with_id(self):
        assert _parse_command("/done abc123") == ("/done", "abc123")

    def test_help(self):
        assert _parse_command("/help") == ("/help", "")

    def test_start(self):
        assert _parse_command("/start") == ("/start", "")

    def test_non_command(self):
        assert _parse_command("hello world") is None

    def test_empty(self):
        assert _parse_command("") is None

    def test_none(self):
        assert _parse_command(None) is None

    def test_strip_bot_mention(self):
        assert _parse_command("/task@mybot fix bug") == ("/task", "fix bug")

    def test_clear(self):
        assert _parse_command("/clear") == ("/clear", "")


class TestCmdTask:
    @patch("bot._write_ops_event")
    def test_add_task(self, mock_ops, mock_redis):
        actor = {"actor": "123", "username": "alex", "name": "Alex"}
        resp = cmd_task("fix trailing stop", actor, mock_redis)
        assert "✅" in resp
        assert "queued" in resp
        mock_redis.rpush.assert_called_once()
        # Verify JSON payload
        pushed = mock_redis.rpush.call_args[0][1]
        data = json.loads(pushed)
        assert data["text"] == "fix trailing stop"
        assert data["from_user"] == "alex"
        assert data["status"] == "pending"
        assert len(data["id"]) == 6

    @patch("bot._write_ops_event")
    def test_empty_task(self, mock_ops, mock_redis):
        resp = cmd_task("", {}, mock_redis)
        assert "❌" in resp

    @patch("bot._write_ops_event")
    def test_queue_full(self, mock_ops, mock_redis):
        mock_redis.llen.return_value = 100
        resp = cmd_task("one more", {}, mock_redis)
        assert "full" in resp


class TestCmdTasks:
    def test_empty_queue(self, mock_redis):
        resp = cmd_tasks(mock_redis)
        assert "No pending" in resp

    def test_with_tasks(self, mock_redis):
        tasks = [
            json.dumps({"id": "abc123", "text": "fix bug", "from_user": "alex", "ts": 1710792600000}),
            json.dumps({"id": "def456", "text": "add metric", "from_user": "alex", "ts": 1710792700000}),
        ]
        mock_redis.lrange.return_value = tasks
        resp = cmd_tasks(mock_redis)
        assert "abc123" in resp
        assert "def456" in resp
        assert "fix bug" in resp
        assert "Total: 2" in resp


class TestCmdDone:
    @patch("bot._write_ops_event")
    def test_mark_done(self, mock_ops, mock_redis):
        task = json.dumps({"id": "abc123", "text": "fix bug", "ts": 1710792600000, "status": "pending"})
        mock_redis.lrange.return_value = [task]
        actor = {"actor": "123"}
        resp = cmd_done("abc123", actor, mock_redis)
        assert "✅" in resp
        assert "abc123" in resp
        mock_redis.lrem.assert_called_once()
        mock_redis.rpush.assert_called_once()

    @patch("bot._write_ops_event")
    def test_not_found(self, mock_ops, mock_redis):
        mock_redis.lrange.return_value = []
        resp = cmd_done("xyz", {}, mock_redis)
        assert "not found" in resp

    @patch("bot._write_ops_event")
    def test_hash_prefix(self, mock_ops, mock_redis):
        task = json.dumps({"id": "abc123", "text": "fix", "ts": 0, "status": "pending"})
        mock_redis.lrange.return_value = [task]
        resp = cmd_done("#abc123", {}, mock_redis)
        assert "✅" in resp


class TestCmdClear:
    @patch("bot._write_ops_event")
    def test_clear(self, mock_ops, mock_redis):
        mock_redis.llen.return_value = 3
        resp = cmd_clear({"actor": "123"}, mock_redis)
        assert "3" in resp
        mock_redis.delete.assert_called_once()

    @patch("bot._write_ops_event")
    def test_clear_empty(self, mock_ops, mock_redis):
        mock_redis.llen.return_value = 0
        resp = cmd_clear({}, mock_redis)
        assert "empty" in resp


class TestSecurity:
    @patch("bot.ALLOWED_CHAT_ID", "-100500")
    def test_chat_allowed(self):
        assert _chat_allowed(-100500) is True
        assert _chat_allowed(123) is False
        assert _chat_allowed(None) is False

    @patch("bot.ALLOWED_CHAT_ID", "")
    def test_chat_no_restriction(self):
        assert _chat_allowed(123) is True

    @patch("bot.ADMIN_USER_IDS", ["123", "456"])
    def test_is_admin(self):
        assert _is_admin(123) is True
        assert _is_admin(456) is True
        assert _is_admin(789) is False
        assert _is_admin(None) is False

    @patch("bot.ADMIN_USER_IDS", [])
    def test_no_admin_restriction(self):
        assert _is_admin(999) is True
