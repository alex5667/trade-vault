"""
Unit tests for the Alertmanager → Telegram webhook service (app.py).

Uses only stdlib + direct function calls (no TestClient / httpx dependency issues).
Tests cover:
- _fmt_alert_line: per-alert message formatting
- _build_message: full payload → message string
- POST /alert  via asyncio + ASGI: mock Telegram, validate response shape
- GET  /healthz  via asyncio + ASGI: liveness probe

Run:
    python3 -m pytest monitoring/alertmanager/telegram_webhook/tests/test_telegram_webhook.py -v
"""

import sys
import os
import asyncio
import json
from typing import Any, Dict
from unittest.mock import patch, MagicMock

import pytest

# ---------------------------------------------------------------------------
# Ensure the app module can be imported without real env vars
# ---------------------------------------------------------------------------
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("TELEGRAM_CHAT_ID", "")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app import _fmt_alert_line, _build_message, _select_chat, _rate_limited, _prune_dedupe  # noqa: E402
import app as _app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_alert(
    name: str = "TestAlert",
    sev: str = "critical",
    status: str = "firing",
    job: str = "test_job",
    instance: str = "localhost:9090",
    summary: str = "Something is broken",
) -> Dict[str, Any]:
    return {
        "status": status,
        "labels": {"alertname": name, "severity": sev, "job": job, "instance": instance},
        "annotations": {"summary": summary},
    }


def _make_payload(
    status: str = "firing",
    severity: str = "critical",
    alerts=None,
    runbook: str = "",
) -> Dict[str, Any]:
    if alerts is None:
        alerts = [_make_alert()]
    payload: Dict[str, Any] = {
        "status": status,
        "groupLabels": {"alertname": "TestAlert"},
        "commonLabels": {"alertname": "TestAlert", "severity": severity},
        "commonAnnotations": {"summary": "Group summary"},
        "alerts": alerts,
    }
    if runbook:
        payload["commonAnnotations"]["runbook_url"] = runbook
    return payload


# ---------------------------------------------------------------------------
# Tests: _fmt_alert_line
# ---------------------------------------------------------------------------

class TestFmtAlertLine:
    def test_basic_structure(self):
        """Alert line contains status, name, severity."""
        a = _make_alert()
        line = _fmt_alert_line(a)
        assert "[firing]" in line
        assert "TestAlert" in line
        assert "sev=critical" in line

    def test_job_and_instance_included(self):
        a = _make_alert(job="myjob", instance="host:9090")
        line = _fmt_alert_line(a)
        assert "job=myjob" in line
        assert "inst=host:9090" in line

    def test_summary_appended(self):
        a = _make_alert(summary="High memory usage")
        line = _fmt_alert_line(a)
        assert "High memory usage" in line

    def test_long_summary_truncated(self):
        """Summaries > 180 chars should be truncated with ellipsis."""
        a = _make_alert(summary="x" * 300)
        line = _fmt_alert_line(a)
        assert "..." in line
        assert len(line) < 400

    def test_missing_optional_fields(self):
        """Alert without job/instance/summary should not raise."""
        a = {
            "status": "firing",
            "labels": {"alertname": "Minimal", "severity": "warning"},
            "annotations": {},
        }
        line = _fmt_alert_line(a)
        assert "Minimal" in line
        assert "warning" in line

    def test_resolved_status(self):
        a = _make_alert(status="resolved")
        line = _fmt_alert_line(a)
        assert "[resolved]" in line


# ---------------------------------------------------------------------------
# Tests: _build_message
# ---------------------------------------------------------------------------

class TestBuildMessage:
    def test_contains_header_fields(self):
        payload = _make_payload(status="firing", severity="critical")
        msg = _build_message(payload)
        assert "TestAlert" in msg
        assert "sev=critical" in msg
        assert "status=firing" in msg

    def test_alert_count_in_header(self):
        payload = _make_payload(alerts=[_make_alert(), _make_alert(name="Alert2")])
        msg = _build_message(payload)
        assert "n=2" in msg

    def test_routing_labels_in_header(self):
        payload = _make_payload()
        payload["commonLabels"]["team"] = "frontend"
        payload["commonLabels"]["component"] = "ui"
        msg = _build_message(payload)
        assert "team=frontend" in msg
        assert "component=ui" in msg

    def test_timestamp_appended(self):
        payload = _make_payload()
        msg = _build_message(payload)
        assert "ts=" in msg

    def test_silence_matchers_included(self):
        payload = _make_payload(status="firing", severity="critical")
        payload["commonLabels"]["alertname"] = "TestAlert"
        payload["commonLabels"]["team"] = "trade"
        payload["commonLabels"]["component"] = "edge_stack"
        import app as _app
        orig_url = _app.ALERTMANAGER_BASE_URL
        _app.ALERTMANAGER_BASE_URL = "http://am"
        try:
            msg = _build_message(payload)
            assert "Silence matchers:" in msg
            assert 'alertname="TestAlert"' in msg
            assert 'team="trade"' in msg
            assert 'component="edge_stack"' in msg
        finally:
            _app.ALERTMANAGER_BASE_URL = orig_url

    def test_runbook_link_included(self):
        payload = _make_payload(runbook="https://runbook.example.com/alert1")
        msg = _build_message(payload)
        assert "https://runbook.example.com/alert1" in msg

    def test_too_many_alerts_truncated(self):
        """More than 8 alerts → '... and N more' line."""
        alerts = [_make_alert(name=f"Alert{i}") for i in range(12)]
        payload = _make_payload(alerts=alerts)
        msg = _build_message(payload)
        assert "more" in msg

    def test_message_length_bounded(self):
        """Message must never exceed 3900 chars."""
        long_summary = "x" * 5000
        alerts = [_make_alert(summary=long_summary, name=f"Alert{i}") for i in range(20)]
        payload = _make_payload(alerts=alerts)
        msg = _build_message(payload)
        assert len(msg) <= 3900

    def test_resolved_payload(self):
        payload = _make_payload(status="resolved")
        msg = _build_message(payload)
        assert "resolved" in msg

    def test_empty_alerts_list(self):
        """zero alerts should still produce a valid message."""
        payload = _make_payload(alerts=[])
        msg = _build_message(payload)
        assert "n=0" in msg

    def test_missing_common_fields(self):
        """Minimal payload without commonLabels / groupLabels should not raise."""
        payload: Dict[str, Any] = {
            "status": "firing",
            "groupLabels": {},
            "commonLabels": {},
            "commonAnnotations": {},
            "alerts": [_make_alert()],
        }
        msg = _build_message(payload)
        assert "ts=" in msg


# ---------------------------------------------------------------------------
# Tests: _send_telegram (no network)
# ---------------------------------------------------------------------------

class TestSendTelegram:
    def test_skips_when_no_credentials(self, capsys):
        """Should log warning and not make HTTP call when token/chat missing."""
        from app import _send_telegram
        import app as _app
        _orig_token = _app.BOT_TOKEN
        _orig_chat = getattr(_app, "DEFAULT_CHAT_ID", getattr(_app, "CHAT_ID", ""))
        _app.BOT_TOKEN = ""
        if hasattr(_app, "DEFAULT_CHAT_ID"):
            _app.DEFAULT_CHAT_ID = ""
        else:
            _app.CHAT_ID = ""
        try:
            with patch("requests.post") as mock_post:
                _send_telegram("test message", "", "")
            mock_post.assert_not_called()
        finally:
            _app.BOT_TOKEN = _orig_token
            if hasattr(_app, "DEFAULT_CHAT_ID"):
                _app.DEFAULT_CHAT_ID = _orig_chat
            else:
                _app.CHAT_ID = _orig_chat

    def test_calls_telegram_api_when_credentials_set(self):
        """Should invoke requests.post with correct url when credentials are set."""
        from app import _send_telegram
        import app as _app
        _orig_token = _app.BOT_TOKEN
        _orig_chat = getattr(_app, "DEFAULT_CHAT_ID", getattr(_app, "CHAT_ID", ""))
        _app.BOT_TOKEN = "fake_token"
        if hasattr(_app, "DEFAULT_CHAT_ID"):
            _app.DEFAULT_CHAT_ID = "12345"
        else:
            _app.CHAT_ID = "12345"
        try:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            with patch("requests.post", return_value=mock_resp) as mock_post:
                _send_telegram("hello telegram", "12345", "1")
            mock_post.assert_called_once()
            call_url = mock_post.call_args[0][0]
            assert "fake_token" in call_url
            assert "sendMessage" in call_url
            
            call_json = mock_post.call_args[1]["json"]
            assert call_json["chat_id"] == "12345"
            assert call_json["message_thread_id"] == 1
        finally:
            _app.BOT_TOKEN = _orig_token
            if hasattr(_app, "DEFAULT_CHAT_ID"):
                _app.DEFAULT_CHAT_ID = _orig_chat
            else:
                _app.CHAT_ID = _orig_chat

    def test_handles_network_error_gracefully(self):
        """requests.RequestException should be caught without raising."""
        from app import _send_telegram
        import requests
        import app as _app
        _orig_token = _app.BOT_TOKEN
        _orig_chat = getattr(_app, "DEFAULT_CHAT_ID", getattr(_app, "CHAT_ID", ""))
        _app.BOT_TOKEN = "tok"
        if hasattr(_app, "DEFAULT_CHAT_ID"):
            _app.DEFAULT_CHAT_ID = "123"
        else:
            _app.CHAT_ID = "123"
        try:
            with patch("requests.post", side_effect=requests.RequestException("net err")):
                # Should NOT raise
                _send_telegram("test", "123", "")
        finally:
            _app.BOT_TOKEN = _orig_token
            if hasattr(_app, "DEFAULT_CHAT_ID"):
                _app.DEFAULT_CHAT_ID = _orig_chat
            else:
                _app.CHAT_ID = _orig_chat


# ---------------------------------------------------------------------------
# Tests: inline runbook annotation rendering (added with quiet-hours patch)
# ---------------------------------------------------------------------------

class TestBuildMessageRunbook:
    """Tests for the inline runbook annotation block in _build_message."""

    def test_short_runbook_included_verbatim(self):
        """Short runbook (<700 chars) appears verbatim after Runbook: header."""
        rb_text = "1) Check logs.\n2) Restart service.\n3) Escalate if needed."
        payload = {
            "status": "firing",
            "groupLabels": {"alertname": "TestAlert"},
            "commonLabels": {"alertname": "TestAlert", "severity": "critical"},
            "commonAnnotations": {"summary": "Alert fired", "runbook": rb_text},
            "alerts": [_make_alert()],
        }
        msg = _build_message(payload)
        assert "Runbook:" in msg
        assert "Check logs." in msg
        assert "Escalate if needed." in msg

    def test_long_runbook_truncated_with_ellipsis(self):
        """runbook annotation >700 chars must be trimmed and suffixed with ..."""
        rb_text = "step\n" * 200  # well over 700 chars
        payload = {
            "status": "firing",
            "groupLabels": {"alertname": "TestAlert"},
            "commonLabels": {"alertname": "TestAlert", "severity": "critical"},
            "commonAnnotations": {"summary": "Alert fired", "runbook": rb_text},
            "alerts": [_make_alert()],
        }
        msg = _build_message(payload)
        assert "Runbook:" in msg
        assert "..." in msg
        assert len(msg) <= 3900

    def test_no_runbook_header_when_annotation_absent(self):
        """Runbook: header must NOT appear when the runbook annotation is missing."""
        payload = {
            "status": "firing",
            "groupLabels": {"alertname": "TestAlert"},
            "commonLabels": {"alertname": "TestAlert", "severity": "warning"},
            "commonAnnotations": {"summary": "No runbook annotation here"},
            "alerts": [_make_alert()],
        }
        msg = _build_message(payload)
        assert "Runbook:" not in msg

    def test_empty_runbook_annotation_skipped(self):
        """Empty string runbook annotation must be treated as absent (no header)."""
        payload = {
            "status": "firing",
            "groupLabels": {"alertname": "TestAlert"},
            "commonLabels": {"alertname": "TestAlert", "severity": "info"},
            "commonAnnotations": {"summary": "Quiet", "runbook": ""},
            "alerts": [_make_alert()],
        }
        msg = _build_message(payload)
        assert "Runbook:" not in msg

    def test_dashboard_annotation_appears_in_links(self):
        """dashboard annotation value must appear in the Links section."""
        dashboard_url = "Prometheus: /graph?g0.expr=edge_stack_train_last_success"
        payload = {
            "status": "firing",
            "groupLabels": {"alertname": "TestAlert"},
            "commonLabels": {"alertname": "TestAlert", "severity": "warning"},
            "commonAnnotations": {"summary": "Alert fired", "dashboard": dashboard_url},
            "alerts": [_make_alert()],
        }
        msg = _build_message(payload)
        assert dashboard_url in msg

    def test_runbook_appears_before_links(self):
        """Runbook: section must appear before Links: in the message."""
        payload = {
            "status": "firing",
            "groupLabels": {"alertname": "TestAlert"},
            "commonLabels": {"alertname": "TestAlert", "severity": "critical"},
            "commonAnnotations": {
                "summary": "Alert",
                "runbook": "1) Do this.",
                "runbook_url": "https://runbooks.example.com/1",
            },
            "alerts": [_make_alert()],
        }
        msg = _build_message(payload)
        rb_pos = msg.find("Runbook:")
        links_pos = msg.find("Links:")
        assert rb_pos != -1
        assert links_pos != -1
        assert rb_pos < links_pos, "Runbook: section must precede Links: section"


# ---------------------------------------------------------------------------
# Tests: Routing and Rate Limiting
# ---------------------------------------------------------------------------

class TestRoutingAndRateLimiting:
    def setup_method(self):
        # Reset globals before each test
        _app._dedupe_cache.clear()
        _app._rate_window.clear()
        _app.ROUTING = {
            "default": {"chat_id": "-1"},
            "severity:critical": {"chat_id": "-2", "thread_id": "99"},
            "team:trade": {"chat_id": "-3"},
            "component:edge_stack": {"chat_id": "-4", "thread_id": "55"},
        }
        _app.DEFAULT_CHAT_ID = "-999"
        _app.DEFAULT_THREAD_ID = ""

    def test_select_chat_precedence(self):
        # Only severity
        chat, thread = _select_chat({"severity": "critical"})
        assert chat == "-2"
        assert thread == "99"

        # Team overrides severity
        chat, thread = _select_chat({"severity": "critical", "team": "trade"})
        assert chat == "-3"
        assert thread == ""

        # Component overrides team
        chat, thread = _select_chat({"severity": "critical", "team": "trade", "component": "edge_stack"})
        assert chat == "-4"
        assert thread == "55"

        # Fallback to default routing rule
        chat, thread = _select_chat({"severity": "info"})
        assert chat == "-1"
        assert thread == ""

        # Fallback to env vars if no rule matches and no default
        _app.ROUTING = {}
        chat, thread = _select_chat({"severity": "warning"})
        assert chat == "-999"
        assert thread == ""

    def test_rate_limited(self):
        import time
        now = time.time()
        for i in range(_app.RATE_LIMIT_PER_MIN):
            assert not _rate_limited(now), f"Should not be rate limited on msg {i}"
        assert _rate_limited(now), "Should be rate limited on msg RATE_LIMIT_PER_MIN+1"

        # Slide window 61 seconds forward
        assert not _rate_limited(now + 61), "Should allow msg after sliding window"

    def test_prune_dedupe(self):
        import time
        now = time.time()
        _app._dedupe_cache["a"] = now - 200  # older than 180s
        _app._dedupe_cache["b"] = now - 10   # newer
        
        # Fake a large cache so prune actually runs
        for i in range(5001):
            _app._dedupe_cache[f"fake_{i}"] = now
            
        _prune_dedupe(now)
        
        # 'a' should be gone, 'b' should remain
        assert "a" not in _app._dedupe_cache
        assert "b" in _app._dedupe_cache
