from unittest.mock import MagicMock, patch
import json
import argparse
import pytest
from orderflow_services.of_gate_dlq_drilldown_p83 import (
    cmd_stats,
    cmd_top,
    cmd_sample,
    cmd_replay,
    cmd_purge,
    _parse_dlq_msg,
    _extract_keys,
)

@pytest.fixture
def mock_redis():
    with patch("orderflow_services.of_gate_dlq_drilldown_p83._connect_redis") as m:
        mock = MagicMock()
        m.return_value = mock
        yield mock

def test_stats(mock_redis):
    args = argparse.Namespace(streams=["stream:dlq:of_gate_metrics"])
    mock_redis.xlen.return_value = 100
    mock_redis.xrevrange.return_value = [("1700000000000-0", {"stream": b"metrics:of_gate", "err": b"parse_error"})]
    assert cmd_stats(args) == 0
    mock_redis.xlen.assert_called_with("stream:dlq:of_gate_metrics")
    mock_redis.xrevrange.assert_called_with("stream:dlq:of_gate_metrics", max="+", min="-", count=1)

def test_top(mock_redis):
    args = argparse.Namespace(streams=["stream:dlq:of_gate_metrics"], limit=10)
    mock_redis.xrevrange.return_value = [
        ("1700000000000-0", {"stream": "metrics:of_gate", "err": "parse_error: msg", "payload": json.dumps({"dq_code": "ts_ms_missing"})}),
        ("1700000000001-0", {"stream": "metrics:of_gate", "err": "format_error: bad", "payload": json.dumps({"dq_code": "bad_format"})}),
    ]
    assert cmd_top(args) == 0
    mock_redis.xrevrange.assert_called_once()

def test_sample(mock_redis, capsys):
    args = argparse.Namespace(
        source="stream:dlq:of_gate_metrics", n=2, limit=10,
        dq_code="", reason_code="", err_prefix=""
    )
    mock_redis.xrevrange.return_value = [
        ("1700000000000-0", {"stream": "metrics", "err": "parse_error", "payload": "{}"}),
    ]
    assert cmd_sample(args) == 0
    captured = capsys.readouterr()
    assert "dlq_id" in captured.out

def test_replay_dry_run(mock_redis, capsys):
    args = argparse.Namespace(
        source="stream:dlq:of_gate_metrics", target="metrics:of_gate", max=10, dry_run=True,
        start_id="-", dq_code="", reason_code="", err_prefix="", no_meta=False, maxlen=100
    )
    mock_redis.xrange.return_value = [
        ("1700000000000-0", {"stream": "metrics:of_gate", "err": "parse_error", "payload": '{"test": 1}'})
    ]
    assert cmd_replay(args) == 0
    mock_redis.xadd.assert_not_called()
    captured = capsys.readouterr()
    assert "replay(dry)" in captured.out

def test_replay_commit(mock_redis, capsys):
    args = argparse.Namespace(
        source="stream:dlq:of_gate_metrics", target="metrics:of_gate", max=10, dry_run=False,
        start_id="-", dq_code="", reason_code="", err_prefix="", no_meta=False, maxlen=100
    )
    mock_redis.xrange.return_value = [
        ("1700000000000-0", {"stream": "metrics:of_gate", "err": "parse_error", "payload": '{"test": 1}'})
    ]
    assert cmd_replay(args) == 0
    mock_redis.xadd.assert_called_once()
    captured = capsys.readouterr()
    assert "replay" in captured.out

def test_purge_trim(mock_redis, capsys):
    args = argparse.Namespace(
        source="stream:dlq:of_gate_metrics", yes=True, ids="", maxlen=10
    )
    assert cmd_purge(args) == 0
    mock_redis.xtrim.assert_called_once_with("stream:dlq:of_gate_metrics", maxlen=10, approximate=True)

def test_purge_del(mock_redis, capsys):
    args = argparse.Namespace(
        source="stream:dlq:of_gate_metrics", yes=True, ids="170-0,171-0", maxlen=None
    )
    mock_redis.xdel.return_value = 2
    assert cmd_purge(args) == 0
    mock_redis.xdel.assert_called_once_with("stream:dlq:of_gate_metrics", "170-0", "171-0")

