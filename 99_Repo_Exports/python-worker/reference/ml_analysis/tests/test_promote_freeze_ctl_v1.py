import json
import argparse
from unittest.mock import patch, MagicMock
import pytest

from ml_analysis.tools.promote_freeze_ctl import cmd_status, cmd_set, cmd_clear, build_parser

class MockFreezeState:
    def __init__(self, active, until_ts_ms, reason, source):
        self.active = active
        self.until_ts_ms = until_ts_ms
        self.reason = reason
        self.source = source

@patch("ml_analysis.tools.promote_freeze_ctl.read_freeze")
def test_cmd_status(mock_read, capsys):
    mock_read.return_value = MockFreezeState(True, 123456789, "test freeze", "manual")
    args = argparse.Namespace(redis_url="redis://localhost:6379/0")
    
    assert cmd_status(args) == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["active"] is True
    assert data["until_ts_ms"] == 123456789
    assert data["reason"] == "test freeze"

@patch("ml_analysis.tools.promote_freeze_ctl.set_freeze")
@patch("ml_analysis.tools.promote_freeze_ctl._write_ops_event")
def test_cmd_set(mock_write_ops, mock_set, capsys):
    mock_set.return_value = True
    args = argparse.Namespace(
        redis_url="redis://localhost:6379/0"
        duration_s=3600
        reason="manual investigation"
        source="manual"
        actor="ops"
    )
    
    assert cmd_set(args) == 0
    mock_set.assert_called_once_with(
        "redis://localhost:6379/0", 
        duration_s=3600, 
        reason="manual investigation", 
        source="manual", 
        extra={"actor": "ops"}
    )
    mock_write_ops.assert_called_once()
    
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["ok"] is True

@patch("ml_analysis.tools.promote_freeze_ctl.set_freeze")
@patch("ml_analysis.tools.promote_freeze_ctl._write_ops_event")
def test_cmd_set_fail(mock_write_ops, mock_set, capsys):
    mock_set.return_value = False
    args = argparse.Namespace(
        redis_url="redis://localhost:6379/0"
        duration_s=3600
        reason="manual investigation"
        source="manual"
        actor="ops"
    )
    
    assert cmd_set(args) == 2
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["ok"] is False

@patch("ml_analysis.tools.promote_freeze_ctl.clear_freeze")
@patch("ml_analysis.tools.promote_freeze_ctl._write_ops_event")
def test_cmd_clear(mock_write_ops, mock_clear, capsys):
    mock_clear.return_value = True
    args = argparse.Namespace(
        redis_url="redis://localhost:6379/0"
        actor="ops"
    )
    
    assert cmd_clear(args) == 0
    mock_clear.assert_called_once_with("redis://localhost:6379/0")
    mock_write_ops.assert_called_once()
    
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["ok"] is True

def test_build_parser():
    parser = build_parser()
    
    args = parser.parse_args(["status"])
    assert args.func == cmd_status
    
    args = parser.parse_args(["set", "--reason", "test"])
    assert args.func == cmd_set
    assert args.reason == "test"
    
    args = parser.parse_args(["clear"])
    assert args.func == cmd_clear
