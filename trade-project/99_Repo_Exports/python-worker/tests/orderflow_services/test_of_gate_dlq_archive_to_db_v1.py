import argparse
from unittest.mock import MagicMock, patch

import pytest

from orderflow_services.of_gate_dlq_archive_to_db_v1 import _checkpoint_key, parse_dlq_fields, run_once


@pytest.fixture
def mock_redis():
    with patch("orderflow_services.of_gate_dlq_archive_to_db_v1._connect_redis") as m:
        mock = MagicMock()
        m.return_value = mock
        yield mock

@pytest.fixture
def mock_pg():
    with patch("orderflow_services.of_gate_dlq_archive_to_db_v1.PgWriter") as m:
        mock = MagicMock()
        m.return_value = mock
        yield mock

def test_parse_dlq_fields():
    dlq_id = "1700000000000-0"
    fields = {"stream": b"test_stream", "err": b"test_err", "payload": b'{"dq_code": "code1", "reason_code": "code2"}'}
    parsed = parse_dlq_fields(dlq_id, fields)
    assert parsed[0] == "test_stream"
    assert parsed[2] == "test_err"
    assert parsed[3] == "code1"
    assert parsed[4] == "code2"

@patch("orderflow_services.of_gate_dlq_archive_to_db_v1.pick_dsn", return_value="pg://")
def test_run_once(mock_pick_dsn, mock_redis, mock_pg, capsys):
    args = argparse.Namespace(
        streams=["stream:test1"], auto_migrate=True, no_checkpoint=False, tail=0,
        batch=10, delete_after=False, yes=False
    )
    mock_redis.get.return_value = b"1600000000000-0"
    mock_redis.xrange.return_value = [
        (b"1700000000000-0", {b"stream": b"test", b"payload": b"{}"})
    ]

    mock_pg.insert_rows.return_value = 1

    res = run_once(args)
    assert res == 0
    mock_pg.ensure_tables.assert_called_once()
    mock_redis.get.assert_called_with(_checkpoint_key("stream:test1"))
    mock_pg.insert_rows.assert_called_once()
    mock_redis.set.assert_called_with(_checkpoint_key("stream:test1"), "1700000000000-0")

@patch("orderflow_services.of_gate_dlq_archive_to_db_v1.pick_dsn", return_value="pg://")
def test_run_once_tail(mock_pick_dsn, mock_redis, mock_pg, capsys):
    args = argparse.Namespace(
        streams=["stream:test1"], auto_migrate=False, no_checkpoint=False, tail=10,
        batch=10, delete_after=False, yes=False
    )
    mock_redis.xrevrange.return_value = [
        (b"1700000000000-0", {b"stream": b"test", b"payload": b"{}"})
    ]

    mock_pg.insert_rows.return_value = 1

    res = run_once(args)
    assert res == 0
    mock_pg.ensure_tables.assert_not_called()
    mock_pg.insert_rows.assert_called_once()
    mock_redis.set.assert_not_called() # Tail mode should not checkpoint
