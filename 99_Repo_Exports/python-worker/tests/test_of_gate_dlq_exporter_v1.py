import os
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from orderflow_services.of_gate_dlq_exporter_v1 import (
    Exporter,
    _id_to_ms,
    _parse_streams,
    of_gate_dlq_age_sec,
    of_gate_dlq_exporter_errors_total,
    of_gate_dlq_exporter_poll_ts_ms,
    of_gate_dlq_exporter_up,
    of_gate_dlq_last_id_ms,
    of_gate_dlq_len,
)


def test_parse_streams():
    assert _parse_streams("") == []
    assert _parse_streams("a,b, c") == ["a", "b", "c"]
    assert _parse_streams("stream:dlq:1") == ["stream:dlq:1"]


def test_id_to_ms():
    assert _id_to_ms("1600000000000-0") == 1600000000000
    assert _id_to_ms("") == 0
    assert _id_to_ms("invalid") == 0


@patch.dict(os.environ, {"REDIS_URL": "redis://localhost"}, clear=True)
@patch("redis.Redis.from_url")
def test_exporter_poll_one_empty(mock_from_url):
    mock_redis = MagicMock()
    mock_redis.xlen.return_value = 0
    mock_from_url.return_value = mock_redis

    exp = Exporter()
    n, first_id, last_id, p_counts = exp._poll_one("stream:1")
    assert n == 0
    assert last_id == 0


@patch.dict(os.environ, {"REDIS_URL": "redis://localhost"}, clear=True)
@patch("redis.Redis.from_url")
def test_exporter_poll_one_has_data(mock_from_url):
    mock_redis = MagicMock()
    mock_redis.xlen.return_value = 5
    # xrevrange returns list of (id, dict)
    mock_redis.xrevrange.return_value = [("1600001234567-0", {"a": "b"})]
    mock_from_url.return_value = mock_redis

    exp = Exporter()
    n, first_id, last_id, p_counts = exp._poll_one("stream:1")
    assert n == 5
    assert last_id == 1600001234567


@patch.dict(os.environ, {"REDIS_URL": "redis://localhost", "OF_GATE_DLQ_STREAMS": "s1,s2"}, clear=True)
@patch("redis.Redis.from_url")
def test_exporter_loop_iteration(mock_from_url):
    mock_redis = MagicMock()
    mock_redis.xlen.side_effect = [10, 0]
    mock_redis.xrevrange.side_effect = [
        [("1000000000000-0", {})],
        [],
    ]
    mock_from_url.return_value = mock_redis

    exp = Exporter()
    
    # We want to run one loop iteration and stop
    original_poll_one = exp._poll_one
    def fake_poll_one(key: str):
        # schedule stop so loop ends
        exp.running = False
        return original_poll_one(key)
        
    exp._poll_one = fake_poll_one
    
    with patch("orderflow_services.of_gate_dlq_exporter_v1._now_ms", return_value=1000000050000):
        # Sleep patch to make loop return instantly
        with patch("time.sleep"):
            exp.loop()
            
    # Check metrics
    s1_len = of_gate_dlq_len.labels(stream="s1")._value.get()
    s2_len = of_gate_dlq_len.labels(stream="s2")._value.get()
    assert s1_len == 10.0
    assert s2_len == 0.0
    
    s1_age = of_gate_dlq_age_sec.labels(stream="s1")._value.get()
    # 1000000050000 - 1000000000000 = 50000 ms = 50 sec
    assert s1_age == 50.0
    
    assert of_gate_dlq_exporter_up._value.get() == 1.0
