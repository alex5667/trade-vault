import pytest
from unittest.mock import MagicMock, patch
import redis
import logging
from tools.auto_apply_guard_prom_exporter_v1 import AutoApplyGuardExporter

@pytest.fixture
def mock_redis():
    with patch("redis.Redis.from_url") as mock:
        yield mock

def test_collect_metrics_busy_loading(mock_redis, caplog):
    # Setup mock redis to raise BusyLoadingError
    mock_redis_instance = MagicMock()
    mock_redis.return_value = mock_redis_instance
    
    # BusyLoadingError usually has "Loading" in the message
    # In redis-py it might be redis.exceptions.BusyLoadingError
    # We'll simulate it by raising a RedisError that our helper recognizes
    mock_pipe = mock_redis_instance.pipeline.return_value
    mock_pipe.execute.side_effect = redis.RedisError("Redis is LOADING the dataset in memory")
    
    exporter = AutoApplyGuardExporter("redis://mock:6379/0")
    
    with caplog.at_level(logging.WARNING):
        exporter.collect_metrics()
    
    assert "Redis is loading the dataset in memory" in caplog.text
    # Ensure it's not logged as ERROR
    assert "level\":\"ERROR" not in caplog.text

def test_collect_metrics_transient_error(mock_redis, caplog):
    mock_redis_instance = MagicMock()
    mock_redis.return_value = mock_redis_instance
    
    mock_pipe = mock_redis_instance.pipeline.return_value
    mock_pipe.execute.side_effect = redis.RedisError("Connection reset by peer")
    
    exporter = AutoApplyGuardExporter("redis://mock:6379/0")
    
    with caplog.at_level(logging.WARNING):
        exporter.collect_metrics()
    
    assert "Redis transient error collecting metrics" in caplog.text

def test_collect_metrics_fatal_error(mock_redis, caplog):
    mock_redis_instance = MagicMock()
    mock_redis.return_value = mock_redis_instance
    
    mock_pipe = mock_redis_instance.pipeline.return_value
    mock_pipe.execute.side_effect = redis.RedisError("Some fatal error")
    
    exporter = AutoApplyGuardExporter("redis://mock:6379/0")
    
    # The default log format in tool is JSON-like, let's just check level
    # Actually the exporter sets up logging in global scope, so we might need to be careful with caplog
    with caplog.at_level(logging.ERROR):
        exporter.collect_metrics()
    
    assert "Redis error collecting metrics" in caplog.text
