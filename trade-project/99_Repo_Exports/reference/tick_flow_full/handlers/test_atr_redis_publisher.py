# test_atr_redis_publisher.py
"""
Tests for AtrRedisPublisher - pipeline, keys, ttl, float parsing.
"""
import pytest
from unittest.mock import Mock

from handlers.atr_redis_publisher import AtrRedisPublisher


def test_publish_writes_hash_and_legacy():
    redis = Mock()
    pipe = Mock()
    redis.pipeline.return_value = pipe

    pub = AtrRedisPublisher(redis, "BTCUSDT")
    pub.publish("1m", 12.345, ts_ms=1700000000000)

    # pipeline should be used
    redis.pipeline.assert_called_once()

    # keys
    hash_key = "atrh:BTCUSDT:1m"
    legacy_key = "atr:BTCUSDT:1m"

    # hset + expire + set + execute
    assert pipe.hset.call_count == 1
    assert pipe.expire.call_count == 1
    assert pipe.set.call_count == 1
    assert pipe.execute.call_count == 1

    args, kwargs = pipe.hset.call_args
    assert args[0] == hash_key
    mapping = kwargs.get("mapping") or {}
    assert "v" in mapping and "ts" in mapping
    assert mapping["ts"] == "1700000000000"

    args, kwargs = pipe.set.call_args
    assert args[0] == legacy_key
    assert "ex" in kwargs


def test_publish_ignores_non_positive():
    redis = Mock()
    pub = AtrRedisPublisher(redis, "BTCUSDT")
    pub.publish("1m", 0.0, ts_ms=1)
    pub.publish("1m", -1.0, ts_ms=1)
    assert redis.pipeline.call_count == 0


def test_publish_ignores_no_redis():
    pub = AtrRedisPublisher(None, "BTCUSDT")
    pub.publish("1m", 1.0, ts_ms=1)  # should not throw
