import redis

from common.redis_errors import (
    is_redis_connection_error,
    is_redis_key_error,
    is_redis_stream_error,
    get_redis_error_category,
)


def test_connection_error_category():
    e = redis.exceptions.ConnectionError("connection refused")
    assert is_redis_connection_error(e) is True
    assert get_redis_error_category(e) in ("connection", "timeout", "busy")


def test_timeout_is_connection_category():
    e = redis.exceptions.TimeoutError("Timeout")
    assert is_redis_connection_error(e) is True
    assert get_redis_error_category(e) == "connection"  # TimeoutError is classified as connection


def test_wrongtype_is_key_error():
    e = redis.exceptions.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")
    assert is_redis_key_error(e) is True
    assert get_redis_error_category(e) == "key"


def test_nogroup_is_stream_error():
    e = redis.exceptions.ResponseError("NOGROUP No such key 'mystream' or consumer group 'mygroup'")
    assert is_redis_stream_error(e) is True
    assert get_redis_error_category(e) == "stream"


def test_unknown_fallback():
    e = ValueError("something else")
    assert get_redis_error_category(e) in ("unknown", "transient")
