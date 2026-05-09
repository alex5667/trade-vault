import redis

from common.redis_errors import (
    get_redis_error_category,
    is_redis_busy_loading_error,
    is_redis_key_error,
    is_redis_stream_error,
    is_redis_timeout_error,
    is_transient_error,
)


def test_timeout_category_and_transient():
    e = redis.exceptions.TimeoutError("Timeout reading from socket")
    assert is_redis_timeout_error(e) is True
    # Note: redis-py TimeoutError inherits from ConnectionError, so category is "connection"
    assert get_redis_error_category(e) == "connection"
    assert is_transient_error(e) is True


def test_busy_category_and_transient():
    e = redis.exceptions.BusyLoadingError("Busy loading")
    assert is_redis_busy_loading_error(e) is True
    # Note: redis-py BusyLoadingError inherits from ConnectionError, so category is "connection"
    assert get_redis_error_category(e) == "connection"
    assert is_transient_error(e) is True


def test_stream_error_category():
    e = redis.exceptions.ResponseError("NOGROUP No such key 'x' or consumer group")
    assert is_redis_stream_error(e) is True
    assert get_redis_error_category(e) == "stream"


def test_key_error_category_wrongtype():
    e = redis.exceptions.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")
    assert is_redis_key_error(e) is True
    assert get_redis_error_category(e) == "key"
