# python-worker/tests/test_redis_stream_consumer_pending.py

from redis.exceptions import ResponseError

from core.redis_stream_consumer import SyncRedisStreamHelper, _parse_xpending_summary


class DummyRedis:
    def __init__(self, xpending_res=None, xpending_exc=None):
        self._res = xpending_res
        self._exc = xpending_exc

    def xpending(self, stream, group):
        if self._exc:
            raise self._exc
        return self._res


def test_parse_xpending_summary_dict():
    assert _parse_xpending_summary({"pending": 7, "min": "0-0", "max": "1-0", "consumers": []}) == 7


def test_parse_xpending_summary_tuple():
    assert _parse_xpending_summary((12, b"0-0", b"1-0", [])) == 12


def test_parse_xpending_summary_unknown():
    assert _parse_xpending_summary("weird") == 0


def test_pending_len_ok_dict():
    r = DummyRedis(xpending_res={"pending": 3})
    c = SyncRedisStreamHelper(r, group="g", consumer="c1")
    assert c.pending_len("book:BTCUSDT") == 3


def test_pending_len_nogroup_returns_zero():
    r = DummyRedis(xpending_exc=ResponseError("NOGROUP No such key 'x' or consumer group 'g'"))
    c = SyncRedisStreamHelper(r, group="g", consumer="c1")
    assert c.pending_len("book:BTCUSDT") == 0
