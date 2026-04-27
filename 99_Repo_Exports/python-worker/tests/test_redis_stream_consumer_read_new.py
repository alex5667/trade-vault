import pytest

from core.redis_stream_consumer import SyncRedisStreamHelper


class FakeRedis:
    def __init__(self, res):
        self._res = res
        self.calls = 0

    def xreadgroup(self, group, consumer, streams_dict, count=10, block=0):
        self.calls += 1
        return self._res

    def xgroup_create(self, stream, group, id="0", mkstream=True):
        return True


def test_read_new_returns_all_messages_and_decodes_bytes():
    res = [
        (b"book:BTCUSDT", [
            (b"1-0", {b"ts_ms": b"1000", b"foo": b"bar"}),
            (b"2-0", {b"ts_ms": b"1100", b"n": 123}),
        ]),
        ("ticks:BTCUSDT", [
            ("3-0", {"ts": "1200", "x": "y"}),
        ]),
    ]
    r = FakeRedis(res)
    c = SyncRedisStreamHelper(r, "g", "c1")

    msgs = c.read_new(["book:BTCUSDT", "ticks:BTCUSDT"], count=100, block_ms=0)
    assert len(msgs) == 3
    assert msgs[0].stream == "book:BTCUSDT"
    assert msgs[0].msg_id == "1-0"
    assert msgs[0].fields["foo"] == "bar"
    assert msgs[1].fields["n"] == "123"
    assert msgs[2].stream == "ticks:BTCUSDT"