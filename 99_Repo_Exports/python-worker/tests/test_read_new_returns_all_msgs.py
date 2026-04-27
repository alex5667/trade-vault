from __future__ import annotations


class _FakeRedis:
    def __init__(self, res):
        self._res = res
        self.created = []

    def xreadgroup(self, group, consumer, streams_dict, count, block):
        return self._res

    def xgroup_create(self, stream, group, id="0", mkstream=True):
        self.created.append((stream, group, id, mkstream))


def test_read_new_returns_all_messages_and_normalizes_bytes():
    from core.redis_stream_consumer import SyncRedisStreamHelper

    res = [
        (b"ticks:BTCUSDT", [
            (b"1700000000000-0", {b"a": b"1", b"b": b"2"}),
            (b"1700000000001-0", {b"x": b"y"}),
        ]),
        ("book:BTCUSDT", [
            ("1700000000002-0", {"k": "v"}),
        ]),
    ]
    client = _FakeRedis(res)
    c = SyncRedisStreamHelper(client=client, group="g", consumer="c")

    msgs = c.read_new(["ticks:BTCUSDT", "book:BTCUSDT"], count=10, block_ms=0)

    assert len(msgs) == 3
    assert msgs[0].stream == "ticks:BTCUSDT"
    assert msgs[0].msg_id == "1700000000000-0"
    assert msgs[0].fields == {"a": "1", "b": "2"}
    assert msgs[1].msg_id == "1700000000001-0"
    assert msgs[2].stream == "book:BTCUSDT"
    assert msgs[2].fields == {"k": "v"}
