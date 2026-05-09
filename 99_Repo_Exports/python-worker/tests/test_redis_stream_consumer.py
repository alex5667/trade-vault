
from redis.exceptions import ResponseError

from core.redis_stream_consumer import SyncRedisStreamHelper
from core.redis_keys import RedisStreams as RS


class FakeRedis:
    def __init__(self):
        self.created = []
        self._first = True

    def xreadgroup(self, group, consumer, streams_dict, count=10, block=0):
        # first call: emulate NOGROUP
        if self._first:
            self._first = False
            raise ResponseError("NOGROUP No such key")

        # second call: return 2 streams with 2 msgs total
        # redis-py returns: [(b"stream", [(b"id", {b"k": b"v"})])...]
        return [
            (b"ticks:BTCUSDT", [(b"1700000000000-0", {b"ts": b"1700000000000", b"px": b"100"})]),
            ("book:BTCUSDT", [("1700000000001-0", {"ts_ms": "1700000000001", "bid": "99"})]),
        ]

    def xgroup_create(self, stream, group, id="0", mkstream=True):
        self.created.append((stream, group, id, mkstream))
        return True


def test_read_new_creates_group_on_nogroup_and_returns_all_msgs():
    client = FakeRedis()
    consumer = SyncRedisStreamHelper(client=client, group="g", consumer="c")

    msgs = consumer.read_new(["ticks:BTCUSDT", "book:BTCUSDT"], count=10, block_ms=0)

    assert len(msgs) == 2
    assert {m.stream for m in msgs} == {"ticks:BTCUSDT", "book:BTCUSDT"}
    assert msgs[0].msg_id and msgs[1].msg_id

    # group created for both streams with default recovery_start_id="$"
    assert len(client.created) == 2
    for stream, group, start_id, mkstream in client.created:
        assert start_id == "$"  # default recovery_start_id


def test_read_new_decodes_bytes_fields():
    client = FakeRedis()
    consumer = SyncRedisStreamHelper(client=client, group="g", consumer="c")
    msgs = consumer.read_new(["ticks:BTCUSDT"], count=10, block_ms=0)

    tick = next(m for m in msgs if m.stream == "ticks:BTCUSDT")
    assert tick.fields["ts"] == "1700000000000"
    assert tick.fields["px"] == "100"


def test_read_new_outbox_uses_recovery_start_id_zero():
    """Test that outbox consumers use recovery_start_id='0' to avoid message loss."""
    client = FakeRedis()
    # Outbox consumers should use recovery_start_id="0"
    consumer = SyncRedisStreamHelper(client=client, group="outbox-group", consumer="c", recovery_start_id="0")

    msgs = consumer.read_new([RS.SIGNAL_OUTBOX], count=10, block_ms=0)

    assert len(msgs) == 2  # from FakeRedis

    # Should have created group with start_id="0" for outbox
    assert len(client.created) == 1
    stream, group, start_id, mkstream = client.created[0]
    assert stream == RS.SIGNAL_OUTBOX
    assert start_id == "0"  # outbox should start from beginning
