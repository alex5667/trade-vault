import json

from core.unified_signal_emitter import UnifiedSignalEmitter


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.streams = {}
        self._seq = 0

    def set(self, key, value, nx=False, xx=False, ex=None):
        exists = key in self.kv
        if nx and exists:
            return False
        if xx and not exists:
            return False
        self.kv[key] = (value, ex)
        return True

    def delete(self, key):
        self.kv.pop(key, None)
        return 1

    def xadd(self, stream, fields, maxlen=None, approximate=None, **kwargs):
        self._seq += 1
        eid = f"{self._seq}-0"
        self.streams.setdefault(stream, []).append((eid, dict(fields)))
        return eid


class FakeLogger:
    def warning(self, msg):  # pragma: no cover
        pass
    def exception(self, msg):  # pragma: no cover
        pass


def test_emitter_puts_labels_inside_payload_labels():
    r = FakeRedis()
    e = UnifiedSignalEmitter(redis=r, logger=FakeLogger(), outbox_stream="signals:outbox")

    res = e.emit(
        signal_id="sid-10",
        kind="breakout",
        symbol="BTCUSDT",
        side="buy",
        raw_score=2.0,
        final_score=1.2,
        confidence_pct=78.0,
        payload={"foo": "bar"},
        labels={"l2_stale": False, "reason": "ok"},
        ts_event_ms=123456,
    )
    assert res.ok and res.written
    assert len(r.streams["signals:outbox"]) == 1

    _, fields = r.streams["signals:outbox"][0]
    payload = json.loads(fields["payload_json"])
    assert payload["foo"] == "bar"
    assert payload["labels"]["l2_stale"] is False
    assert payload["labels"]["reason"] == "ok"


def test_emitter_idempotent_same_signal_id():
    r = FakeRedis()
    e = UnifiedSignalEmitter(redis=r, logger=FakeLogger(), outbox_stream="signals:outbox")

    res1 = e.emit(signal_id="sid-11", kind="extreme", symbol="ETHUSDT", payload={"a": 1})
    assert res1.ok and res1.written

    res2 = e.emit(signal_id="sid-11", kind="extreme", symbol="ETHUSDT", payload={"a": 1})
    assert res2.ok and res2.duplicate
    assert len(r.streams["signals:outbox"]) == 1
