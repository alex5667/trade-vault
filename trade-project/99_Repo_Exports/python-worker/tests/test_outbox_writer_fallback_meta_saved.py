import json

from handlers.emitter.outbox_writer import OutboxWriter  # поправьте импорт если нужно


class FakeRedis:
    def __init__(self):
        self.kv = {}

    def set(self, key, value, nx=False, ex=None, xx=False):
        if nx and key in self.kv:
            return False
        if xx and key not in self.kv:
            return False
        self.kv[key] = value
        return True


class Pub:
    def publish(self, payload):
        # emulate xadd entry id
        return "1-0"


class Logger:
    def exception(self, *args, **kwargs):
        return


def test_outbox_writer_fallback_success_saves_meta(monkeypatch):
    # Force fallback path: no stream_key / dedup can be false
    w = OutboxWriter.__new__(OutboxWriter)
    r = FakeRedis()

    w._redis = lambda: r
    w._stream_key = None
    w._retries = 0
    w._retry_sleep_ms = 0
    w._pub = Pub()
    w._dedup_ttl_ms = 60_000
    w._logger = Logger()

    # methods used early in write()
    w._dedup_key = lambda signal_id: f"dedup:{signal_id}"
    w._sem_key = lambda payload: None

    # meta serializer must be deterministic json
    w._serialize_meta = lambda meta: json.dumps(meta, ensure_ascii=False, separators=(",", ":"))

    # Use known prefix
    monkeypatch.setenv("OUTBOX_META_PREFIX", "signal:meta:")
    monkeypatch.setenv("OUTBOX_META_TTL_SEC", "60")

    ok = w.write(
        payload={"sid": "s1", "ts_ms": 1, "targets": {}, "meta": {}},
        signal_id="s1",
        dedup=False,
        meta={"payload_meta": {"k": 1}},
    )
    assert ok is True

    meta_key = "signal:meta:s1"
    assert meta_key in r.kv
    # meta json must be valid
    obj = json.loads(r.kv[meta_key])
    assert "payload_meta" in obj
