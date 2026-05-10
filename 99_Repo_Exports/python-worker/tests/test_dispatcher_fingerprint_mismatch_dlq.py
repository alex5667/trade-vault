import json

from common.json_safe import to_json_safe
from common.payload_fingerprint import fingerprint_tradeable_payload
from services.dispatch.dispatcher_app import SignalDispatcher
from tests.fake_redis import FakeRedis
from core.redis_keys import RedisStreams as RS


def test_parse_envelope_fingerprint_mismatch_goes_dlq_and_returns_none():
    r = FakeRedis()
    d = SignalDispatcher.__new__(SignalDispatcher)
    d.redis = r
    d.dlq_stream = RS.SIGNAL_DLQ

    env = {"sid": "s1", "ts_ms": 1, "targets": {"notify": {"x": "1"}}, "meta": {}}
    env_safe = to_json_safe(env)
    sha1, _n = fingerprint_tradeable_payload(env_safe)
    env_safe["meta"]["payload_sha1"] = sha1

    # corrupt AFTER fingerprint
    env_safe["targets"]["notify"]["x"] = "2"
    raw = json.dumps(env_safe, ensure_ascii=False)
    out = d._parse_envelope({"data": raw})
    assert out is None
    assert r.xlen(RS.SIGNAL_DLQ) == 1
