from __future__ import annotations

import time
from core.redis_keys import RedisStreams as RS


class _FakeRedisStream:
    """
    Minimal fake for SignalOutboxPublisher Lua contract:
      XADD stream ... 'data' envelope_json
    We store entries as list[(id, fields)] where fields are strings.
    """
    def __init__(self) -> None:
        self.entries: list[dict[str, str]] = []
        self._i = 0

    def evalsha(self, sha, numkeys, *args):  # noqa: D401
        # args layout from SignalOutboxPublisher.publish():
        # KEYS[1]=dedup_key, KEYS[2]=stream, ARGV[1]=ttl, ARGV[2]=maxlen, ARGV[3]=envelope_json
        envelope_json = args[-1]
        self._i += 1
        msg_id = f"{self._i}-0"
        self.entries.append({"data": str(envelope_json)})
        return [1, msg_id]

    def eval(self, script, numkeys, *args):
        return self.evalsha("na", numkeys, *args)


def test_outbox_adapter_hard_invalid_ts_is_corrected_to_now_and_marked(monkeypatch):
    from core.signal_outbox import OutboxSettings, SignalOutboxPublisher
    from handlers.emitter.outbox_publisher_adapter import OutboxPublisherAdapter
    from runners.trade_monitor_runner import _parse_signal

    # Build publisher with fake redis behind it.
    fake = _FakeRedisStream()
    pub = SignalOutboxPublisher.__new__(SignalOutboxPublisher)
    pub.redis = fake
    pub.settings = OutboxSettings(outbox_stream=RS.SIGNAL_OUTBOX, outbox_maxlen=1000, dedup_ttl_ms=60000, dedup_bucket_ms=60000)
    pub._sha = "na"
    pub._ensure_script = lambda: "na"
    pub.build_dedup_key = lambda **kw: "dedup:key"

    adapter = OutboxPublisherAdapter(outbox_publisher=pub)

    # Freeze time to make correction deterministic.
    monkeypatch.setattr(time, "time", lambda: 1_700_000_000.0)  # seconds
    now_ms = 1_700_000_000_000

    payload = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "timeframe": "1m",
        "entry": 100.0,
        "sl": 99.0,
        "tp_levels": [101.0, 102.0, 103.0],
        # non-epoch clock (minutes-of-day) => must be rejected and stripped
        "ts": 600,
    }

    msg_id = adapter.publish(payload)
    assert isinstance(msg_id, str) and msg_id

    # Outbox contract: single field 'data' with JSON.
    fields = dict(fake.entries[-1])
    assert "data" in fields

    raw = _parse_signal(fields)
    # HARD behavior: adapter must correct to now (epoch ms) and mark it.
    assert int(raw.get("ts") or 0) == now_ms
    assert int(raw.get("ts_ms") or 0) == now_ms
    # Audit markers (must be present):
    assert raw.get("ts_invalid") == 1
    assert raw.get("ts_raw") == 600
    assert raw.get("ts_corrected") == 1
    assert raw.get("ts_corrected_to") == "now"
