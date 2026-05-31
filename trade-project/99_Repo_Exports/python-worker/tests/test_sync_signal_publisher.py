from __future__ import annotations

import json

from services.sync_signal_publisher import StreamSink, SyncSignalPublisher
from utils.time_utils import get_ny_time_millis


class FakeRedis:
    def __init__(self):
        self.streams = {}
        self.counters = {}

    def xadd(self, stream, fields, maxlen=0, approximate=True):
        self.streams.setdefault(stream, []).append(fields)
        return "1-0"

    def incr(self, key):
        self.counters[key] = int(self.counters.get(key, 0)) + 1
        return self.counters[key]


def test_sync_publisher_writes_json_and_contract_mirrors():
    r = FakeRedis()
    pub = SyncSignalPublisher(redis_client=r, source="test", metrics_prefix="t", logger=None)

    payload = {"symbol": "BTCUSDT", "direction": "LONG", "entry": 10.0, "confidence": 0.8, "ts": get_ny_time_millis()}
    res = pub.xadd_json(sink=StreamSink(name="raw", field="payload", maxlen=10), payload=payload, symbol="BTCUSDT")
    assert res.ok is True
    ser = r.streams["raw"][0]["payload"]
    obj = json.loads(ser)
    assert obj["symbol"] == "BTCUSDT"
    assert int(obj["ts_ms"]) > 0
    assert obj["direction"] in ("LONG", "SHORT")
    assert obj["side"] in ("BUY", "SELL")
    assert int(obj["side_int"]) in (1, -1)
    assert "signal_id" in obj and "sid" in obj
    assert 0.0 <= float(obj["confidence01"]) <= 1.0
    assert 0.0 <= float(obj["confidence_pct"]) <= 100.0
