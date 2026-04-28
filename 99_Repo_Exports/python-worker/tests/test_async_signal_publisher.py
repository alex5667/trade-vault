from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import time
import asyncio
import pytest
import redis
from prometheus_client import REGISTRY

from services.async_signal_publisher import (
    AsyncSignalPublisher, 
    StreamSink,
    PUB_OK_TOTAL,
    PUB_ERR_TOTAL,
    PUB_BUSY_TOTAL,
    PUB_RETRIES_ENQUEUED_TOTAL,
    PUB_RETRIES_SUCCESS_TOTAL,
    PUB_DROPPED_TOTAL
)


class FakeAsyncRedis:
    def __init__(self):
        self.streams = {}
        self.raise_on = {}  # op -> exc

    async def xadd(self, stream: str, fields: dict, maxlen: int = 0, approximate: bool = True):
        if "xadd" in self.raise_on:
            raise self.raise_on["xadd"]
        self.streams.setdefault(stream, []).append(fields)
        return "1-0"


def get_metric_value(metric, **labels):
    try:
        return metric.labels(**labels)._value.get()
    except Exception:
        return 0


@pytest.fixture(autouse=True)
def reset_metrics():
    # Helper to reset metrics between tests if needed, 
    # but prometheus_client metrics are global. 
    # We'll just track deltas or check values carefully.
    pass


@pytest.mark.asyncio
async def test_async_publisher_normalizes_contract_and_writes_payload_field():
    r = FakeAsyncRedis()
    source = f"test_{get_ny_time_millis()}"
    pub = AsyncSignalPublisher(redis_client=r, source=source, metrics_prefix="t", logger=None)

    payload = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "entry": 123.45,
        "confidence": 0.8,
        "ts": get_ny_time_millis(),
    }
    
    before_ok = get_metric_value(PUB_OK_TOTAL, source=source, stream="raw")
    
    res = await pub.xadd_json(sink=StreamSink(name="raw", field="payload", maxlen=10), payload=payload, symbol="BTCUSDT")
    assert res.ok is True
    assert "raw" in r.streams and len(r.streams["raw"]) == 1

    after_ok = get_metric_value(PUB_OK_TOTAL, source=source, stream="raw")
    assert after_ok == before_ok + 1

    ser = r.streams["raw"][0]["payload"]
    obj = json.loads(ser)
    assert obj["symbol"] == "BTCUSDT"
    assert int(obj["ts_ms"]) > 0
    assert obj["side"] in ("LONG", "SHORT")
    assert obj["side_lc"] in ("long", "short")
    assert obj["side_uc"] in ("LONG", "SHORT")
    assert int(obj["side_int"]) in (1, -1)
    assert "signal_id" in obj and "sid" in obj
    assert 0.0 <= float(obj["confidence01"]) <= 1.0
    assert 0.0 <= float(obj["confidence_pct"]) <= 100.0


@pytest.mark.asyncio
async def test_async_publisher_raw_crypto_stream_fast_xadd_only(monkeypatch):
    monkeypatch.setenv("ASYNC_PUB_RAW_FAST_XADD_ONLY", "1")
    r = FakeAsyncRedis()
    source = f"test_raw_fast_{get_ny_time_millis()}"
    pub = AsyncSignalPublisher(redis_client=r, source=source, metrics_prefix="t", logger=None)

    payload = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "entry": 123.45,
        "confidence": 0.8,
    }

    res = await pub.xadd_json(
        sink=StreamSink(name="signals:crypto:raw", field="payload", maxlen=10),
        payload=payload,
        symbol="BTCUSDT",
    )

    assert res.ok is True
    obj = json.loads(r.streams["signals:crypto:raw"][0]["payload"])
    assert obj == payload


@pytest.mark.asyncio
async def test_async_publisher_busyloading_short_circuit():
    r = FakeAsyncRedis()
    r.raise_on["xadd"] = redis.exceptions.BusyLoadingError()
    source = f"test_busy_{get_ny_time_millis()}"
    pub = AsyncSignalPublisher(redis_client=r, source=source, metrics_prefix="t", logger=None)

    payload = {"symbol": "BTCUSDT", "side": "LONG", "entry": 10.0, "ts_ms": get_ny_time_millis()}
    
    before_busy = get_metric_value(PUB_BUSY_TOTAL, source=source, stream="raw")
    
    res = await pub.xadd_json(sink=StreamSink(name="raw", field="payload"), payload=payload, symbol="BTCUSDT")
    assert res.ok is False
    assert res.busy_loading is True
    assert "raw" not in r.streams
    
    after_busy = get_metric_value(PUB_BUSY_TOTAL, source=source, stream="raw")
    assert after_busy == before_busy + 1


@pytest.mark.asyncio
async def test_async_publisher_retries_on_network_error():
    r = FakeAsyncRedis()
    r.raise_on["xadd"] = Exception("network fail")
    source = f"test_retry_{get_ny_time_millis()}"
    pub = AsyncSignalPublisher(redis_client=r, source=source, metrics_prefix="t", logger=None)

    payload = {"symbol": "BTCUSDT", "side": "LONG", "entry": 10.0, "ts_ms": get_ny_time_millis()}
    pub.start()
    
    before_enqueued = get_metric_value(PUB_RETRIES_ENQUEUED_TOTAL, source=source, symbol="BTCUSDT")
    
    # 1) Call xadd_json. It should return ok=False but queue it.
    res = await pub.xadd_json(sink=StreamSink(name="raw", field="payload"), payload=payload, symbol="BTCUSDT")
    assert res.ok is False
    
    after_enqueued = get_metric_value(PUB_RETRIES_ENQUEUED_TOTAL, source=source, symbol="BTCUSDT")
    assert after_enqueued == before_enqueued + 1
    
    # 2) Clear the error and wait
    r.raise_on.pop("xadd")
    
    before_success = get_metric_value(PUB_RETRIES_SUCCESS_TOTAL, source=source, symbol="BTCUSDT")
    
    # Wait for background worker. 
    # Attempt 1 wait_sec = 0.5s * 2^(1-1) = 0.5s.
    for _ in range(20):
        if get_metric_value(PUB_RETRIES_SUCCESS_TOTAL, source=source, symbol="BTCUSDT") > before_success:
            break
        await asyncio.sleep(0.1)
        
    assert get_metric_value(PUB_RETRIES_SUCCESS_TOTAL, source=source, symbol="BTCUSDT") == before_success + 1
    assert "raw" in r.streams and len(r.streams["raw"]) == 1
    
    # Cleanup background task
    pub._worker_task.cancel()
    try:
        await pub._worker_task
    except asyncio.CancelledError:
        pass
