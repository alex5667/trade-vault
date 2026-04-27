from __future__ import annotations

import json
import time
import types
import redis


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.streams = {}
        self.lists = {}
        self.counters = {}
        self.raise_on = {}  # op -> exception

    def incr(self, key: str):
        self.counters[key] = int(self.counters.get(key, 0)) + 1
        return self.counters[key]

    def set(self, key: str, val: str, ex: int = 0):
        if "set" in self.raise_on:
            raise self.raise_on["set"]
        self.kv[key] = val
        return True

    def xadd(self, stream: str, fields: dict, maxlen: int = 0, approximate: bool = True):
        if "xadd" in self.raise_on:
            raise self.raise_on["xadd"]
        self.streams.setdefault(stream, []).append(fields)
        return "1-0"

    def lpush(self, key: str, val: str):
        if "lpush" in self.raise_on:
            raise self.raise_on["lpush"]
        self.lists.setdefault(key, []).insert(0, val)
        return len(self.lists[key])

    def xadd(self, stream: str, fields: dict, maxlen: int = 0, approximate: bool = True):
        self.streams.setdefault(stream, []).append(fields)
        return "1-0"

    def lpush(self, key: str, val: str):
        self.lists.setdefault(key, []).insert(0, val)
        return len(self.lists[key])


class DummyOrderBuilder:
    def build_order_from_signal(self, signal_payload: dict):
        # minimal order payload
        return {"sid": signal_payload.get("sid"), "symbol": signal_payload.get("symbol"), "side": signal_payload.get("side")}


def test_build_payload_has_normalized_mirrors(monkeypatch):
    monkeypatch.setenv("ICEBERG_SID_RANDOM_SUFFIX", "0")
    from services.binance_iceberg_detector import _build_iceberg_signal_payload

    st = types.SimpleNamespace(refresh_count=3, visible_qty=12.5, since_ts=time.time() - 2.0)
    p = _build_iceberg_signal_payload(
        symbol="BTCUSDT",
        direction="LONG",
        price=100.0,
        state=st,
        level_info={"kind": "bid", "price": 99.5},
    )
    assert p["side"] == "LONG"
    assert p["kind"] == "iceberg"
    assert p["entry_price"] == 100.0
    assert p["price"] == 100.0
    assert int(p["ts_ms"]) == int(p["ts"])


def test_publish_payload_enforces_contract_and_writes_all_sinks(monkeypatch):
    monkeypatch.setenv("SIGNAL_PREPROCESS_ENABLED", "1")
    monkeypatch.setenv("ICEBERG_SID_RANDOM_SUFFIX", "0")
    from services.binance_iceberg_detector import _build_iceberg_signal_payload
    from services.signal_publisher import SignalPublisher, PublishSinks

    r = FakeRedis()
    ob = DummyOrderBuilder()

    st = types.SimpleNamespace(refresh_count=1, visible_qty=1.0, since_ts=time.time() - 1.0)
    payload = _build_iceberg_signal_payload(
        symbol="BTCUSDT",
        direction="LONG",
        price=123.45,
        state=st,
        level_info={"kind": "bid", "price": 123.0},
    )

    pub = SignalPublisher(
        redis_client=r,
        sinks=PublishSinks(
            store_prefix="signals:",
            raw_stream="raw_stream",
            notify_stream="notify_stream",
            orders_queue="orders_q",
        ),
        source="binance_iceberg_detector",
        metrics_prefix="iceberg_publish",
        logger=None,
        order_builder=ob,
    )
    res = pub.publish(payload, symbol="BTCUSDT")
    assert res.ok is True

    # store
    sid = payload["signal_id"]
    stored = json.loads(r.kv[f"signals:{sid}"])

    # Contract fields expected from preprocess + builder mirrors
    assert "ts_ms" in stored
    assert stored.get("side") in ("LONG", "SHORT")
    assert int(stored.get("side_int", 0)) in (1, -1)
    assert float(stored.get("entry_price", 0.0)) > 0
    assert float(stored.get("price", 0.0)) > 0

    # streams
    assert "raw_stream" in r.streams and len(r.streams["raw_stream"]) == 1
    assert "notify_stream" in r.streams and len(r.streams["notify_stream"]) == 1

    # orders queue
    assert "orders_q" in r.lists and len(r.lists["orders_q"]) == 1


def test_publish_busyloading_fail_open(monkeypatch):
    monkeypatch.setenv("SIGNAL_PREPROCESS_ENABLED", "1")
    monkeypatch.setenv("ICEBERG_SID_RANDOM_SUFFIX", "0")
    from services.binance_iceberg_detector import _build_iceberg_signal_payload
    from services.signal_publisher import SignalPublisher, PublishSinks

    r = FakeRedis()
    r.raise_on["set"] = redis.exceptions.BusyLoadingError()

    st = types.SimpleNamespace(refresh_count=1, visible_qty=1.0, since_ts=time.time() - 1.0)
    payload = _build_iceberg_signal_payload(
        symbol="BTCUSDT",
        direction="SHORT",
        price=10.0,
        state=st,
        level_info={"kind": "ask", "price": 10.1},
    )

    pub = SignalPublisher(
        redis_client=r,
        sinks=PublishSinks(
            store_prefix="signals:",
            raw_stream="raw_stream",
            notify_stream="notify_stream",
            orders_queue="orders_q",
        ),
        source="binance_iceberg_detector",
        metrics_prefix="iceberg_publish",
        logger=None,
        order_builder=DummyOrderBuilder(),
    )
    res = pub.publish(payload, symbol="BTCUSDT")
    assert res.ok is False
    assert res.busy_loading is True