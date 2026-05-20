"""POST /orders/queue input validation regression.

Earlier code accepted any payload with a `sid` field and XADD'd it raw into
the MT5 queue — a malformed manual POST could wedge the EA on read.

We test the handler function directly (skipping TestClient) because the
project's installed starlette/httpx pair is incompatible with FastAPI's
TestClient ctor shape.
"""
from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def queue_order(monkeypatch):
    fake_redis = MagicMock()
    fake_redis.xgroup_create.return_value = None
    fake_redis.xadd.return_value = "1-0"
    fake_redis.ping.return_value = True

    import redis as redis_mod
    monkeypatch.setattr(redis_mod, "from_url", lambda *_a, **_kw: fake_redis)

    sys.modules.pop("services.orders_http_bridge", None)
    from services.orders_http_bridge import queue_order as handler

    return handler, fake_redis


def _body(resp):
    """Extract JSON body from a JSONResponse or a plain dict return."""
    if isinstance(resp, dict):
        return resp, 200
    # fastapi.responses.JSONResponse
    return json.loads(bytes(resp.body).decode()), resp.status_code


def test_missing_sid_rejected(queue_order):
    handler, _ = queue_order
    body, status = _body(handler({"symbol": "BTCUSDT", "direction": "BUY"}))
    assert status == 400
    assert body["error"] == "sid_required"


def test_missing_symbol_rejected(queue_order):
    handler, _ = queue_order
    body, status = _body(handler({"sid": "x", "direction": "BUY"}))
    assert status == 400
    assert body["error"] == "symbol_required"


def test_invalid_direction_rejected(queue_order):
    handler, _ = queue_order
    body, status = _body(handler({"sid": "x", "symbol": "BTCUSDT", "direction": "DIAGONAL"}))
    assert status == 400
    assert body["error"] == "direction_invalid"


def test_numeric_field_not_a_number(queue_order):
    handler, _ = queue_order
    body, status = _body(handler({
        "sid": "x", "symbol": "BTCUSDT", "direction": "BUY", "entry": "abc"
    }))
    assert status == 400
    assert body["error"] == "numeric_field_invalid"
    assert body["field"] == "entry"


def test_numeric_field_negative_rejected(queue_order):
    handler, _ = queue_order
    body, status = _body(handler({
        "sid": "x", "symbol": "BTCUSDT", "direction": "BUY", "qty": -1.0
    }))
    assert status == 400
    assert body["error"] == "numeric_field_out_of_range"
    assert body["field"] == "qty"


def test_numeric_field_nan_rejected(queue_order):
    handler, _ = queue_order
    body, status = _body(handler({
        "sid": "x", "symbol": "BTCUSDT", "direction": "BUY", "sl": float("nan")
    }))
    assert status == 400
    assert body["error"] == "numeric_field_out_of_range"


def test_valid_payload_normalizes_and_queues(queue_order):
    handler, fake = queue_order
    body, status = _body(handler({
        "sid": "iceberg:BTCUSDT:1779129093297:S",
        "symbol": "btcusdt",  # lowercase → must be uppercased
        "direction": "SHORT",
        "entry": 50000.0,
        "sl": 50500.0,
        "tp_levels": [49500.0, 49000.0],
    }))
    assert status == 200, body
    assert body["queued"] is True
    assert body["payload"]["symbol"] == "BTCUSDT"
    # tp_levels json-encoded for Redis Stream
    assert isinstance(body["payload"]["tp_levels"], str)
    fake.xadd.assert_called_once()


def test_buy_and_sell_aliases_accepted(queue_order):
    handler, _ = queue_order
    for d in ("BUY", "SELL", "LONG", "SHORT", "long", "Sell"):
        body, status = _body(handler({
            "sid": "x", "symbol": "BTCUSDT", "direction": d,
        }))
        assert status == 200, (d, body)
