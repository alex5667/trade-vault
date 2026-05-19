#!/usr/bin/env python3
"""
Orders HTTP Bridge - REST API for MT5 OrderExecutor.

Provides REST endpoints for MT5 Expert Advisor to poll orders and
confirm executions.

Endpoints:
    GET /healthz - Health check
    GET /orders/poll?symbol= - Poll next order from queue
    POST /orders/queue - Manually add order to queue
    POST /orders/confirm - Confirm order execution

Usage:
    uvicorn services.orders_http_bridge:app --host 0.0.0.0 --port 8088
"""

import json
import os

import redis
from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse

from core.redis_keys import RedisStreams as RS
import contextlib

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
# Router is used for MT5 by default. Binance uses orders:queue:binance (handled by binance_executor).
ORDERS_QUEUE = os.getenv("ORDERS_QUEUE_MT5") or os.getenv("ORDERS_QUEUE") or RS.ORDERS_QUEUE_MT5
EXEC_STREAM = os.getenv("EXEC_STREAM", RS.ORDERS_EXEC)
# orders:exec stream size cap (0 = unlimited). Recommended production value: 500000.
EXEC_STREAM_MAXLEN: int | None = int(os.getenv("EXEC_STREAM_MAXLEN", "0")) or None

# Connect to Redis
r = redis.from_url(REDIS_URL, decode_responses=True)

# Create FastAPI app
app = FastAPI(
    title="Orders HTTP Bridge",
    description="REST API для MT5 OrderExecutor",
    version="6.0.0"
)


# Consumer group for MT5 polling
GROUP_NAME = "mt5-executor-group"
CONSUMER_NAME = "mt5-ea-1"

# Initialize Redis consumer group
with contextlib.suppress(Exception):
    r.xgroup_create(ORDERS_QUEUE, GROUP_NAME, id="0", mkstream=True)


@app.get("/healthz")
def health():
    """Health check endpoint."""
    try:
        r.ping()
        return {"ok": True, "redis": "connected", "queue": ORDERS_QUEUE}
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=503
        )


@app.post("/orders/queue")
def queue_order(payload: dict):
    """
    Manually add order to queue (as a Stream).
    """
    sid = (payload.get("sid") or "").strip()
    if not sid:
        return JSONResponse({"error": "sid_required"}, status_code=400)

    payload["sid"] = sid
    if "tp_levels" in payload and isinstance(payload["tp_levels"], list):
        payload["tp_levels"] = json.dumps(payload["tp_levels"])

    try:
        r.xadd(ORDERS_QUEUE, payload, maxlen=1000, approximate=True)
        return {"queued": True, "payload": payload}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/orders/poll")
def poll_orders(symbol: str | None = Query(None)):
    """
    Poll next order from queue (Stream xreadgroup).
    """
    try:
        # Read new messages from the stream
        msgs = r.xreadgroup(GROUP_NAME, CONSUMER_NAME, {ORDERS_QUEUE: ">"}, count=1)
        if not msgs:
            return Response(status_code=204)

        stream_name, entries = msgs[0]
        msg_id, payload = entries[0]

        # Add message ID to payload so client can confirm it
        payload["_msg_id"] = msg_id

        # If tp_levels was JSON-encoded, decode it
        if "tp_levels" in payload and isinstance(payload["tp_levels"], str):
            with contextlib.suppress(Exception):
                payload["tp_levels"] = json.loads(payload["tp_levels"])

        # Redis Stream stringifies all values; normalize numeric fields back to int
        # so consumers don't need to handle dual string/int encoding (F6 contract fix).
        if "side_int" in payload:
            with contextlib.suppress(ValueError, TypeError):
                payload["side_int"] = int(payload["side_int"])

        # Symbol filter
        if symbol and payload.get("symbol") and payload["symbol"] != symbol:
            # We can't easily "push back" to a stream in a group,
            # but we can leave it un-ACKed and it will be re-read by others or on timeout.
            # However, for simplicity with single-EA setups, we just return it or ignore.
            # In MT5 context, usually one EA handles one symbol or all symbols.
            pass

        return payload
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/orders/confirm")
def confirm_execution(exec_report: dict):
    """
    Confirm order execution and ACK stream message.
    """
    msg_id = exec_report.pop("_msg_id", None)
    try:
        # Add to execution stream
        r.xadd(EXEC_STREAM, exec_report, maxlen=EXEC_STREAM_MAXLEN, approximate=True)

        # ACK the message in orders queue if ID was provided
        if msg_id:
            r.xack(ORDERS_QUEUE, GROUP_NAME, msg_id)
            r.xdel(ORDERS_QUEUE, msg_id) # Optional: clean up stream

        return {"ok": True, "recorded": True, "acked": bool(msg_id)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/orders/stats")
def get_stats():
    """Get queue and execution statistics."""
    try:
        queue_info = r.xinfo_stream(ORDERS_QUEUE)
        queue_len = queue_info.get("length", 0)
        exec_len = r.xlen(EXEC_STREAM)

        return {
            "queue_length": queue_len,
            "executions_total": exec_len
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("ORDERS_HTTP_PORT", "8088"))
    uvicorn.run(app, host="0.0.0.0", port=port)

