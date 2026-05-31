#!/usr/bin/env python3
"""
Orders HTTP Bridge - REST API for MT5 OrderExecutor.

Provides REST endpoints for MT5 Expert Advisor to poll orders and
confirm executions.

Endpoints:
    GET /healthz - Health check
    GET /orders/poll?symbol=XAUUSD - Poll next order from queue
    POST /orders/queue - Manually add order to queue
    POST /orders/confirm - Confirm order execution

Usage:
    uvicorn services.orders_http_bridge:app --host 0.0.0.0 --port 8088
"""

import os
import json
import redis
from fastapi import FastAPI, Response, Query
from fastapi.responses import JSONResponse
from typing import Dict, Optional

from core.redis_keys import RedisStreams as RS


# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
# Router is used for MT5 by default. Binance uses orders:queue:binance (handled by binance_executor).
ORDERS_QUEUE = os.getenv("ORDERS_QUEUE_MT5") or os.getenv("ORDERS_QUEUE") or RS.ORDERS_QUEUE_MT5
EXEC_STREAM = os.getenv("EXEC_STREAM", RS.ORDERS_EXEC)
# orders:exec stream size cap (0 = unlimited). Recommended production value: 500000.
EXEC_STREAM_MAXLEN: Optional[int] = int(os.getenv("EXEC_STREAM_MAXLEN", "0")) or None

# Connect to Redis
r = redis.from_url(REDIS_URL, decode_responses=True)

# Create FastAPI app
app = FastAPI(
    title="Orders HTTP Bridge",
    description="REST API для MT5 OrderExecutor",
    version="6.0.0"
)


@app.get("/healthz")
def health():
    """Health check endpoint."""
    try:
        r.ping()
        return {"ok": True, "redis": "connected"}
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=503
        )


@app.post("/orders/queue")
def queue_order(payload: Dict):
    """
    Manually add order to queue.
    
    Args:
        payload: Order payload dict
        
    Returns:
        Success response
    """
    sid = str(payload.get("sid") or "").strip()
    if not sid:
        return JSONResponse(
            {"error": "sid_required"},
            status_code=400
        )

    payload["sid"] = sid

    try:
        r.lpush(ORDERS_QUEUE, json.dumps(payload))
        return {"queued": True, "payload": payload}
    except Exception as e:
        return JSONResponse(
            {"error": str(e)},
            status_code=500
        )


@app.get("/orders/poll")
def poll_orders(symbol: Optional[str] = Query(None)):
    """
    Poll next order from queue (non-blocking).
    
    Args:
        symbol: Optional symbol filter
        
    Returns:
        Order payload or 204 No Content if queue is empty
    """
    # Non-blocking pop from right (FIFO)
    item = r.rpop(ORDERS_QUEUE)
    
    if not item:
        # No orders in queue
        return Response(status_code=204)
    
    # Parse payload
    try:
        payload = json.loads(item)
    except json.JSONDecodeError:
        return JSONResponse(
            {"error": "bad_json", "raw": item},
            status_code=400
        )
    
    # Symbol filter
    if symbol and payload.get("symbol") and payload["symbol"] != symbol:
        # Not this symbol - push back to left and return 204
        r.lpush(ORDERS_QUEUE, item)
        return Response(status_code=204)
    
    return payload


@app.post("/orders/confirm")
def confirm_execution(exec_report: Dict):
    """
    Confirm order execution.
    
    Args:
        exec_report: Execution report from MT5
        
    Returns:
        Success response
    """
    try:
        # Add to execution stream
        r.xadd(EXEC_STREAM, exec_report, maxlen=EXEC_STREAM_MAXLEN, approximate=True)
        
        return {"ok": True, "recorded": True}
    except Exception as e:
        return JSONResponse(
            {"error": str(e)},
            status_code=500
        )


@app.get("/orders/stats")
def get_stats():
    """Get queue and execution statistics."""
    try:
        queue_len = r.llen(ORDERS_QUEUE)
        exec_len = r.xlen(EXEC_STREAM)
        
        return {
            "queue_length": queue_len,
            "executions_total": exec_len
        }
    except Exception as e:
        return JSONResponse(
            {"error": str(e)},
            status_code=500
        )


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("ORDERS_HTTP_PORT", "8088"))
    uvicorn.run(app, host="0.0.0.0", port=port)

