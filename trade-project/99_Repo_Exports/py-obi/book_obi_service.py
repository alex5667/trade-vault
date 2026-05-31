#!/usr/bin/env python3
"""
OBI Service - Order Book Imbalance analysis with events & PNG rendering.

Simplified standalone service for OBI calculation, event detection, and visualization.

Endpoints:
    POST /book - Receive book snapshot from MT5 BookBridge
    GET /features/obi?symbol=XAUUSD&last=200 - Get OBI history
    GET /events/pull?symbol=XAUUSD&last=50 - Get OBI events
    GET /render/obi.png?symbol=XAUUSD - OBI timeline PNG
    GET /render/depth.png?symbol=XAUUSD - Depth profile PNG
    GET /healthz - Health check

Environment:
    OBI_WINDOW_LEVELS - DOM depth levels (default: 5)
    OBI_THRESHOLD - Sustain threshold (default: 0.25)
    OBI_SUSTAIN_MS - Sustain duration (default: 1200ms)
    RING_SECONDS - History buffer (default: 600s)
    NOTIFY_URL - Notification endpoint (default: http://127.0.0.1:8088/notify)

Usage:
    python3 -m venv .venv && source .venv/bin/activate
    pip install fastapi uvicorn pydantic numpy matplotlib requests
    python book_obi_service.py
"""

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import List, Tuple, Dict, Deque
from fastapi import FastAPI, HTTPException, Response, Request
import os
import io
import time
import numpy as np
import requests
import redis
import matplotlib
matplotlib.use("Agg")  # Headless
import matplotlib.pyplot as plt

# Configuration
OBI_WINDOW_LEVELS = int(os.getenv("OBI_WINDOW_LEVELS", "5"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.25"))
SUSTAIN_MS = int(os.getenv("OBI_SUSTAIN_MS", "1200"))
RING_SECONDS = int(os.getenv("RING_SECONDS", "600"))
NOTIFY_URL = os.getenv("NOTIFY_URL", "http://127.0.0.1:8088/notify").strip()
REDIS_HOST = os.getenv("REDIS_HOST", "redis").strip()
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

app = FastAPI(title="OBI Service", version="7.1.1")

# Redis connection
redis_client = None

def get_redis():
    """Get or create Redis connection with retry logic for loading state."""
    global redis_client
    if redis_client is None:
        max_retries = 40  # Достаточно для загрузки больших AOF файлов (до 80 секунд)
        retry_delay = 2.0
        
        for attempt in range(max_retries):
            try:
                redis_client = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    decode_responses=True,  # Decode to strings for compatibility
                    socket_connect_timeout=30,
                    socket_timeout=120,
                )
                redis_client.ping()
                print(f"✅ Redis connection established: {REDIS_HOST}:{REDIS_PORT}")
                break

            except Exception as e:
                error_str = str(e).lower()

                # Check for various loading/connection errors
                is_loading_error = (
                    "loading the dataset in memory" in error_str or
                    "busy loading" in error_str or
                    "redis is loading" in error_str
                )

                is_recursion_error = (
                    "maximum recursion depth" in error_str or
                    "recursion" in error_str
                )

                if is_recursion_error:
                    print(f"❌ Recursion detected while connecting to Redis: {e}")
                    print("   Try restarting Redis or check configuration")
                    redis_client = None
                    raise

                if attempt < max_retries - 1:
                    # Exponential backoff for all retryable errors
                    delay = min(retry_delay * 1.2, 10.0)

                    if is_loading_error:
                        print(f"⚠️  Redis is loading dataset (attempt {attempt + 1}/{max_retries}): {e}")
                    else:
                        print(f"⚠️  Redis connection error (attempt {attempt + 1}/{max_retries}): {e}")

                    print(f"   Retrying in {delay:.1f}s...")
                    time.sleep(delay)
                    redis_client = None  # Reset for next attempt
                else:
                    if is_loading_error:
                        print(f"❌ Redis still loading after {max_retries} attempts")
                    else:
                        print(f"❌ Failed to connect to Redis after {max_retries} attempts: {e}")
                    redis_client = None
                    raise
    return redis_client


@dataclass(slots=True)
class BookPayload:
    """Book snapshot from MT5."""
    ts: int  # milliseconds
    symbol: str
    bids: List[Tuple[float, float]]  # [[price, vol], ...]
    asks: List[Tuple[float, float]]  # [[price, vol], ...]


@dataclass
class BookPoint:
    """OBI metrics for a single snapshot."""
    ts: float  # seconds
    obi_signed: float
    obi_ratio: float
    bid_sum: float
    ask_sum: float


@dataclass
class OBIEvent:
    """OBI event (sustained above threshold)."""
    ts: float  # seconds
    symbol: str
    kind: str  # obi_sustain_up | obi_sustain_down
    duration_ms: int
    value: float


# Storage
books: Dict[str, Deque[BookPoint]] = defaultdict(lambda: deque(maxlen=5000))
events: Dict[str, Deque[OBIEvent]] = defaultdict(lambda: deque(maxlen=500))
sustain: Dict[str, dict] = defaultdict(lambda: {"dir": 0, "since": None, "last": 0.0})

# Counters for logging
tick_counter = 0
book_counter = 0
TICK_LOG_INTERVAL = 10000  # Log every 10,000th tick
BOOK_LOG_INTERVAL = 10000  # Log every 10,000th book snapshot


def obi_metrics(bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], k: int):
    """Calculate OBI metrics from top k levels."""
    b = np.array(bids[:k], dtype=float) if bids else np.zeros((0, 2))
    a = np.array(asks[:k], dtype=float) if asks else np.zeros((0, 2))
    
    bid_sum = float(b[:, 1].sum()) if b.size else 0.0
    ask_sum = float(a[:, 1].sum()) if a.size else 0.0
    
    total = bid_sum + ask_sum
    if total > 0:
        signed = (ask_sum - bid_sum) / total
    else:
        signed = 0.0
    
    if bid_sum > 0:
        ratio = (ask_sum / bid_sum) - 1.0
    elif ask_sum > 0:
        ratio = float("inf")
    else:
        ratio = 0.0
    
    return signed, ratio, bid_sum, ask_sum


def prune(symbol: str):
    """Remove old data points."""
    now = time.time()
    dq = books[symbol]
    while dq and (now - dq[0].ts) > RING_SECONDS:
        dq.popleft()


@app.post("/book")
def post_book(p: BookPayload):
    """Receive book snapshot and detect OBI events."""
    global book_counter
    
    s, r, bs, as_ = obi_metrics(p.bids, p.asks, OBI_WINDOW_LEVELS)
    pt = BookPoint(p.ts / 1000.0, s, r, bs, as_)
    symbol = p.symbol
    
    books[symbol].append(pt)
    prune(symbol)
    
    # Log every Nth book snapshot
    book_counter += 1
    if book_counter % BOOK_LOG_INTERVAL == 0:
        print(f"📖 Book #{book_counter}: {symbol} OBI={s:.3f} bid_sum={bs:.2f} ask_sum={as_:.2f}")
    
    # Sustain logic
    st = sustain[symbol]
    st["last"] = s
    dir_now = 1 if s >= OBI_THRESHOLD else (-1 if s <= -OBI_THRESHOLD else 0)
    
    if dir_now == 0:
        st["dir"] = 0
        st["since"] = None
    else:
        if st["dir"] != dir_now:
            st["dir"] = dir_now
            st["since"] = p.ts
        else:
            if st["since"] is not None and (p.ts - st["since"]) >= SUSTAIN_MS:
                kind = "obi_sustain_up" if dir_now > 0 else "obi_sustain_down"
                ev = OBIEvent(pt.ts, symbol, kind, int(p.ts - st["since"]), s)
                events[symbol].append(ev)
                st["since"] = p.ts
                
                # Log OBI event
                print(f"⚡ OBI EVENT: {symbol} {kind} OBI={s:.3f} duration={int(p.ts - st['since'])}ms")
                
                # Notify Go gateway
                try:
                    resp = requests.post(NOTIFY_URL, json={
                        "ts": int(ev.ts * 1000),
                        "symbol": symbol,
                        "type": ev.kind,
                        "duration_ms": ev.duration_ms,
                        "obi": ev.value,
                        "threshold": OBI_THRESHOLD
                    }, timeout=1.2)
                    if resp.status_code == 200:
                        print(f"✅ Notified gateway: {kind}")
                except Exception as e:
                    print(f"⚠️  Failed to notify gateway: {e}")
    
    return {"ok": True, "obi": s}


@app.get("/features/obi")
def get_obi(symbol: str, last: int = 200):
    """Get OBI history."""
    dq = books.get(symbol)
    if not dq:
        raise HTTPException(404, "no data")
    
    arr = list(dq)[-last:]
    return {
        "symbol": symbol,
        "count": len(arr),
        "points": [{
            "ts": p.ts,
            "obi_signed": p.obi_signed,
            "obi_ratio": p.obi_ratio,
            "bid_sum": p.bid_sum,
            "ask_sum": p.ask_sum
        } for p in arr],
        "threshold": OBI_THRESHOLD,
        "window_levels": OBI_WINDOW_LEVELS,
    }


@app.get("/events/pull")
def pull(symbol: str, last: int = 50):
    """Get recent OBI events."""
    arr = list(events.get(symbol, []))[-last:]
    return {
        "symbol": symbol,
        "events": [{
            "ts": e.ts,
            "kind": e.kind,
            "duration_ms": e.duration_ms,
            "obi": e.value
        } for e in arr]
    }


def png(fig) -> Response:
    """Convert matplotlib figure to PNG response."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=100)
    plt.close(fig)
    buf.seek(0)
    return Response(content=buf.getvalue(), media_type="image/png")


@app.get("/render/obi.png")
def png_obi(symbol: str, last: int = 300):
    """Render OBI timeline as PNG."""
    dq = books.get(symbol)
    if not dq:
        raise HTTPException(404, "no data")
    
    arr = list(dq)[-last:]
    x = [p.ts for p in arr]
    y = [p.obi_signed for p in arr]
    thr = OBI_THRESHOLD
    
    fig = plt.figure(figsize=(9, 3))
    ax = fig.add_subplot(111)
    ax.plot(x, y, linewidth=1.0, color='blue', alpha=0.7)
    ax.axhline(+thr, linestyle="--", color='green', alpha=0.5)
    ax.axhline(-thr, linestyle="--", color='red', alpha=0.5)
    ax.axhline(0, linewidth=0.8, color='gray', alpha=0.3)
    ax.set_ylim(-1, 1)
    ax.set_title(f"{symbol} OBI (±{thr})")
    ax.set_xlabel("Time")
    ax.set_ylabel("OBI")
    ax.grid(alpha=0.3)
    
    return png(fig)


@app.get("/render/depth.png")
def png_depth(symbol: str):
    """Render depth profile as PNG."""
    dq = books.get(symbol)
    if not dq:
        raise HTTPException(404, "no data")
    
    last = dq[-1]
    
    fig = plt.figure(figsize=(6, 4))
    ax = fig.add_subplot(111)
    ax.barh([0], [last.bid_sum], color='green', alpha=0.7)
    ax.barh([0], [-last.ask_sum], color='red', alpha=0.7)
    ax.set_yticks([])
    ax.set_xlabel("Volume")
    ax.set_title(f"{symbol} Depth @k={OBI_WINDOW_LEVELS}")
    
    return png(fig)


@app.get("/healthz")
def healthz():
    """Health check."""
    return {
        "ok": True,
        "service": "obi-service",
        "symbols": list(books.keys()),
        "points": sum(len(dq) for dq in books.values()),
        "events": sum(len(dq) for dq in events.values())
    }


if __name__ == "__main__":
    import uvicorn
    import logging
    
    # Get port from environment
    port = int(os.getenv("PORT", "8088"))
    
    # Configure logging - show INFO level for errors and important events
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    
    print(f"🚀 OBI Service starting on port {port}...")
    print(f"   OBI Window: {OBI_WINDOW_LEVELS} levels")
    print(f"   OBI Threshold: ±{OBI_THRESHOLD}")
    print(f"   Sustain Duration: {SUSTAIN_MS}ms")
    print(f"   History: {RING_SECONDS}s ({RING_SECONDS // 60} min)")
    print(f"   Notify URL: {NOTIFY_URL}")
    print(f"   Book Log Interval: Every {TICK_LOG_INTERVAL} snapshots")
    print()
    print("📊 Endpoints:")
    print("   POST /book - Receive book snapshots (logged every 10000)")
    print(f"   POST /tick - Receive tick data (logged every {TICK_LOG_INTERVAL})")
    print("   GET /features/obi - OBI history")
    print("   GET /events/pull - OBI events")
    print("   GET /render/obi.png - OBI timeline PNG")
    print("   GET /render/depth.png - Depth profile PNG")
    print()
    
    uvicorn.run(
        "book_obi_service:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        access_log=False,  # Disable access logs (will log manually for important events)
        log_level="info"  # Show info level logs
    )


@app.post("/tick")
async def receive_tick(request: Request):
    """
    Receive tick data from MT5 TickBridge
    
    Expected format:
    {
        "ts": 1761588727889,
        "bid": 3992.17,
        "ask": 3992.37,
        "last": 0.0,
        "volume": 0.0,
        "flags": 6,
        "symbol": "XAUUSD"
    }
    """
    global tick_counter
    
    try:
        # Get raw body
        body = await request.body()
        
        # Decode and clean (remove null terminators, extra whitespace)
        text = body.decode('utf-8').strip('\x00').strip()
        
        # Find the first valid JSON object (in case there's garbage after)
        # Look for closing brace
        brace_count = 0
        json_end = -1
        for i, char in enumerate(text):
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    json_end = i + 1
                    break
        
        if json_end > 0:
            text = text[:json_end]
        
        # Parse JSON
        import json as json_lib
        data = json_lib.loads(text)
        
        # Write to Redis stream
        symbol = data.get("symbol", "UNKNOWN")
        stream_name = f"stream:tick_{symbol}"
        
        try:
            r = get_redis()
            if r is not None:
                # Write data as strings (decode_responses=True handles encoding)
                redis_data = {k: str(v) for k, v in data.items()}
                r.xadd(stream_name, redis_data, maxlen=10000)
                
                # Increment counter and log every 10,000th tick
                tick_counter += 1
                if tick_counter % TICK_LOG_INTERVAL == 0:
                    print(f"📊 Tick #{tick_counter}: {symbol} bid={data.get('bid')} ask={data.get('ask')} → {stream_name}")
            else:
                tick_counter += 1
                if tick_counter % TICK_LOG_INTERVAL == 0:
                    print(f"⚠️  Tick #{tick_counter}: {symbol} (Redis unavailable)")
        except Exception as redis_err:
            # Don't fail the request if Redis is down
            print(f"⚠️  Failed to write tick to Redis: {redis_err}")
        
        return {"ok": True, "received": symbol}
        
    except Exception as e:
        # Log errors always
        print(f"⚠️  Failed to parse tick JSON: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")
