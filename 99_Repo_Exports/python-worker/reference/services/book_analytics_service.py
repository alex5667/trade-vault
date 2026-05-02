#!/usr/bin/env python3
"""
Book Analytics Service - Advanced OBI analysis with events & PNG rendering.

Provides real-time OBI metrics, event detection, and live visualizations.
Complements existing tick_ingest_server.py /book endpoint.

Features:
    - OBI metrics calculation with historical ringbuffer
    - Event detection (sustained OBI above threshold)
    - PNG rendering (depth profile, OBI timeline) for Telegram
    - Push notifications for OBI events

Endpoints:
    POST /book - Receive book snapshot from MT5 BookBridge
    GET /features/obi?symbol=XAUUSD&last=200 - Get OBI history
    GET /metrics/obi?symbol=XAUUSD - Get latest OBI metrics
    GET /events/pull?symbol=XAUUSD - Get recent OBI events
    GET /render/obi.png?symbol=XAUUSD - PNG: OBI timeline
    GET /render/depth.png?symbol=XAUUSD - PNG: Depth profile
    GET /healthz - Health check

Usage:
    python3 -m services.book_analytics_service
    # Or:
    uvicorn services.book_analytics_service:app --host 127.0.0.1 --port 8090
"""

from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from typing import List, Tuple, Dict, Deque
import time
import os
import io

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
import numpy as np
import requests

# Matplotlib для PNG рендеров
import matplotlib
matplotlib.use("Agg")  # Headless backend
import matplotlib.pyplot as plt


# ═══════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════

OBI_WINDOW_LEVELS = int(os.getenv("OBI_WINDOW_LEVELS", "5"))
OBI_THRESHOLD = float(os.getenv("OBI_THRESHOLD", "0.25"))
SUSTAIN_MS = int(os.getenv("OBI_SUSTAIN_MS", "1200"))  # 1.2s sustained
RING_SECONDS = int(os.getenv("OBI_RING_SECONDS", "600"))  # 10 min history
NOTIFY_URL = os.getenv("NOTIFY_URL", "").strip()  # Push notifications endpoint
PORT = int(os.getenv("BOOK_ANALYTICS_PORT", "8090"))
BOOK_MAX_LEVELS = int(os.getenv("BOOK_MAX_LEVELS", "20"))
MARKET = os.getenv("BOOK_MARKET", "FOREX").upper()
_crypto_mode_env = os.getenv("CRYPTO_MODE")
if _crypto_mode_env is None:
    CRYPTO_MODE_ENABLED = MARKET in {"USDT-M", "CRYPTO"}
else:
    CRYPTO_MODE_ENABLED = _crypto_mode_env.lower() in {"1", "true", "yes", "on"}
CRYPTO_SOURCE_WHITELIST = {
    src.strip().lower()
    for src in os.getenv("BOOK_CRYPTO_SOURCES", "binance-futures,binance-futures-testnet").split(",")
    if src.strip()
}
CRYPTO_MARKETS = {"USDT-M", "CRYPTO"}
OBI_DEPTH = int(os.getenv("OBI_DEPTH", "5"))


# ═══════════════════════════════════════════════════════════════
# Data models
# ═══════════════════════════════════════════════════════════════

class BookPayload(BaseModel):
    """Book snapshot from MT5 BookBridge."""
    ts: int  # milliseconds
    symbol: str
    bids: List[Tuple[float, float]]  # [[price, vol], ...], best first
    asks: List[Tuple[float, float]]  # [[price, vol], ...], best first

    class Config:
        extra = "allow"


@dataclass
class BookPoint:
    """OBI metrics for a single book snapshot."""
    ts: float  # seconds
    obi_signed: float  # (ask - bid) / (ask + bid)
    obi_ratio: float  # (ask / bid) - 1
    bid_sum: float
    ask_sum: float
    spread: float
    mid: float


@dataclass
class OBIEvent:
    """OBI event (sustained above threshold)."""
    ts: float  # seconds
    symbol: str
    kind: str  # "obi_sustain_up" | "obi_sustain_down"
    duration_ms: int
    value: float  # OBI value


# ═══════════════════════════════════════════════════════════════
# Storage
# ═══════════════════════════════════════════════════════════════

# Historical data per symbol
books: Dict[str, Deque[BookPoint]] = defaultdict(
    lambda: deque(maxlen=5000)
)

# OBI events per symbol
events: Dict[str, Deque[OBIEvent]] = defaultdict(
    lambda: deque(maxlen=500)
)

# Sustain state tracking
state_sustain: Dict[str, dict] = defaultdict(
    lambda: {"dir": 0, "since": None, "last_value": 0.0}
)


# ═══════════════════════════════════════════════════════════════
# OBI calculation
# ═══════════════════════════════════════════════════════════════

# ✅ GPU Support: lazy initialization
_gpu_service_cache = None

def _get_gpu_service():
    """Получить GPU сервис (lazy initialization)"""
    global _gpu_service_cache
    if _gpu_service_cache is None:
        try:
            from services.gpu_compute_service import get_gpu_service
            _gpu_service_cache = get_gpu_service()
        except Exception:
            _gpu_service_cache = None
    return _gpu_service_cache

def calculate_obi_metrics(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
    depth: int
) -> Tuple[float, float, float, float, float, float]:
    """
    Calculate OBI metrics from order book with GPU acceleration.
    
    Args:
        bids: List of (price, volume) tuples
        asks: List of (price, volume) tuples
        depth: Number of levels to consider
        
    Returns:
        (obi_signed, obi_ratio, bid_sum, ask_sum, spread, mid)
    """
    # Convert to arrays
    b = np.array(bids[:depth], dtype=float) if bids else np.zeros((0, 2))
    a = np.array(asks[:depth], dtype=float) if asks else np.zeros((0, 2))
    
    # Sum volumes
    bid_sum = float(b[:, 1].sum()) if b.size else 0.0
    ask_sum = float(a[:, 1].sum()) if a.size else 0.0
    
    # ✅ GPU Support: используем GPU для вычисления OBI метрик
    gpu_service = _get_gpu_service()
    if gpu_service and gpu_service.is_gpu_available():
        try:
            bid_vol_arr = np.array([bid_sum], dtype=np.float32)
            ask_vol_arr = np.array([ask_sum], dtype=np.float32)
            obi_results = gpu_service.compute_obi_metrics_batch(bid_vol_arr, ask_vol_arr)
            obi_signed = float(obi_results['obi_signed'][0])
            obi_ratio = float(obi_results['obi_ratio'][0])
            if np.isinf(obi_ratio):
                obi_ratio = float("inf") if ask_sum > 0 else 0.0
        except Exception:
            # Fallback to CPU
            total = bid_sum + ask_sum
            obi_signed = (ask_sum - bid_sum) / total if total > 0 else 0.0
            obi_ratio = (ask_sum / bid_sum) - 1.0 if bid_sum > 0 else (float("inf") if ask_sum > 0 else 0.0)
    else:
        # CPU fallback
        total = bid_sum + ask_sum
        obi_signed = (ask_sum - bid_sum) / total if total > 0 else 0.0
        obi_ratio = (ask_sum / bid_sum) - 1.0 if bid_sum > 0 else (float("inf") if ask_sum > 0 else 0.0)
    
    # Spread and mid
    if bids and asks:
        best_bid = bids[0][0]
        best_ask = asks[0][0]
        spread = best_ask - best_bid
        mid = (best_bid + best_ask) / 2.0
    else:
        spread = 0.0
        mid = 0.0
    
    return obi_signed, obi_ratio, bid_sum, ask_sum, spread, mid


def _to_float_levels(levels: List, depth: int) -> List[Tuple[float, float]]:
    """Приводит уровни книги к списку (price, volume) с ограничением по глубине."""
    if not isinstance(levels, list):
        return []

    converted: List[Tuple[float, float]] = []
    for raw in levels[:depth]:
        if not isinstance(raw, (list, tuple)) or len(raw) < 2:
            continue
        try:
            price = float(raw[0])
            volume = float(raw[1])
        except (TypeError, ValueError):
            continue
        converted.append((price, volume))
    return converted


def prune_old(symbol: str) -> None:
    """Remove old data points beyond RING_SECONDS."""
    now = time.time()
    dq = books[symbol]
    
    while dq and (now - dq[0].ts) > RING_SECONDS:
        dq.popleft()


# ═══════════════════════════════════════════════════════════════
# FastAPI app
# ═══════════════════════════════════════════════════════════════

app = FastAPI(
    title="Book Analytics Service",
    description="Advanced OBI analysis and monitoring",
    version="7.1.0",
),


def notify_event(event: OBIEvent) -> None:
    """Send event notification to external service."""
    if not NOTIFY_URL:
        return
    
    try:
        requests.post(
            NOTIFY_URL,
            json={
                "ts": int(event.ts * 1000),
                "type": event.kind,
                "symbol": event.symbol,
                "duration_ms": event.duration_ms,
                "obi": event.value,
                "threshold": OBI_THRESHOLD,
            },
            timeout=1.5
        )
    except Exception as e:
        print(f"⚠️  Failed to send notification: {e}")


@app.post("/book")
def receive_book(payload: BookPayload):
    """
    Receive book snapshot and detect OBI events.
    
    Replaces /book/analyze for compatibility with MT5 BookBridge.
    
    Args:
        payload: Book snapshot from MT5
        
    Returns:
        Status and metrics
    """
    payload_dict = payload.dict()
    raw_bids = payload_dict.get("bids", [])
    raw_asks = payload_dict.get("asks", [])
    source = str(payload_dict.get("source", "")).lower()
    market = str(payload_dict.get("market", "")).upper()
    is_crypto_payload = (
        CRYPTO_MODE_ENABLED
        or market in CRYPTO_MARKETS
        or source in CRYPTO_SOURCE_WHITELIST
    )

    depth_limit = OBI_DEPTH if is_crypto_payload else BOOK_MAX_LEVELS

    bids = _to_float_levels(raw_bids, depth_limit)
    asks = _to_float_levels(raw_asks, depth_limit)
    depth_cfg = OBI_DEPTH if is_crypto_payload else OBI_WINDOW_LEVELS

    obi_s, obi_r, bid_sum, ask_sum, spread, mid = calculate_obi_metrics(
        bids,
        asks,
        depth_cfg
    )
    
    point = BookPoint(
        ts=payload.ts / 1000.0,
        obi_signed=obi_s,
        obi_ratio=obi_r,
        bid_sum=bid_sum,
        ask_sum=ask_sum,
        spread=spread,
        mid=mid
    )
    
    symbol = payload.symbol
    books[symbol].append(point)
    prune_old(symbol)
    
    # === Sustain logic ===
    st = state_sustain[symbol]
    st["last_value"] = obi_s
    
    # Direction: up if OBI >= threshold, down if OBI <= -threshold
    dir_now = 1 if obi_s >= OBI_THRESHOLD else (-1 if obi_s <= -OBI_THRESHOLD else 0)
    now_ms = payload.ts
    
    if dir_now == 0:
        # Reset sustain state
        st["dir"] = 0
        st["since"] = None
    else:
        if st["dir"] != dir_now:
            # Direction changed
            st["dir"] = dir_now
            st["since"] = now_ms
        else:
            # Same direction, check duration
            if st["since"] is not None and (now_ms - st["since"]) >= SUSTAIN_MS:
                kind = "obi_sustain_up" if dir_now > 0 else "obi_sustain_down"
                
                event = OBIEvent(
                    ts=now_ms / 1000.0,
                    symbol=symbol,
                    kind=kind,
                    duration_ms=int(now_ms - st["since"]),
                    value=obi_s
                )
                
                events[symbol].append(event)
                notify_event(event)
                
                # Reset to avoid spam
                st["since"] = now_ms
    
    return {
        "ok": True,
        "symbol": symbol,
        "metrics": asdict(point)
    }


@app.get("/features/obi")
def get_obi_features(symbol: str, last: int = 200):
    """
    Get OBI feature history.
    
    Args:
        symbol: Symbol name
        last: Number of recent points
        
    Returns:
        Historical OBI metrics
    """
    dq = books.get(symbol)
    if not dq:
        raise HTTPException(404, f"No data for {symbol}")
    
    arr = list(dq)[-last:]
    
    return {
        "symbol": symbol,
        "count": len(arr),
        "points": [asdict(p) for p in arr],
        "config": {
            "threshold": OBI_THRESHOLD,
            "window_levels": OBI_WINDOW_LEVELS,
            "ring_seconds": RING_SECONDS
        }
    }


@app.get("/metrics/obi")
def get_latest_obi(symbol: str):
    """
    Get latest OBI metrics.
    
    Args:
        symbol: Symbol name
        
    Returns:
        Latest metrics and aggregates
    """
    dq = books.get(symbol)
    if not dq:
        raise HTTPException(404, f"No data for {symbol}")
    
    latest = dq[-1]
    
    # Calculate moving averages
    recent = list(dq)[-60:]  # Last 60 points
    
    # ✅ GPU Support: используем GPU для вычисления mean и std
    gpu_service = _get_gpu_service()
    if gpu_service and gpu_service.is_gpu_available() and len(recent) > 0:
        try:
            obi_values = np.array([p.obi_signed for p in recent], dtype=np.float32)
            # Используем GPU для mean и std
            if gpu_service.use_gpu:
                import cupy as cp
                obi_gpu = cp.asarray(obi_values)
                avg_obi = float(cp.mean(obi_gpu))
                std_obi = float(cp.std(obi_gpu))
            else:
                avg_obi = float(np.mean(obi_values))
                std_obi = float(np.std(obi_values))
        except Exception:
            # Fallback to CPU
            avg_obi = np.mean([p.obi_signed for p in recent])
            std_obi = np.std([p.obi_signed for p in recent])
    else:
        # CPU fallback
        avg_obi = np.mean([p.obi_signed for p in recent])
        std_obi = np.std([p.obi_signed for p in recent])
    
    # Check if sustained
    sustained = abs(avg_obi) >= OBI_THRESHOLD
    
    return {
        "symbol": symbol,
        "latest": asdict(latest),
        "recent_60": {
            "avg_obi": float(avg_obi),
            "std_obi": float(std_obi),
            "sustained": sustained
        },
        "total_points": len(dq)
    }


@app.get("/events/pull")
def pull_events(symbol: str, last: int = 50):
    """
    Get recent OBI events.
    
    Args:
        symbol: Symbol name
        last: Number of recent events
        
    Returns:
        List of events
    """
    arr = list(events.get(symbol, []))[-last:]
    
    return {
        "symbol": symbol,
        "count": len(arr),
        "events": [{
            "ts": e.ts,
            "kind": e.kind,
            "duration_ms": e.duration_ms,
            "obi": e.value
        } for e in arr]
    }


@app.get("/render/obi.png")
def render_obi_timeline(symbol: str, last: int = 300):
    """
    Render OBI timeline as PNG (for Telegram).
    
    Args:
        symbol: Symbol name
        last: Number of points to show
        
    Returns:
        PNG image
    """
    dq = books.get(symbol)
    if not dq:
        raise HTTPException(404, f"No data for {symbol}")
    
    arr = list(dq)[-last:]
    x = [p.ts for p in arr]
    y = [p.obi_signed for p in arr]
    
    fig = plt.figure(figsize=(9, 3))
    ax = fig.add_subplot(111)
    
    # Plot OBI
    ax.plot(x, y, linewidth=1.0, color='blue', alpha=0.7)
    
    # Threshold lines
    ax.axhline(OBI_THRESHOLD, linestyle="--", color='green', alpha=0.5, label=f'+{OBI_THRESHOLD}')
    ax.axhline(-OBI_THRESHOLD, linestyle="--", color='red', alpha=0.5, label=f'-{OBI_THRESHOLD}')
    ax.axhline(0, linewidth=0.8, color='gray', alpha=0.3)
    
    # Formatting
    ax.set_title(f"{symbol} OBI Timeline (±{OBI_THRESHOLD})")
    ax.set_ylim(-1, 1)
    ax.set_xlabel("Time (Unix)")
    ax.set_ylabel("OBI")
    ax.grid(alpha=0.3)
    ax.legend()
    
    # Convert to PNG
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=100)
    plt.close(fig)
    buf.seek(0)
    
    return Response(content=buf.getvalue(), media_type="image/png")


@app.get("/render/depth.png")
def render_depth_profile(symbol: str):
    """
    Render depth profile as PNG (for Telegram).
    
    Args:
        symbol: Symbol name
        
    Returns:
        PNG image
    """
    dq = books.get(symbol)
    if not dq:
        raise HTTPException(404, f"No data for {symbol}")
    
    last = dq[-1]
    
    active_depth = OBI_DEPTH if CRYPTO_MODE_ENABLED else OBI_WINDOW_LEVELS

    fig = plt.figure(figsize=(6, 4))
    ax = fig.add_subplot(111)
    
    # Bar chart: bids vs asks
    categories = ['Bids', 'Asks']
    values = [last.bid_sum, last.ask_sum]
    colors = ['green', 'red']
    
    ax.bar(categories, values, color=colors, alpha=0.7)
    
    # Formatting
    ax.set_title(f"{symbol} Depth Profile (top {active_depth} levels)")
    ax.set_ylabel("Volume")
    ax.grid(axis='y', alpha=0.3)
    
    # Add values on bars
    for i, (cat, val) in enumerate(zip(categories, values)):
        ax.text(i, val, f'{val:.1f}', ha='center', va='bottom', fontsize=10)
    
    # Convert to PNG
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=100)
    plt.close(fig)
    buf.seek(0)
    
    return Response(content=buf.getvalue(), media_type="image/png")


@app.get("/healthz")
def health():
    """Health check."""
    return {
        "ok": True,
        "symbols": list(books.keys()),
        "total_points": sum(len(dq) for dq in books.values()),
        "total_events": sum(len(dq) for dq in events.values())
    }


if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Book Analytics Service starting on port {PORT}...")
    print(f"   OBI Window: {OBI_WINDOW_LEVELS} levels")
    print(f"   OBI Depth (effective): {OBI_DEPTH} levels")
    print(f"   OBI Threshold: {OBI_THRESHOLD}")
    print(f"   Sustain Duration: {SUSTAIN_MS}ms")
    print(f"   History: {RING_SECONDS}s ({RING_SECONDS // 60} min)")
    print(f"   Notify URL: {NOTIFY_URL or 'disabled'}")
    print(f"   Market: {MARKET}")
    print(f"   Crypto mode: {'enabled' if CRYPTO_MODE_ENABLED else 'disabled'}")
    print()
    print("📊 Endpoints:")
    print(f"   POST /book - Receive book snapshots")
    print(f"   GET /features/obi - OBI history")
    print(f"   GET /metrics/obi - Latest metrics")
    print(f"   GET /events/pull - OBI events")
    print(f"   GET /render/obi.png - OBI timeline PNG")
    print(f"   GET /render/depth.png - Depth profile PNG")
    print()
    
    uvicorn.run(
        "services.book_analytics_service:app",
        host="127.0.0.1",
        port=PORT,
        reload=False
    )

