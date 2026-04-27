#!/usr/bin/env python3
"""
Notification Receiver - Handles OBI events from book_analytics_service.

Receives POST /notify from book_analytics_service and forwards to Telegram.

Endpoints:
    POST /notify - Receive OBI event notification
    GET /healthz - Health check

Usage:
    python3 -m services.notify_receiver
    # Or:
    uvicorn services.notify_receiver:app --host 127.0.0.1 --port 8089
"""

import os
from fastapi import FastAPI
from dataclasses import dataclass
import redis
from core.redis_keys import RedisStreams as RS

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
PORT = int(os.getenv("NOTIFY_RECEIVER_PORT", "8089"))

app = FastAPI(title="notify-receiver", version="7.1.0")

# Redis client
r = redis.from_url(REDIS_URL, decode_responses=True)


@dataclass(slots=True)
class OBINotification:
    """OBI event notification from book_analytics_service."""
    ts: int  # milliseconds
    type: str  # "obi_sustain_up" | "obi_sustain_down"
    symbol: str
    duration_ms: int
    obi: float
    threshold: float


@app.post("/notify")
def receive_notification(notification: OBINotification):
    """
    Receive OBI event notification and forward to Telegram.
    
    Args:
        notification: OBI event data
        
    Returns:
        Status
    """
    # Format message
    emoji = "🟢⬆️" if "up" in notification.type else "🔴⬇️"
    
    text = (
        f"{emoji} **OBI Event: {notification.symbol}**\n\n"
        f"Type: {notification.type}\n"
        f"OBI: {notification.obi:.3f} (threshold: ±{notification.threshold})\n"
        f"Duration: {notification.duration_ms}ms sustained\n"
        f"Time: {notification.ts}"
    )
    
    # Send to Telegram
    try:
        r.xadd(
            NOTIFY_STREAM,
            {
                "text": text,
                "priority": "normal",
                "source": "obi_events"
            }
        , maxlen=50000)
        
        return {"ok": True, "forwarded": True}
    except Exception as e:
        print(f"⚠️  Failed to send notification: {e}")
        return {"ok": False, "error": str(e)}


@app.get("/healthz")
def health():
    """Health check."""
    return {"ok": True, "service": "notify-receiver"}


if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Notification Receiver starting on port {PORT}...")
    print(f"   Forwarding to: {NOTIFY_STREAM}")
    print()
    
    uvicorn.run(
        "services.notify_receiver:app",
        host="127.0.0.1",
        port=PORT,
        reload=False
    )
