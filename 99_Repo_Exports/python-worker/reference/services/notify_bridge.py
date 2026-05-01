#!/usr/bin/env python3
"""
Notify Bridge - Receives OBI events and sends to Telegram.

Receives POST /notify from book_analytics_service and directly sends
to Telegram using Bot API (no Redis intermediary).

Endpoints:
    POST /notify - Receive OBI event and send to Telegram
    GET /healthz - Health check

Environment:
    BOT_TOKEN - Telegram bot token (required)
    CHAT_ID - Telegram chat ID (required)
    TITLE_PREFIX - Message title prefix (optional, default: "[XAUUSD]")
    OBI_HOST - OBI service URL (default: http://127.0.0.1:8090)
    NOTIFY_BRIDGE_PORT - Service port (default: 8089)

Usage:
    export BOT_TOKEN=123456:ABC-DEF...
    export CHAT_ID=123456789
    python3 -m services.notify_bridge
    
    # Or with uvicorn:
    uvicorn services.notify_bridge:app --host 127.0.0.1 --port 8089
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import os
import httpx
import datetime as dt

# Configuration
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
TITLE = os.getenv("TITLE_PREFIX", "[XAUUSD]")
OBI_HOST = os.getenv("OBI_HOST", "http://127.0.0.1:8090")
PORT = int(os.getenv("NOTIFY_BRIDGE_PORT", "8089"))

app = FastAPI(title="notify-bridge", version="7.1.1")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"


async def tg_send_text(text: str, parse: str = "HTML") -> bool:
    """
    Send text message to Telegram.
    
    Args:
        text: Message text
        parse: Parse mode (HTML or Markdown)
        
    Returns:
        Success status
    """
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.post(
                f"{TELEGRAM_API}/sendMessage",
                json={
                    "chat_id": CHAT_ID,
                    "text": text,
                    "parse_mode": parse,
                }
            )
            return response.status_code == 200
    except Exception as e:
        print(f"⚠️  Failed to send text: {e}")
        return False


async def tg_send_photo_by_url(url: str, caption: str = "") -> bool:
    """
    Send photo from URL to Telegram.
    
    Args:
        url: Photo URL
        caption: Photo caption
        
    Returns:
        Success status
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            # Download image
            img_response = await client.get(url)
            img_response.raise_for_status()
            
            # Send to Telegram
            response = await client.post(
#                 f"{TELEGRAM_API}/sendPhoto",,
#                 data={
#                     "chat_id": CHAT_ID,
#                     "caption": caption
#                 }
                files={
                    "photo": ("obi.png", img_response.content, "image/png")
                }
            )
            return response.status_code == 200
    except Exception as e:
        print(f"⚠️  Failed to send photo: {e}")
        return False


@app.post("/notify")
async def notify(req: Request):
    """
    Receive OBI event notification and send to Telegram.
    
    Payload:
        ts: int (milliseconds)
        type: str (event type)
        symbol: str
        duration_ms: int
        obi: float
        threshold: float
        
    Returns:
        Status
    """
    payload = await req.json()
    
    t = payload.get("ts", 0)
    symbol = payload.get("symbol", "XAUUSD")
    typ = payload.get("type", "event")
    obi = payload.get("obi", 0.0)
    dur = payload.get("duration_ms", 0)
    thr = payload.get("threshold", 0.0)
    
    # Format timestamp
    when = dt.datetime.utcfromtimestamp(t / 1000).strftime("%H:%M:%S UTC")
    
    # Emoji based on type
    emoji = "🟢⬆️" if "up" in typ else "🔴⬇️"
    
    # Format message
    header = f"{TITLE} {symbol} {typ}"
    message = (
        f"<b>{emoji} {header}</b>\n\n"
        f"OBI: <code>{obi:.3f}</code> (threshold: ±{thr:.2f})\n"
        f"Duration: <b>{dur}ms</b> sustained\n"
        f"Time: {when}"
    )
    
    # Send text message
    text_sent = await tg_send_text(message)
    
    # Send OBI chart
    url = f"{OBI_HOST}/render/obi.png?symbol={symbol}&last=300"
    photo_sent = await tg_send_photo_by_url(url, caption=f"📊 {symbol} OBI Timeline")
    
    return JSONResponse({
        "ok": True,
        "text_sent": text_sent,
        "photo_sent": photo_sent
    })


@app.get("/healthz")
def health():
    """Health check."""
    return {
        "ok": True,
        "service": "notify-bridge",
        "telegram_api": TELEGRAM_API.split(BOT_TOKEN)[0] + "***",
        "chat_id": CHAT_ID
    }


if __name__ == "__main__":
    import uvicorn
    
    print(f"🚀 Notify Bridge starting on port {PORT}...")
    print(f"   Bot Token: {BOT_TOKEN[:10]}***")
    print(f"   Chat ID: {CHAT_ID}")
    print(f"   Title Prefix: {TITLE}")
    print(f"   OBI Host: {OBI_HOST}")
    print()
    print("📊 Endpoints:")
    print(f"   POST /notify - Receive OBI events → Telegram")
    print(f"   GET /healthz - Health check")
    print()
    
    uvicorn.run(
        "services.notify_bridge:app",
        host="127.0.0.1",
        port=PORT,
        reload=False
    )

