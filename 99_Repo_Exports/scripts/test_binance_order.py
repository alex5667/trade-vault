#!/usr/bin/env python3
"""Test-send script for Binance executor (testnet).

Usage (from repo root):
  REDIS_URL=redis://127.0.0.1:6379/0 python scripts/test_binance_order.py

What it does:
  1. Pushes a MARKET BUY order to orders:queue:binance
  2. Waits up to 30s for a result in orders:exec stream
  3. Prints PASS / FAIL with the exec event details

Prerequisites:
  - Redis running locally or docker compose up
  - BINANCE_API_KEY / BINANCE_API_SECRET + BINANCE_FUTURES_BASE_URL in env
  - pip install redis
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
QUEUE     = os.getenv("ORDERS_QUEUE_BINANCE", "orders:queue:binance")
EXEC_STREAM = os.getenv("EXEC_STREAM", "orders:exec")

# --- test order params ---
SYMBOL  = os.getenv("TEST_SYMBOL", "BTCUSDT")
QTY     = float(os.getenv("TEST_QTY", "0.001"))    # ~90 USDT notional on testnet
SL_PCT  = float(os.getenv("TEST_SL_PCT", "0.02"))  # 2% below current price (approx)
WAIT_S  = int(os.getenv("TEST_WAIT_S", "30"))

try:
    import redis
except ImportError:
    print("❌ redis-py not installed: pip install redis")
    sys.exit(1)


def push_order(r: redis.Redis, sid: str) -> str:
    payload = {
        "action":    "open",
        "sid":       sid,
        "symbol":    SYMBOL,
        "side":      "BUY",
        "qty":       QTY,
        "type":      "MARKET",
        # SL at a large distance on testnet — just to test order placement
        "sl":        1.0,              # intentionally far: won't trigger
        "tp_levels": [9_999_999.0],   # intentionally far: won't trigger
    }
    raw = json.dumps(payload)
    r.rpush(QUEUE, raw)
    print(f"📤 Queued order → {QUEUE}")
    print(f"   sid={sid}")
    print(f"   {payload}")
    return raw


def wait_for_result(r: redis.Redis, sid: str, timeout_s: int) -> dict | None:
    """Read orders:exec stream until we see an event for our sid."""
    last_id = "0-0"
    deadline = time.time() + timeout_s
    print(f"\n⏳ Waiting up to {timeout_s}s for exec event in {EXEC_STREAM} ...")
    while time.time() < deadline:
        results = r.xread({EXEC_STREAM: last_id}, count=50, block=1000)
        if not results:
            continue
        for _stream, messages in results:
            for msg_id, fields in messages:
                last_id = msg_id
                if fields.get("sid") == sid:
                    return fields
    return None


def main() -> None:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        r.ping()
    except Exception as e:
        print(f"❌ Cannot connect to Redis at {REDIS_URL}: {e}")
        sys.exit(1)

    print(f"✅ Redis connected: {REDIS_URL}")
    print(f"   Symbol={SYMBOL}  qty={QTY}")
    print()

    sid = f"test-{SYMBOL.lower()}-{uuid.uuid4().hex[:8]}"
    push_order(r, sid)

    result = wait_for_result(r, sid, WAIT_S)

    print()
    if result is None:
        print(f"❌ TIMEOUT: no exec event received for sid={sid} in {WAIT_S}s")
        print("   Possible causes:")
        print("   1. binance-executor not running (docker compose up binance-executor)")
        print("   2. Wrong EXEC_STREAM / ORDERS_QUEUE_BINANCE keys")
        print("   3. Binance API key/secret invalid or wrong base_url")
        sys.exit(1)

    status    = result.get("status", "?")
    severity  = result.get("severity", "")
    action    = result.get("action", "?")
    symbol    = result.get("symbol", "?")
    qty       = result.get("qty", "?")
    price     = result.get("exec_price", "?")
    order_id  = result.get("binance_order_id", "?")
    msg_field = result.get("msg", "")

    if severity == "error":
        print(f"❌ FAIL — executor returned error:")
        print(f"   action={action} symbol={symbol}")
        print(f"   status={status}")
        print(f"   msg={msg_field}")
    else:
        print(f"✅ PASS — order executed on testnet!")
        print(f"   action={action} symbol={symbol}")
        print(f"   status={status}  qty={qty}  exec_price={price}")
        print(f"   binance_order_id={order_id}")

    print()
    print("Raw exec event:")
    for k, v in sorted(result.items()):
        print(f"   {k}: {v}")


if __name__ == "__main__":
    main()
