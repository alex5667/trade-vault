import asyncio
import json

import redis.asyncio as aioredis

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


async def main():
    print("Connecting to Redis...")
    r = aioredis.from_url("redis://localhost:6379", decode_responses=True)

    source = "binance"
    symbol = "BTCUSDT"

    # 1. Clear old locks and timers
    await r.delete(f"report_lock:{source}:{symbol}")
    await r.delete(f"report_last_hourly_hour:{source}:{symbol}")

    # 2. Add fake trade
    now_ms = get_ny_time_millis()
    order_id = f"test-ml-report-{now_ms}"

    sp = {
        "version": 1,
        "sid": "test_sid",
        "rule": {
            "ok": 1,
            "score": 0.85,
            "scenario": "trend_pullback",
            "have": 2,
            "need": 2
        },
        "ml": {
            "state": "allow",
            "p_edge": 0.62
        }
    }

    trade_data = {
        "id": order_id,
        "order_id": order_id,
        "symbol": "BTCUSDT",
        "source": "binance",
        "strategy": "cryptoorderflow",
        "status": "closed",
        "side": "LONG",
        "pnl_net": "55.5",
        "pnl_gross": "56.0",
        "mfe_pnl": "60.0",
        "close_reason": "TP1",
        "entry_ts_ms": str(now_ms - 60000),
        "exit_ts_ms": str(now_ms),
        "one_r_money": "100.0",
        "risk_amount": "100.0",
        "signal_payload": json.dumps(sp)
    }

    # Save to hash for hydration
    await r.hset(f"order:{order_id}", mapping=trade_data)

    # Force count to threshold
    from services.orderflow.configuration import REPORT_TRIGGER_COUNT
    trigger_count = REPORT_TRIGGER_COUNT if REPORT_TRIGGER_COUNT > 0 else 25

    counter_key = f"report_trade_count:{source}:{symbol}"
    await r.set(counter_key, trigger_count - 1)

    print(f"Set counter to {trigger_count - 1}. Publishing trade to stream to hit threshold {trigger_count}...")

    # Add to stream
    await r.xadd("trades:closed", trade_data)

    print(f"Published fake trade {order_id}. Polling notify stream...")

    # Check stream
    for _ in range(10):
        await asyncio.sleep(1.0)
        entries = await r.xrevrange(RS.NOTIFY_TELEGRAM, "+", "-", 1)
        if entries:
            text = entries[0][1].get("text", "")
            if "Отчет" in text and symbol in text:
                print("\n=== SUCCESS: REPORT GENERATED ===\n")
                if "🤖 ML Performance" in text:
                    print("ML Performance section found!")
                    # Extract the section
                    lines = text.split('\n')
                    in_ml = False
                    for line in lines:
                        if "ML Performance" in line or "ML Condition" in line:
                            in_ml = True
                        elif in_ml and not line.strip():
                            in_ml = False

                        if in_ml:
                            print(line)
                else:
                    print("ML Performance section NOT found!")
                return
    print("Timed out waiting for report.")

asyncio.run(main())
