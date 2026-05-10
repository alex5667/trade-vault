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

    # Force the count threshold for testing
    await r.set(f"report_trade_count:{source}:{symbol}", 24)
    await r.delete(f"report_lock:{source}:{symbol}")

    now_ms = get_ny_time_millis()
    order_id = f"test-ml-report-{now_ms}"

    # 1. Trade with ML
    sp = {
        "version": 1,
        "sid": "test_sid_1",
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
        "exit_ts_ms": str(now_ms),
        "entry_ts_ms": str(now_ms - 60000),
        "signal_payload": json.dumps(sp)
    }

    print(f"Adding trade {order_id} to trades:closed and hash")
    await r.hset(f"order:{order_id}", mapping=trade_data)
    await r.xadd(RS.TRADES_CLOSED, trade_data)

    print("Trade added. Checking notify:telegram stream for 10 seconds...")
    last_id = "$"
    for _ in range(10):
        await asyncio.sleep(1)
        res = await r.xread({RS.NOTIFY_TELEGRAM: last_id}, count=1, block=1)
        if res:
            for stream, messages in res:
                for msg_id, msg in messages:
                    text = msg.get("text", "")
                    if "ML Performance" in text:
                        print("\n=== TELEGRAM REPORT FOUND ===")
                        lines = text.split('\n')
                        in_ml = False
                        for line in lines:
                            if "ML Performance" in line:
                                in_ml = True
                            elif in_ml and (not line.strip() or "---" in line or "Alerts" in line):
                                in_ml = False

                            if in_ml:
                                print(line)
                        return
                    last_id = msg_id
    print("Wait finished. No ML report found.")

asyncio.run(main())
