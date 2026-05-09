import asyncio
import json

import redis.asyncio as aioredis

from utils.time_utils import get_ny_time_millis


async def main():
    print("Connecting to Redis...")
    r = aioredis.from_url("redis://localhost:6379", decode_responses=True)

    # 1. Create a fake closed trade with ML payload
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

    # Add to stream to trigger Reporter
    await r.xadd("trades:closed", trade_data)
    print(f"Published fake trade {order_id} to trades:closed stream.")

    # Also directly set the report counter to trigger it
    source = "binance"
    symbol = "BTCUSDT"
    counter_key = f"report_trade_count:{source}:{symbol}"

    current = await r.incr(counter_key)
    print(f"Current report trade count for {symbol}: {current}")

    if current < 25:
        await r.set(counter_key, 25)
        print("Forced trade count to 25 to trigger the report.")

    # Clear lock if exists
    await r.delete(f"report_lock:{source}:{symbol}")

    print("Done. Check Telegram or notify logs soon.")

asyncio.run(main())
