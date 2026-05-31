import asyncio
import os
import sys
from collections import defaultdict

import numpy as np
import redis.asyncio as aioredis

from core.microbar_streams import read_microbars

sys.path.append("/app")

SYMBOLS = ["BTCUSDT", "ETHUSDT"]

async def main():
    r = aioredis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

    # Fetch per-symbol microbars (split-streams aware)
    count_per = int(os.getenv("MICROBAR_READ_COUNT", "10000"))

    # Dedup by ts_ms
    bars = defaultdict(dict)  # sym -> {ts_ms -> bar}
    for sym in SYMBOLS:
        try:
            mbs = await read_microbars(r, sym=sym, count=count_per, reverse=False)
        except Exception:
            mbs = []
        for pld in mbs:
            try:
                s = (pld.get("symbol") or sym)
                if s not in SYMBOLS:
                    continue
                ts = int(pld.get("ts_ms") or 0)
                if ts and ts not in bars[s]:
                    bars[s][ts] = pld
            except Exception:
                continue



    # Calc
    for sym in SYMBOLS:
        sorted_bars = sorted(bars[sym].values(), key=lambda x: x["ts_ms"])
        deltas = []
        for i in range(1, len(sorted_bars)):
            curr = sorted_bars[i]
            prev = sorted_bars[i-1]
            d = curr["cvd"] - prev["cvd"]
            val = abs(d) * curr["close"]
            deltas.append(val)

        print(f"--- {sym} ---")
        if deltas:
            p50 = np.percentile(deltas, 50)
            p80 = np.percentile(deltas, 80)
            p95 = np.percentile(deltas, 95)
            print(f"delta_usd_p50: {p50:.0f}")
            print(f"delta_usd_p80: {p80:.0f}")
            print(f"delta_usd_p95: {p95:.0f}")
            print(f"count: {len(deltas)}")
        else:
            print("No data")

        # Inst rate
        try:
            # Quick estimate from last ticks
            tk = await r.xrevrange(f"stream:tick_{sym}", count=5000)
            if len(tk) > 10:
                bins = defaultdict(int)
                for i in tk:
                    ts = int(i[0].split('-')[0])
                    bins[ts//1000] += 1
                rates = list(bins.values())
                print(f"rate_p10: {np.percentile(rates, 10):.1f}")
                print(f"rate_p50: {np.percentile(rates, 50):.1f}")
                print(f"rate_n: {len(rates)}")
        except Exception: pass

    await r.close()

if __name__ == "__main__":
    asyncio.run(main())
