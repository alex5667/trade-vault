from __future__ import annotations

import os
import time
import redis

from core.atr_source_selector_v2 import ATRSourceSelector


def main() -> None:
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(url, decode_responses=False)
    sel = ATRSourceSelector(r)

    period = int(os.getenv("ATR_SELECTOR_PERIOD_SEC", "300"))
    price_prefix = os.getenv("ATR_SELECTOR_PRICE_KEY_PREFIX", "cfg:last_px:")
    sym_set = os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols")

    while True:
        t0 = time.time()
        if os.getenv("ATR_SELECTOR_ENABLE", "0") == "1":
            try:
                syms = list(r.smembers(sym_set) or [])
                for s in syms:
                    sym = s.decode("utf-8", "ignore") if isinstance(s, bytes) else str(s)
                    px_raw = r.get(price_prefix + sym)
                    try:
                        px = float(px_raw.decode("utf-8", "ignore") if isinstance(px_raw, bytes) else px_raw or 0.0)
                    except Exception:
                        px = 0.0
                    if px > 0:
                        sel.select(sym, px=px)
            except Exception:
                pass
        dt = time.time() - t0
        time.sleep(max(1, period - int(dt)))


if __name__ == "__main__":
    main()

