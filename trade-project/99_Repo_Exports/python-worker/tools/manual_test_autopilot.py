from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
manual_test_autopilot.py

Helper script to:
1. Optionally inject fake CLOSED trades into Redis (streams).
2. Run the Autopilot Report pipeline (Export -> Tune -> Report).
3. Print the result.

Fallback: If Redis is unreachable, mocks the interaction to demonstrate logic.
"""

import argparse
import os
import random
import sys
from unittest.mock import MagicMock

# Add parent directory to path to allow imports from services/tools
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import redis
except ImportError:
    redis = None

from services.tm_autopilot_report_service import run_pipeline, send_telegram_report
from tools.export_trade_closed_ndjson import iter_position_closed


def generate_fake_trade(i, now):
    symbols = ["BTCUSDT", "ETHUSDT"]
    regimes = ["trend", "range"]
    scenarios = ["continuation", "reversal"]
    tiers = ["0", "1", "2"]

    ts = now - random.randint(0, 24 * 3600 * 1000)

    sym = random.choice(symbols)
    reg = random.choice(regimes)
    scn = random.choice(scenarios)
    tier = random.choice(tiers)

    # Simulate some logic: Tier 1 in Trend/Continuation is good
    if reg == "trend" and scn == "continuation" and tier == "1":
        r_mult = random.gauss(0.8, 1.5) # Positive bias
    else:
        r_mult = random.gauss(-0.2, 1.5) # Negative bias

    payload = {
        "event_type": "POSITION_CLOSED",
        "event_id": f"fake-{i}-{ts}",
        "sid": f"fake-sid-{i}",
        "symbol": sym,
        "ts": ts,
        "price": 50000 + random.random()*1000,
        "pnl": r_mult * 100,
        "source": "manual_test",
        "regime": reg,
        "scenario": scn,
        "ab_arm": "DEFAULT",
        "ab_group": "default",
        "risk_usd": 100.0,
        "r_mult": r_mult,
        "abs_lvl_tier": tier,
        "dn_tier": tier,
        "of_confirm_ok": "1",
        "book_health_ok": "1",
        "pressure_sps": "5.5",
    }
    return payload, f"{ts}-{i}"

def mock_redis_client(n=500):
    print("Creating MOCK Redis client...")
    r = MagicMock()
    now = get_ny_time_millis()

    # Store fake trades in memory
    fake_data = []
    for i in range(n):
        payload, msg_id = generate_fake_trade(i, now)
        flat = {k: str(v) for k, v in payload.items()}
        fake_data.append((msg_id, flat))

    # Sort by ID descending (xrevrange)
    fake_data.sort(key=lambda x: x[0], reverse=True)

    def side_effect_xrevrange(name, max='+', min='-', count=None):
        start_idx = 0
        if hasattr(r, '_iter_pos'):
             start = r._iter_pos
        else:
             r._iter_pos = 0
             start = 0

        chunk = fake_data[start:start+count] if count else fake_data
        r._iter_pos += len(chunk)
        return chunk

    r.xrevrange.side_effect = side_effect_xrevrange

    def side_effect_xadd(name, fields, **kwargs):
        print(f"[MOCK REDIS] XADD {name}: {str(fields)[:100]}...")
        return "1-0"

    r.xadd.side_effect = side_effect_xadd

    return r

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=RS.EVENTS_TRADES)
    ap.add_argument("--notify-stream", default=RS.NOTIFY_TELEGRAM)
    ap.add_argument("--inject", action="store_true", help="Inject fake data before running")
    ap.add_argument("--out-dir", default="/tmp/tm_autopilot_test")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    r = None
    use_mock = False

    if redis:
        try:
            r = redis.from_url(args.redis, decode_responses=True)
            r.ping()
        except Exception:
            print("Redis unreachable. Switching to MOCK mode.")
            use_mock = True
    else:
        use_mock = True

    if use_mock:
        r = mock_redis_client(n=500)
        # Patch modules so valid imports use our mock
        if redis:
             redis.from_url = MagicMock(return_value=r)
        else:
             sys.modules['redis'] = MagicMock()
             sys.modules['redis'].from_url.return_value = r

    # DEBUG verify iteration
    print("DEBUG: Verifying mock data stream...")
    r._iter_pos = 0
    cnt = 0
    now = get_ny_time_millis()
    for _ in iter_position_closed(r=r, stream=args.stream, since_ms=now - 7*24*3600*1000):
        cnt += 1
    print(f"DEBUG: Found {cnt} closed trades from mock stream.")
    r._iter_pos = 0 # Reset for pipeline run!

    print("Running pipeline...")
    md, out = run_pipeline(
        redis_url=args.redis,
        window_hours=24 * 7,
        window_days=7,
        out_dir=args.out_dir
    )

    print("\nXXX REPORT START XXX\n")
    print(md)
    print("\nXXX REPORT END XXX\n")

    print(f"Sending to {args.notify_stream}...")
    send_telegram_report(r, stream=args.notify_stream, text=md, ts_ms=int(out.get("ts_ms", 0)))
    print("Done.")

if __name__ == "__main__":
    main()
