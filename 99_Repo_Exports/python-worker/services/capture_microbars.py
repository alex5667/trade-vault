
import asyncio
import json
import os
import time
from datetime import UTC, datetime

import redis.asyncio as aioredis

# Configuration
# Internal redis URL for the container
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
# Split streams support: use per-symbol streams instead of legacy shared stream
STREAM_TEMPLATE = os.getenv("MICROBAR_PER_SYMBOL_STREAM_TEMPLATE", "events:microbar_closed:{sym}")
SYMBOLS_SET = os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols")
LEGACY_STREAM = os.getenv("MICROBAR_LEGACY_STREAM", "events:microbar_closed")
# Write to logs dir which is mapped to host
OUTPUT_FILE = "logs/microbar_closed_slice.ndjson"
DURATION_SEC = 15 * 60  # 15 minutes
TARGET_SYMBOLS = {"BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "1000PEPEUSDT"}

def _decode(x) -> str:
    """Decode bytes to string, handling None and non-bytes types."""
    if x is None:
        return ""
    if isinstance(x, bytes):
        return x.decode("utf-8", "ignore")
    return str(x)

async def _discover_symbols(r, limit: int = 2000) -> list[str]:
    """
    Discover active symbols from the symbols-set maintained by the publisher.
    Uses SSCAN to avoid blocking on large sets.
    """
    out: list[str] = []
    cursor = 0
    while True:
        cursor, batch = await r.sscan(SYMBOLS_SET, cursor=cursor, count=10000)
        for s in batch or []:
            sym = _decode(s)
            if sym:
                out.append(sym)
                if len(out) >= limit:
                    return sorted(set(out))
        if int(cursor) == 0:
            break
    return sorted(set(out))

def _make_stream_keys(symbols: list[str]) -> list[str]:
    """Generate stream keys from template and symbol list."""
    if "{sym}" not in STREAM_TEMPLATE:
        return [STREAM_TEMPLATE]
    return [STREAM_TEMPLATE.format(sym=s) for s in symbols]

async def main():
    print(f"[{datetime.now(UTC)}] Starting capture...")
    print(f"Target Symbols: {TARGET_SYMBOLS}")
    print(f"Stream Template: {STREAM_TEMPLATE}")
    print(f"Symbols Set: {SYMBOLS_SET}")
    print(f"Duration: {DURATION_SEC}s")
    print(f"Output: {OUTPUT_FILE}")

    try:
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        await r.ping()
        print("Connected to Redis.")
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return

    # Get active symbols from symbols set
    try:
        all_syms = await r.smembers(SYMBOLS_SET)
        active_syms = {s for s in all_syms if s in TARGET_SYMBOLS}
        print(f"Active symbols from set: {active_syms}")
    except Exception as e:
        print(f"Warning: Could not read symbols set: {e}, using target symbols directly")
        active_syms = TARGET_SYMBOLS

    # Build stream keys for per-symbol streams
    stream_keys = {}
    last_ids = {}
    if "{sym}" in STREAM_TEMPLATE:
        for sym in active_syms:
            stream_key = STREAM_TEMPLATE.format(sym=sym)
            stream_keys[stream_key] = sym
            last_ids[stream_key] = "$"
        print(f"Reading from {len(stream_keys)} per-symbol streams")
    else:
        # Fallback to legacy stream if template doesn't support split
        stream_keys[LEGACY_STREAM] = None
        last_ids[LEGACY_STREAM] = "$"
        print(f"Reading from legacy stream: {LEGACY_STREAM}")

    start_time = time.time()
    count = 0

    # Ensure logs dir exists (it should)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        while True:
            elapsed = time.time() - start_time
            if elapsed >= DURATION_SEC:
                print(f"Time limit reached ({DURATION_SEC}s).")
                break

            remaining = max(0.1, DURATION_SEC - elapsed)
            block_ms = min(1000, int(remaining * 1000))

            try:
                # Read from all streams
                streams = await r.xread(last_ids, count=100, block=block_ms)

                if not streams:
                    continue

                for stream_name, messages in streams:
                    sym_from_stream = stream_keys.get(stream_name)
                    for message_id, data in messages:
                        last_ids[stream_name] = message_id

                        payload_str = data.get("payload")
                        if not payload_str:
                            continue

                        try:
                            payload = json.loads(payload_str)
                            # Extract symbol from payload if not from stream key
                            sym = sym_from_stream or payload.get("symbol")
                            if sym and sym in TARGET_SYMBOLS:
                                json.dump(payload, f, ensure_ascii=False)
                                f.write("\n")
                                f.flush()
                                count += 1
                                if count % 10 == 0:
                                    print(f"Captured {count} events... (last: {sym} @ {payload.get('ts_ms')})")
                        except Exception:
                            continue

            except Exception as e:
                print(f"Error reading stream: {e}")
                await asyncio.sleep(1)

    print(f"Finished. Total captured: {count}")
    await r.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAborted by user.")
