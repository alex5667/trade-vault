
import asyncio
import json
import os
import time
from datetime import UTC, datetime

import redis.asyncio as aioredis
import contextlib

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
# Legacy shared stream (kept for migration / dual-write)
LEGACY_STREAM_KEY = os.getenv("MICROBAR_LEGACY_STREAM", "events:microbar_closed")
SYMBOLS_SET_KEY = os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols")
# Per-symbol stream template (preferred when split streams are enabled)
STREAM_TEMPLATE = os.getenv("MICROBAR_PER_SYMBOL_STREAM_TEMPLATE", "events:microbar_closed:{sym}")
OUTPUT_FILE = "microbar_closed_slice.ndjson"
DURATION_SEC = 15 * 60  # 15 minutes
SYMBOLS = {"BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "1000PEPEUSDT"}

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
        cursor, batch = await r.sscan(SYMBOLS_SET_KEY, cursor=cursor, count=10000)
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
    print(f"Target Symbols: {SYMBOLS}")
    print(f"Stream: {STREAM_KEY}")
    print(f"Duration: {DURATION_SEC}s")
    print(f"Output: {OUTPUT_FILE}")

    try:
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        await r.ping()
        print("Connected to Redis.")
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return

    # Create distinct consumer group/name to avoid stealing/conflict if any (though usually events are fan-out? Stream groups share offset)
    # Actually, for `events:` streams, usually we use XREAD (fan-out by using unique group or just XREAD from $)
    # If we use XREAD block, we get new messages.

    last_id = "$"
    start_time = time.time()
    count = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        while True:
            elapsed = time.time() - start_time
            if elapsed >= DURATION_SEC:
                print(f"Time limit reached ({DURATION_SEC}s).")
                break

            remaining = max(0.1, DURATION_SEC - elapsed)
            block_ms = min(1000, int(remaining * 1000))

            try:
                # Split-stream aware reader:
                # - if MICROBAR_SPLIT_STREAMS_ENABLE=1 => read from events:microbar_closed:{sym}
                # - else fallback to legacy shared stream (events:microbar_closed)
                split = os.getenv("MICROBAR_SPLIT_STREAMS_ENABLE", "0") == "1"

                # Optional: deterministic capture universe
                # MICROBAR_CAPTURE_SYMBOLS=BTCUSDT,ETHUSDT
                sym_env = os.getenv("MICROBAR_CAPTURE_SYMBOLS", "").strip()
                symbols = [s.strip() for s in sym_env.split(",") if s.strip()] if sym_env else []
                if split and not symbols:
                    symbols = await _discover_symbols(r, limit=int(os.getenv("MICROBAR_CAPTURE_MAX_SYMBOLS", "200")))

                stream_keys = _make_stream_keys(symbols) if split else [LEGACY_STREAM_KEY]
                # Track per-stream last_id to avoid missing/duplicating data
                last_ids: dict[str, str] = dict.fromkeys(stream_keys, last_id)

                # Read from multiple streams in one XREAD call (fan-in)
                streams = await r.xread(last_ids, count=100, block=block_ms)

                # Update per-stream last_ids for next iteration
                for sk, entries in streams or []:
                    sks = _decode(sk)
                    if entries:
                        last_ids[sks] = _decode(entries[-1][0])

                # Preserve legacy variable last_id as max seen ID (best-effort)
                if streams:
                    with contextlib.suppress(Exception):
                        last_id = max((_decode(entries[-1][0]) for _, entries in streams if entries), default=last_id)

                if not streams:
                    continue

                for stream_name, messages in streams:
                    for message_id, data in messages:
                        payload_str = data.get("payload")
                        if not payload_str:
                            continue

                        try:
                            payload = json.loads(payload_str)
                            sym = payload.get("symbol")
                            if sym in SYMBOLS:
                                # Write to file
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
    await r.aclose()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAborted by user.")
