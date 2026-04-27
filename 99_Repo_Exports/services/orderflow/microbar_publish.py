from __future__ import annotations

import os
import json
from typing import Any, Dict


async def publish_microbar_closed(redis, *, symbol: str, payload_obj: Dict[str, Any]) -> None:
    """Publish microbar_closed with optional per-symbol retention split.

    Why:
      - a single global stream is dominated by majors and blinds minors due to MAXLEN.

    Migration:
      - when MICROBAR_SPLIT_DUAL_WRITE=1, still writes to legacy stream.
    """
    sym = str(symbol or "").upper()
    if not sym:
        return

    split = os.getenv("MICROBAR_SPLIT_STREAMS_ENABLE", "0").strip().lower() in {"1", "true", "yes"}
    dual = os.getenv("MICROBAR_SPLIT_DUAL_WRITE", "1").strip().lower() in {"1", "true", "yes"}

    per_prefix = os.getenv("MICROBAR_PER_SYMBOL_PREFIX", "events:microbar_closed:")
    majors_stream = os.getenv("MICROBAR_MAJORS_STREAM", "events:microbar_closed:majors")
    legacy_stream = os.getenv("MICROBAR_LEGACY_STREAM", "events:microbar_closed")
    symbols_set = os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols")

    per_maxlen = int(os.getenv("MICROBAR_PER_SYMBOL_MAXLEN", "50000"))
    legacy_maxlen = int(os.getenv("MICROBAR_LEGACY_MAXLEN", "200000"))
    set_ttl = int(os.getenv("MICROBAR_SYMBOLS_SET_TTL_SEC", "86400"))

    majors = {x.strip().upper() for x in os.getenv("MICROBAR_MAJORS", "BTCUSDT,ETHUSDT").split(",") if x.strip()}

    payload = {"payload": json.dumps(payload_obj, ensure_ascii=False)}

    if not split:
        await redis.xadd(legacy_stream, payload, maxlen=legacy_maxlen, approximate=True)
        return

    # per-symbol stream
    await redis.xadd(f"{per_prefix}{sym}", payload, maxlen=per_maxlen, approximate=True)
    try:
        # discovery set (best-effort)
        await redis.sadd(symbols_set, sym)
        await redis.expire(symbols_set, set_ttl)
    except Exception:
        pass

    # optional majors stream
    if sym in majors:
        await redis.xadd(majors_stream, payload, maxlen=legacy_maxlen, approximate=True)

    # dual-write legacy for migration
    if dual:
        await redis.xadd(legacy_stream, payload, maxlen=legacy_maxlen, approximate=True)

