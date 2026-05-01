from __future__ import annotations

import logging
import os
import json
from functools import lru_cache
from typing import Any, Dict

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _env_cfg() -> Dict[str, Any]:
    """Cache all env reads once per process (hot path: called per bar × symbol)."""
    return {
        "split": os.getenv("MICROBAR_SPLIT_STREAMS_ENABLE", "0").strip().lower() in {"1", "true", "yes"},
        "dual": os.getenv("MICROBAR_SPLIT_DUAL_WRITE", "1").strip().lower() in {"1", "true", "yes"},
        "per_prefix": os.getenv("MICROBAR_PER_SYMBOL_PREFIX", "events:microbar_closed:"),
        "majors_stream": os.getenv("MICROBAR_MAJORS_STREAM", "events:microbar_closed:majors"),
        "legacy_stream": os.getenv("MICROBAR_LEGACY_STREAM", "events:microbar_closed"),
        "symbols_set": os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols"),
        "per_maxlen": int(os.getenv("MICROBAR_PER_SYMBOL_MAXLEN", "5000")),
        "legacy_maxlen": int(os.getenv("MICROBAR_LEGACY_MAXLEN", "10000")),
        "set_ttl": int(os.getenv("MICROBAR_SYMBOLS_SET_TTL_SEC", "86400")),
        "majors": frozenset(
            x.strip().upper()
            for x in os.getenv("MICROBAR_MAJORS", "BTCUSDT,ETHUSDT").split(",")
            if x.strip()
        )
    }


async def publish_microbar_closed(redis_client, *, symbol: str, payload_obj: Dict[str, Any]) -> None:
    """Publish microbar_closed with optional per-symbol retention split.

    Why:
      - a single global stream is dominated by majors and blinds minors due to MAXLEN.

    Migration:
      - when MICROBAR_SPLIT_DUAL_WRITE=1, still writes to legacy stream.

    Error policy:
      - All Redis I/O is wrapped; transient TimeoutError / ConnectionError are
        logged at WARNING and swallowed so the calling task never raises.
    """
    sym = str(symbol or "").upper()
    if not sym:
        return

    cfg = _env_cfg()
    payload = {"payload": json.dumps(payload_obj, ensure_ascii=False)}

    for attempt in range(1, 6):
        try:
            if not cfg["split"]:
                await redis_client.xadd(cfg["legacy_stream"], payload, maxlen=cfg["legacy_maxlen"], approximate=True)
                return

            # per-symbol stream
            await redis_client.xadd(f"{cfg['per_prefix']}{sym}", payload, maxlen=cfg["per_maxlen"], approximate=True)

            try:
                # discovery set (best-effort)
                await redis_client.sadd(cfg["symbols_set"], sym)
                await redis_client.expire(cfg["symbols_set"], cfg["set_ttl"])
            except Exception:
                pass

            # optional majors stream
            if sym in cfg["majors"]:
                await redis_client.xadd(cfg["majors_stream"], payload, maxlen=cfg["legacy_maxlen"], approximate=True)

            # dual-write legacy for migration
            if cfg["dual"]:
                await redis_client.xadd(cfg["legacy_stream"], payload, maxlen=cfg["legacy_maxlen"], approximate=True)
            
            return  # Success
            
        except Exception as exc:
            import redis.exceptions as redis_exceptions
            import asyncio
            is_timeout = isinstance(exc, (redis_exceptions.ConnectionError, redis_exceptions.TimeoutError, asyncio.TimeoutError, TimeoutError)) or "TimeoutError" in type(exc).__name__
            if is_timeout and attempt < 5:
                logger.debug(
                    "microbar_publish: transient Redis error for %s (attempt %d/5): %s – retrying...",
                    sym, attempt, exc
                )
                await asyncio.sleep(min(5.0, 0.5 * (2 ** (attempt - 1))))
                continue
            else:
                logger.warning(
                    "microbar_publish: Redis error for %s after %d attempts (%s: %s) – skipping",
                    sym, attempt, type(exc).__name__, exc,
                )
                return
