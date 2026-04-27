from __future__ import annotations

import os
from typing import List


def microbar_legacy_stream() -> str:
    from core.redis_keys import RedisStreams as RS
    return os.getenv("MICROBAR_LEGACY_STREAM", RS.EVENTS_MICROBAR_CLOSED)


def microbar_per_symbol_prefix() -> str:
    return os.getenv("MICROBAR_PER_SYMBOL_PREFIX", "events:microbar_closed:")


def microbar_majors_stream() -> str:
    return os.getenv("MICROBAR_MAJORS_STREAM", "events:microbar_closed:majors")


def microbar_symbols_set() -> str:
    return os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols")


def microbar_stream_for_symbol(symbol: str) -> str:
    return f"{microbar_per_symbol_prefix()}{str(symbol)}"


def list_microbar_symbols(r, max_n: int = 1000) -> List[str]:
    """Returns active symbols from microbar symbols set."""
    key = microbar_symbols_set()
    xs = list(r.smembers(key) or [])
    out: List[str] = []
    for x in xs[:max_n]:
        try:
            out.append(x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else str(x))
        except Exception:
            continue
    out.sort()
    return out

