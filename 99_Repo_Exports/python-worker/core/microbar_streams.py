from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

# Split-streams defaults (aligned with the migration plan)
from core.redis_keys import RedisStreams as RS
from core.redis_keys import RedisKeyPrefixes as RK
LEGACY_STREAM = os.getenv("MICROBAR_LEGACY_STREAM", RS.EVENTS_MICROBAR_CLOSED)
PER_SYMBOL_PREFIX = os.getenv("MICROBAR_PER_SYMBOL_PREFIX", RK.MICROBAR_PER_SYMBOL)
ALT_PER_SYMBOL_PREFIX = os.getenv("MICROBAR_ALT_PER_SYMBOL_PREFIX", "microbar_closed:")
SYMBOLS_SET = os.getenv("MICROBAR_SYMBOLS_SET", RK.MICROBAR_SYMBOLS_SET)
ALT_SYMBOLS_SET = os.getenv("MICROBAR_ALT_SYMBOLS_SET", "microbar_closed:symbols")


def _to_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x)


def _as_payload(fields: Dict[str, Any]) -> Dict[str, Any]:
    # common project pattern: {"payload": "<json>"} OR flat fields
    if not isinstance(fields, dict):
        return {}
    if "payload" in fields and fields["payload"]:
        try:
            return json.loads(fields["payload"])
        except Exception:
            return {}
    return dict(fields)


async def list_symbols(r, fallback: Optional[List[str]] = None) -> List[str]:
    # Prefer split-streams symbols set
    try:
        syms = await r.smembers(SYMBOLS_SET)
        if syms:
            out = sorted([_to_str(x) for x in syms if _to_str(x)])
            if out:
                return out
    except Exception:
        pass

    # Alt set (older naming)
    try:
        syms = await r.smembers(ALT_SYMBOLS_SET)
        if syms:
            out = sorted([_to_str(x) for x in syms if _to_str(x)])
            if out:
                return out
    except Exception:
        pass

    return list(fallback or [])


async def pick_stream_key(r, sym: str) -> str:
    # Prefer per-symbol streams (plan)
    k1 = f"{PER_SYMBOL_PREFIX}{sym}"
    try:
        if await r.exists(k1):
            return k1
    except Exception:
        pass

    # Older per-symbol naming
    k2 = f"{ALT_PER_SYMBOL_PREFIX}{sym}"
    try:
        if await r.exists(k2):
            return k2
    except Exception:
        pass

    # Legacy shared stream
    return LEGACY_STREAM


async def read_microbars(
    r,
    *,
    sym: str,
    count: int = 5000,
    reverse: bool = False,
    start_id: str = "-",
    end_id: str = "+",
) -> List[Dict[str, Any]]:
    key = await pick_stream_key(r, sym)
    items: List[Tuple[str, Dict[str, Any]]] = []
    try:
        if reverse:
            items = await r.xrevrange(key, max=end_id, min=start_id, count=count)
        else:
            items = await r.xrange(key, min=start_id, max=end_id, count=count)
    except Exception:
        items = []

    out: List[Dict[str, Any]] = []

    if key == LEGACY_STREAM:
        # shared stream => filter by symbol inside payload/fields
        for _, fields in items:
            p = _as_payload(fields)
            if _to_str(p.get("symbol", "")).upper() == sym.upper():
                out.append(p)
        return out

    for _, fields in items:
        out.append(_as_payload(fields))
    return out
















