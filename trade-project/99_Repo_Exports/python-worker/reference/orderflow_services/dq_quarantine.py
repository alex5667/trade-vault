from __future__ import annotations

import json
from typing import Any

from utils.time_utils import get_ny_time_millis


def _truncate(s: str, n: int = 4000) -> str:
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."


async def emit_quarantine_row(
    redis,
    *,
    stream: str,
    payload: dict[str, Any],
    why: str,
    emit_src: str,
    maxlen: int = 200000,
) -> None:
    """
    Write invalid/untrusted rows to a separate quarantine stream for DQ review.
    """
    now_ms = get_ny_time_millis()
    try:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        raw = "{}"

    row: dict[str, Any] = {
        "ts_seen_ms": str(now_ms),
        "emit_src": str(emit_src),
        "why": str(why)[:160],
        "symbol": (payload.get("symbol", "") or ""),
        "ts_ms": (payload.get("ts_ms", "") or ""),
        "scenario_v4": (payload.get("scenario_v4", "") or ""),
        "raw": _truncate(raw, 4000),
    }
    await redis.xadd(stream, row, maxlen=maxlen, approximate=True)
