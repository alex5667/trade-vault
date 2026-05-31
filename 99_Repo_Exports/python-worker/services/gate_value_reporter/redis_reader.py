"""Bounded XRANGE reader for the gate_value_reporter.

Same shape as ml_confirm_sre_poller.outcome_metrics._xrange_recent — duplicated
locally to keep this service self-contained without reaching into another
service's private helper.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from redis.asyncio import Redis

log = logging.getLogger("gate_value_reporter.redis_reader")


async def xrange_recent(
    r: Redis,
    stream: str,
    lookback_ms: int,
    *,
    batch: int = 5000,
    max_entries: int = 200_000,
) -> list[tuple[str, dict[str, Any]]]:
    """Read entries from `stream` within the last `lookback_ms`.

    Returns a list of (entry_id, fields). Best-effort: failures return [].
    """
    now_ms = int(time.time() * 1000)
    start_ms = max(0, now_ms - lookback_ms)
    out: list[tuple[str, dict[str, Any]]] = []
    cursor = f"{start_ms}-0"

    try:
        while True:
            chunk = await r.xrange(stream, min=cursor, max="+", count=batch)
            if not chunk:
                break
            for entry_id, fields in chunk:
                out.append((str(entry_id), dict(fields)))
            if len(out) >= max_entries:
                log.warning("xrange %s capped at %d entries", stream, len(out))
                break
            last_id = str(chunk[-1][0])
            if last_id == cursor:
                break
            base, _, seq = last_id.partition("-")
            try:
                cursor = f"{base}-{int(seq or 0) + 1}"
            except ValueError:
                break
    except Exception as e:
        log.warning("xrange failed for %s: %s", stream, e)

    return out
