from __future__ import annotations

import json
from typing import Any

from utils.task_manager import safe_create_task
from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


async def _fetch_json(redis, key: str) -> dict[str, Any] | None:
    try:
        raw = await redis.get(key)
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "ignore")
        return json.loads(raw)
    except Exception:
        return None


def maybe_refresh_json(redis, *, key: str, dst: dict[str, Any], dst_key: str, refresh_ms: int) -> None:
    """
    Best-effort async refresh into an in-memory dict without blocking hot-path.
    Stores:
      dst[dst_key] = parsed json
      dst[dst_key + ':ts_ms'] = now_ms
    """
    now = _now_ms()
    last = int(dst.get(dst_key + ":ts_ms", 0) or 0)
    if refresh_ms <= 0 or (now - last) < refresh_ms:
        return

    async def _task():
        d = await _fetch_json(redis, key)
        if d is not None:
            dst[dst_key] = d
        dst[dst_key + ":ts_ms"] = _now_ms()

    try:
        safe_create_task(_task())
    except (RuntimeError, Exception):
        # fail-open: if no event loop or any error, just update timestamp to prevent immediate retry
        dst[dst_key + ":ts_ms"] = now

