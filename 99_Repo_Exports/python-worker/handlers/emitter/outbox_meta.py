from __future__ import annotations

import os
import json
from typing import Any


def meta_key(signal_id: str) -> str:
    """
    Ключ sidecar meta для сигнала.
    Должен совпадать с OutboxWriter._meta_key().
    """
    prefix = os.getenv("OUTBOX_META_PREFIX", "signal:meta:")
    return f"{prefix}{signal_id}"


def fetch_meta(redis: Any, signal_id: str) -> dict[str, Any]:
    """
    Подтянуть meta из Redis по signal_id.
    Fail-open: при любой ошибке возвращаем пустой dict.
    """
    if redis is None or not signal_id:
        return {}
    try:
        raw = redis.get(meta_key(signal_id))
        if not raw:
            return {}
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        obj = json.loads(str(raw))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}
