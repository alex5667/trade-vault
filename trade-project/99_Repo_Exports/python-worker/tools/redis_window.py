from __future__ import annotations

import json
from typing import Any

import redis

from common.redis_errors import retry_redis_operation


def _safe_json_loads(x: Any) -> Any | None:
    """Return parsed JSON or None. Accepts already-parsed dict/list."""
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    if not isinstance(x, str):
        return None
    s = x.strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _merge_payload_fields(fields: dict[str, Any]) -> dict[str, Any]:
    """Best-effort: merge nested JSON payload/indicators into flat fields.

    Many producers write a compact flat schema (p_edge, p_min, ...) and optionally
    attach a JSON `payload` with additional context (indicators, schema pins,
    exec-cost fields, etc). Downstream tooling often wants to read a time window
    and compute aggregates without having to special-case payload parsing.

    Rules:
    - Never overwrite existing top-level fields.
    - Only merge scalar values (str/int/float/bool/None).
    - Merge order: payload.* then payload.indicators.* then fields.indicators.*.
    """
    out: dict[str, Any] = dict(fields)

    def _is_scalar(v: Any) -> bool:
        return isinstance(v, (str, int, float, bool)) or v is None

    def _merge_from_dict(d: dict[str, Any]) -> None:
        # payload top-level
        for k, v in d.items():
            if k == "indicators":
                continue
            if k in out:
                continue
            if _is_scalar(v):
                out[k] = v

        # payload.indicators
        ind_obj = _safe_json_loads(d.get("indicators"))
        if isinstance(ind_obj, dict):
            for k, v in ind_obj.items():
                if k in out:
                    continue
                if _is_scalar(v):
                    out[k] = v

    payload_obj = _safe_json_loads(fields.get("payload"))
    if isinstance(payload_obj, dict):
        _merge_from_dict(payload_obj)

    ind_field_obj = _safe_json_loads(fields.get("indicators"))
    if isinstance(ind_field_obj, dict):
        for k, v in ind_field_obj.items():
            if k in out:
                continue
            if _is_scalar(v):
                out[k] = v

    return out


def read_recent_stream(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> list[dict[str, Any]]:
    """Read recent messages from Redis stream within a time window.

    Scans stream backwards from latest, stopping when timestamp < since_ms.
    Returns messages in chronological order (oldest first).

    Args:
        r: Redis client (decode_responses=True)
        stream: Stream name
        since_ms: Minimum timestamp (epoch milliseconds)
        max_scan: Maximum number of messages to scan (safety limit)

    Returns:
        List of message field dicts (chronological order), with payload/indicators
        merged into the flat dict when present.
    """
    rows: list[dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = retry_redis_operation(
            lambda: r.xrevrange(stream, max=last_id, min="-", count=2000),
            operation_name=f"read_recent_stream xrevrange {stream}",
        )
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            try:
                ts = int(float(fields.get("ts_ms", 0) or 0))
            except Exception:
                ts = 0
            if ts and ts < since_ms:
                scanned = max_scan
                break
            rows.append(_merge_payload_fields(dict(fields)))
    rows.reverse()
    return rows

