from utils.time_utils import get_ny_time_millis

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
conf_cal_ops_eventlog_v1.py

Structured event logging for confidence calibration operations.
Writes:
- Redis Stream (XADD)
- optional PubSub channel (PUBLISH)

Fail-open: if redis not installed or connection missing, no crash.
"""

import json
import logging
from typing import Any

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None

logger = logging.getLogger("ConfCalOpsEventLog")


def now_ms() -> int:
    return get_ny_time_millis()


def _encode(v: Any) -> Any:
    if isinstance(v, (dict, list, bool, type(None))):
        return json.dumps(v, ensure_ascii=False)
    return v


def write_stream_event(
    r: "redis.Redis",  # type: ignore
    *,
    stream_key: str,
    event_type: str,
    payload: dict[str, Any],
    run_id: str = "",
    maxlen: int = 10000,
) -> str | None:
    if not r:
        return None
    try:
        fields: dict[str, Any] = {
            "ts_ms": now_ms(),
            "event": str(event_type),
            "run_id": (run_id or ""),
        }
        for k, v in (payload or {}).items():
            fields[str(k)] = _encode(v)
        msg_id = r.xadd(stream_key, fields, maxlen=maxlen, approximate=True)
        return msg_id if isinstance(msg_id, str) else msg_id.decode("utf-8", "replace")
    except Exception as e:
        logger.warning("Failed to write stream event %s: %s", event_type, e)
        return None


def publish_event(
    r: "redis.Redis",  # type: ignore
    *,
    channel: str,
    event_type: str,
    payload: dict[str, Any],
    run_id: str = "",
) -> None:
    if not r:
        return
    try:
        obj = {"ts_ms": now_ms(), "event": str(event_type), "run_id": (run_id or ""), "payload": payload}
        r.publish(channel, json.dumps(obj, ensure_ascii=False))
    except Exception as e:
        logger.warning("Failed to publish event %s: %s", event_type, e)
