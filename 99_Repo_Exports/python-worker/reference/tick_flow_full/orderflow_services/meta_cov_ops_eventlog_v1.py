from utils.time_utils import get_ny_time_millis

#!/usr/bin/env python3
"""meta_cov_ops_eventlog_v1.py

P37: Helper module for structured event logging to Redis Stream and cfg2 snapshot.
"""

import json
import logging
from typing import Any

try:
    import redis
except ImportError:
    redis = None

logger = logging.getLogger("MetaCovEventLog")

# Default Stream
DEFAULT_STREAM = "events:meta_cov_ops"

def now_ms() -> int:
    return get_ny_time_millis()

def write_event(
    r: "redis.Redis",
    stream_key: str,
    run_id: str,
    event_type: str,
    payload: dict[str, Any],
    maxlen: int = 10000
) -> str | None:
    """
    Writes an event to Redis Stream.
    payload should be a flat dict of strings/numbers mostly,
    but we can auto-serialize complex types to json string if needed.
    """
    if not r:
        logger.warning("No Redis connection, skipping event write.")
        return None

    try:
        # Prepare fields
        fields = {
            "ts_ms": now_ms(),
            "event": event_type,
            "run_id": run_id,
        }

        # Merge payload, ensuring values are strings or numbers
        for k, v in payload.items():
            if isinstance(v, (dict, list, bool, type(None))):
                 fields[k] = json.dumps(v)
            else:
                 fields[k] = v

        # XADD
        msg_id = r.xadd(stream_key, fields, maxlen=maxlen, approximate=True)
        return msg_id if isinstance(msg_id, str) else msg_id.decode()
    except Exception as e:
        logger.error(f"Failed to write event {event_type}: {e}")
        return None

def write_cfg2_snapshot(
    r: "redis.Redis",
    cfg_key: str,
    snapshot_data: dict[str, Any]
) -> None:
    """
    Writes snapshot metrics to settings:dynamic_cfg (HSET).
    Keys will be prefixed with 'meta_cov_ops_' if not already? 
    Actually, the caller should provide full keys as per P37 spec.
    """
    if not r:
        return

    try:
        # P37 spec keys: meta_cov_ops_last_ts_ms, etc.
        # We assume snapshot_data contains the exact field names and values.
        # We verify values are strings/numbers.
        mapping = {}
        for k, v in snapshot_data.items():
             if isinstance(v, (dict, list, bool, type(None))):
                 mapping[k] = json.dumps(v)
             else:
                 mapping[k] = v

        if mapping:
            r.hset(cfg_key, mapping=mapping)
            logger.info(f"Updated cfg2 snapshot at {cfg_key} with {len(mapping)} fields.")
    except Exception as e:
        logger.error(f"Failed to write cfg2 snapshot: {e}")

