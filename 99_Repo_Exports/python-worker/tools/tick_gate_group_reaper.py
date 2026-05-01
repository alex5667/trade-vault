from __future__ import annotations
"""tick_gate_group_reaper

Purpose
-------
Maintain Redis Stream consumer group health for the tick gate aggregator by
periodically auto-claiming old PEL entries (XAUTOCLAIM) and optionally ACKing them.

Why
---
If a consumer dies while holding pending entries, the group may stall and the
aggregator will stop advancing. This reaper provides a safety net.

Env
---
REDIS_URL
TICK_GATE_REDIS_STREAM (default: ops:tick_quality_gate)
TICK_GATE_REDIS_GROUP  (default: tick_gate_agg)
TICK_GATE_REAPER_IDLE_MS (default: 300000)
TICK_GATE_REAPER_CLAIM_COUNT (default: 200)
TICK_GATE_REAPER_INTERVAL_S (default: 15)
TICK_GATE_REAPER_ACK_ONLY (default: 1)  # 1=claim+ack, 0=claim-only
TICK_GATE_REAPER_METRICS_PORT (default: 9113)
"""


import os
import time
from typing import Any, Dict, List, Tuple

import redis  # type: ignore
from prometheus_client import Counter, Gauge, start_http_server


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, str(default))).strip())
    except Exception:
        return default


STREAM = os.getenv("TICK_GATE_REDIS_STREAM", "ops:tick_quality_gate")
GROUP = os.getenv("TICK_GATE_REDIS_GROUP", "tick_gate_agg")
IDLE_MS = _env_int("TICK_GATE_REAPER_IDLE_MS", 300_000)
CLAIM_COUNT = _env_int("TICK_GATE_REAPER_CLAIM_COUNT", 200)
INTERVAL_S = _env_int("TICK_GATE_REAPER_INTERVAL_S", 15)
ACK_ONLY = _env_int("TICK_GATE_REAPER_ACK_ONLY", 1) == 1
METRICS_PORT = _env_int("TICK_GATE_REAPER_METRICS_PORT", 9113)


reaper_runs_total = Counter(
    "tick_gate_reaper_runs_total",
    "Total reaper loop iterations",
)
reaper_errors_total = Counter(
    "tick_gate_reaper_errors_total",
    "Total reaper errors",
)
reaper_claimed_total = Counter(
    "tick_gate_reaper_claimed_total",
    "Total messages claimed from PEL",
)
reaper_acked_total = Counter(
    "tick_gate_reaper_acked_total",
    "Total messages acknowledged after claim",
)

reaper_pending = Gauge(
    "tick_gate_reaper_pending",
    "Current pending messages in group PEL",
)
reaper_last_success_ts = Gauge(
    "tick_gate_reaper_last_success_ts_seconds",
    "Unix timestamp of last successful reaper action",
)
reaper_last_success_age = Gauge(
    "tick_gate_reaper_last_success_age_s",
    "Seconds since last successful reaper action",
)


def _redis() -> "redis.Redis":
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    return redis.Redis.from_url(url, decode_responses=True, socket_timeout=5, socket_connect_timeout=5)


def _group_exists(r: "redis.Redis") -> bool:
    try:
        groups = r.xinfo_groups(STREAM)
        return any(g.get("name") == GROUP for g in groups)
    except Exception:
        return False


def _ensure_group(r: "redis.Redis") -> None:
    if _group_exists(r):
        return
    try:
        # Create group at start (0-0). If stream does not exist, mkstream=True creates it.
        r.xgroup_create(name=STREAM, groupname=GROUP, id="0-0", mkstream=True)
    except Exception:
        # Group may have been created concurrently.
        return


def _pending_info(r: "redis.Redis") -> Tuple[int, str]:
    """Return (pending_count, smallest_id)"""
    try:
        info = r.xpending(STREAM, GROUP)
        # redis-py returns dict: {'pending': N, 'min': '...', 'max': '...', 'consumers': [...]}
        pending = int(info.get("pending") or 0)
        min_id = str(info.get("min") or "0-0")
        return pending, min_id
    except Exception:
        return 0, "0-0"


def _xautoclaim(r: "redis.Redis", start_id: str) -> Tuple[str, List[str]]:
    """Return (next_start_id, claimed_ids)."""
    # redis-py exposes xautoclaim since 4.3+. Fallback to execute_command.
    try:
        res = r.xautoclaim(name=STREAM, groupname=GROUP, consumername="reaper", min_idle_time=IDLE_MS, start_id=start_id, count=CLAIM_COUNT)
        # res: (next_start_id, [ (id, {fields}) ... ], deleted_ids)
        next_id = str(res[0])
        msgs = res[1] or []
        claimed_ids = [m[0] for m in msgs if m and m[0]]
        return next_id, claimed_ids
    except Exception:
        try:
            res = r.execute_command("XAUTOCLAIM", STREAM, GROUP, "reaper", IDLE_MS, start_id, "COUNT", CLAIM_COUNT)
            next_id = str(res[0])
            msgs = res[1] or []
            claimed_ids = [m[0] for m in msgs if m and m[0]]
            return next_id, claimed_ids
        except Exception:
            return start_id, []


def _ack_ids(r: "redis.Redis", ids: List[str]) -> int:
    if not ids:
        return 0
    try:
        # XACK stream group id [id ...]
        return int(r.xack(STREAM, GROUP, *ids))
    except Exception:
        return 0


def main() -> None:
    start_http_server(METRICS_PORT)
    r = _redis()
    _ensure_group(r)

    last_success = 0.0

    while True:
        reaper_runs_total.inc()
        now = time.time()
        if last_success > 0:
            reaper_last_success_age.set(max(0.0, now - last_success))
        else:
            reaper_last_success_age.set(1e9)

        try:
            pending, min_id = _pending_info(r)
            reaper_pending.set(pending)

            if pending <= 0:
                time.sleep(INTERVAL_S)
                continue

            start_id = min_id if min_id else "0-0"
            claimed_any = 0
            acked_any = 0
            # Try a few rounds to drain older pending items.
            for _ in range(5):
                next_id, claimed_ids = _xautoclaim(r, start_id)
                if not claimed_ids:
                    break
                claimed_any += len(claimed_ids)
                reaper_claimed_total.inc(len(claimed_ids))
                if ACK_ONLY:
                    acked = _ack_ids(r, claimed_ids)
                    if acked:
                        acked_any += acked
                        reaper_acked_total.inc(acked)
                # Advance cursor to avoid re-claiming the same range.
                start_id = next_id

            if claimed_any > 0 or (pending > 0 and acked_any > 0):
                last_success = time.time()
                reaper_last_success_ts.set(last_success)
                reaper_last_success_age.set(0.0)

        except Exception:
            reaper_errors_total.inc()

        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()
