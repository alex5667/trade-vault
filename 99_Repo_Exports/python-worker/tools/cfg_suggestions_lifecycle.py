#!/usr/bin/env python3
"""
python-worker/tools/cfg_suggestions_lifecycle.py

Library for checking suggestion health in Redis.
Supports detecting:
- Latest pointer for a given kind/scope.
- Metadata for specific suggestion IDs (SIDs).
- Approvals (HASH or specific keys).
- Applied status.

Status determination:
1. Created: Found meta key.
2. Approved:
   - status=approved/ok/ready in meta
   - approved=1/true/yes/ok in status HASH
   - n_approved >= min_approvals
3. Applied: Found applied key for SID.
"""
import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from common.redis_errors import is_transient_error
from utils.time_utils import get_ny_time_millis


async def async_retry_redis_operation[T](
    operation: Callable[[], Awaitable[T]],
    operation_name: str = "Redis operation",
    max_retries: int = 10,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    on_final_failure: Callable[[Exception], T] | None = None,
) -> T:
    last_exception: Exception | None = None
    for attempt in range(max_retries):
        try:
            return await operation()
        except Exception as e:
            last_exception = e
            if not is_transient_error(e):
                raise
            if attempt < max_retries - 1:
                delay = min(max_delay, base_delay * (2 ** attempt))
                await asyncio.sleep(delay)
            else:
                if on_final_failure is not None:
                    return on_final_failure(e)
                raise
    raise last_exception or RuntimeError("Retry failed")


def now_ms() -> int:
    return get_ny_time_millis()

def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except (ValueError, TypeError):
        return d

def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except (ValueError, TypeError):
        return d

async def check_suggestions_health(
    r: Any,
    prefix: str,
    kind: str,
    scopes: list[str],
    max_created_age_ms: int = 3600000,   # 1h
    max_approved_age_ms: int = 600000,    # 10m
    strict: bool = False
) -> tuple[dict[str, Any], list[str]]:
    """
    Analyzes suggestions for given kind and scopes.
    Returns:
        (results_dict, alerts_list)
    """
    now = now_ms()
    alerts: list[str] = []

    # Kind/Scope -> Latest SID
    # {prefix}:latest:{kind}:{scope} -> sid

    summary = {
        "kind": kind,
        "scopes": scopes,
        "n_pending": 0,
        "n_approved": 0,
        "n_applied": 0,
        "oldest_pending_ms": 0,
        "stuck_sids": []
    }

    all_sids = set()
    latest_sids = {}

    for scope in scopes:
        key = f"{prefix}:latest:{kind}:{scope}"
        sid = await async_retry_redis_operation(
            operation=lambda: r.get(key),
            operation_name="get_latest_sid",
        )
        if sid:
            if isinstance(sid, bytes):
                sid = sid.decode("utf-8")
            latest_sids[scope] = sid
            all_sids.add(sid)

    # Also scan for all meta keys if possible, but let's stick to 'latest' pointers first
    # as per patch description which mentions 'latest pointer' keys.
    # If we need to find OLD pending ones, we might need SCAN on {prefix}:meta:*
    # But usually 'latest' is what we care about for 'current' lifecycle.

    for sid in sorted(list(all_sids)):
        meta_key = f"{prefix}:meta:{sid}"
        meta_raw = await r.get(meta_key)
        if not meta_raw:
            continue

        try:
            meta = json.loads(meta_raw)
        except Exception:
            if strict:
                alerts.append(f"cfg_sugg_err:meta_json_parse:{sid}:{str(e)}")
            continue

        created_at = _i(meta.get("created_at_ms", meta.get("ts_ms", 0)))
        age_ms = now - created_at if created_at else 0

        # Check applied
        applied_key = f"{prefix}:applied:{sid}"
        try:
            is_applied = bool(await r.exists(applied_key))
        except Exception:
            is_applied = await r.get(applied_key) is not None

        # Check approved
        approvals_key = f"{prefix}:approvals:{sid}"
        approvals = {}
        try:
            # Check if it's a hash
            rtype = str(await r.type(approvals_key)).lower()
            if "hash" in rtype:
                approvals = await r.hgetall(approvals_key)
            elif await r.exists(approvals_key):
                approvals = {"status": await r.get(approvals_key)}
        except Exception:
            # Fallback for mock issues or unexpected types
            try:
                approvals = await r.hgetall(approvals_key)
            except Exception:
                try:
                    v = await r.get(approvals_key)
                    if v: approvals = {"status": v}
                except Exception:
                    pass

        is_approved = False
        # Heuristics for discovery
        status_in_meta = (meta.get("status", "")).lower()
        if status_in_meta in ("approved", "ok", "ready"):
            is_approved = True
        elif approvals:
            # Check for boolean-like flags in HASH
            for val in approvals.values():
                v = str(val).lower()
                if v in ("1", "true", "yes", "ok", "approved"):
                    is_approved = True
                    break
            # Check for count-based
            if not is_approved:
                n_appr = len([v for v in approvals.values() if str(v).lower() in ("1", "true", "yes", "ok", "approved")])
                min_appr = _i(meta.get("min_approvals", 1))
                if n_appr >= min_appr:
                    is_approved = True

        if is_applied:
            summary["n_applied"] += 1
        elif is_approved:
            summary["n_approved"] += 1
            if age_ms > max_approved_age_ms:
                alerts.append(f"cfg_sugg:approved_not_applied:{sid}:{age_ms//1000}s")
                if len(summary["stuck_sids"]) < 5:
                    summary["stuck_sids"].append({"sid": sid, "reason": "approved_not_applied", "age_s": age_ms//1000})
        else:
            summary["n_pending"] += 1
            if created_at:
                summary["oldest_pending_ms"] = max(summary["oldest_pending_ms"], age_ms)
            if age_ms > max_created_age_ms:
                alerts.append(f"cfg_sugg:pending_too_long:{sid}:{age_ms//1000}s")
                if len(summary["stuck_sids"]) < 5:
                    summary["stuck_sids"].append({"sid": sid, "reason": "pending_too_long", "age_s": age_ms//1000})

    return summary, alerts
