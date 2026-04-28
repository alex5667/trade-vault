# services/redis_streams_runtime.py
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import redis
from prometheus_client import Counter

import logging
import os
import sys

# Wrap in a registry check to avoid "Duplicated timeseries" during test collection 
# if the module is loaded multiple times (e.g. under different paths)
from prometheus_client import REGISTRY
_metric_name = "consumer_group_recovery_attempts"
if _metric_name in REGISTRY._names_to_collectors:
    CONSUMER_GROUP_RECOVERY_ATTEMPTS = REGISTRY._names_to_collectors[_metric_name]
elif _metric_name + "_total" in REGISTRY._names_to_collectors:
    CONSUMER_GROUP_RECOVERY_ATTEMPTS = REGISTRY._names_to_collectors[_metric_name + "_total"]
else:
    CONSUMER_GROUP_RECOVERY_ATTEMPTS = Counter(
        _metric_name,
        "Total attempts to recover consumer group",
        ["group_name", "stream_name"],
    )


@dataclass
class StreamMsg:
    stream: str
    msg_id: str
    fields: Dict[str, str]


def ensure_group(r: redis.Redis, stream: str, group: str, start_id: str = "$") -> None:
    # MKSTREAM создаст stream если его нет
    try:
        r.xgroup_create(stream, group, id=start_id, mkstream=True)
    except Exception as e:
        # BUSYGROUP игнорируем
        if "BUSYGROUP" not in str(e):
            raise


# ── Stream-discovery cache ────────────────────────────────────────────────────
# Keyed by (redis connection id, frozenset of patterns) → (timestamp, sorted list)
_discover_cache: Dict[Tuple, Tuple[float, List[str]]] = {}
_DISCOVER_CACHE_TTL_SEC: float = float(os.getenv("TM_DISCOVER_CACHE_TTL_SEC", "120"))


def redis_keys_safe(r: redis.Redis, pattern: str, scan_count: int = 100) -> List[str]:
    """Non-blocking KEYS replacement using SCAN iteration.

    Uses a small scan_count (default 100) to release the Redis event-loop
    between batches and avoid the 41ms+ blocking seen with KEYS on large
    keyspaces.
    """
    found: List[str] = []
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=scan_count)
        for k in keys:
            found.append(k if isinstance(k, str) else k.decode("utf-8", errors="ignore"))
        if cursor == 0:
            break
        time.sleep(0.001)  # yield ~1ms between batches
    return found


def discover_streams(
    r: redis.Redis,
    patterns: List[str],
    scan_count: int = 100,
    use_cache: bool = True,
) -> List[str]:
    """Discover stream keys matching patterns.

    P3 fix: scan_count default 5000 → 100 to avoid multi-millisecond Redis
    event-loop blocks on large keyspaces.  A TTL-based in-process cache
    (TM_DISCOVER_CACHE_TTL_SEC, default 120s) prevents repeated full-keyspace
    scans on every RESCAN_EVERY_SEC tick.
    """
    cache_key: Tuple = (id(r), frozenset(patterns))
    now = time.monotonic()

    if use_cache and cache_key in _discover_cache:
        ts, cached = _discover_cache[cache_key]
        if now - ts < _DISCOVER_CACHE_TTL_SEC:
            return cached

    streams: Set[str] = set()
    for pat in patterns:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pat, count=scan_count)
            if keys:
                # Use pipeline to batch TYPE checks — one RTT per SCAN batch
                pipe = r.pipeline()
                for k in keys:
                    pipe.type(k)
                types = pipe.execute()

                for k, t in zip(keys, types):
                    if isinstance(t, (bytes, bytearray)):
                        t = t.decode("utf-8", errors="ignore")
                    if t == "stream":
                        streams.add(
                            k if isinstance(k, str) else k.decode("utf-8", errors="ignore")
                        )
            if cursor == 0:
                break
            # Yield ~2ms between batches so hot-path XADD/XREADGROUP can proceed
            time.sleep(0.002)

    result = sorted(streams)
    if use_cache:
        _discover_cache[cache_key] = (now, result)
    return result


def invalidate_discover_cache(r: Optional[redis.Redis] = None) -> None:
    """Invalidate the discover-streams cache (e.g. after a new stream is created)."""
    if r is None:
        _discover_cache.clear()
    else:
        for k in list(_discover_cache.keys()):
            if k[0] == id(r):
                del _discover_cache[k]


# Maximum retries for transient Redis connection errors in xreadgroup_multi
_XREADGROUP_MAX_RETRIES = int(os.getenv("TM_XREADGROUP_MAX_RETRIES", "3"))
_XREADGROUP_BASE_BACKOFF_SEC = float(os.getenv("TM_XREADGROUP_BASE_BACKOFF_SEC", "0.5"))

_xreadgroup_log = logging.getLogger("redis_streams_runtime")


def xreadgroup_multi(
    r: redis.Redis,
    group: str,
    consumer: str,
    streams: List[str],
    count: int = 200,
    block_ms: int = 1000,
) -> List[StreamMsg]:
    if not streams:
        time.sleep(block_ms / 1000.0)
        return []

    stream_dict = {s: ">" for s in streams}
    last_err: Optional[Exception] = None

    for attempt in range(_XREADGROUP_MAX_RETRIES + 1):
        try:
            resp = r.xreadgroup(
                groupname=group, consumername=consumer,
                streams=stream_dict, count=count, block=block_ms,
            ) or []
            break  # success
        except redis.ResponseError as e:
            if "NOGROUP" in str(e):
                for s in streams:
                    try:
                        CONSUMER_GROUP_RECOVERY_ATTEMPTS.labels(stream=s).inc()
                    except Exception:
                        pass
                    ensure_group(r, s, group, start_id="$")
                # Retry once after group creation
                resp = r.xreadgroup(
                    groupname=group, consumername=consumer,
                    streams=stream_dict, count=count, block=block_ms,
                ) or []
                break
            else:
                raise
        except (redis.ConnectionError, redis.TimeoutError, ConnectionError, OSError) as e:
            last_err = e
            if attempt < _XREADGROUP_MAX_RETRIES:
                backoff = _XREADGROUP_BASE_BACKOFF_SEC * (2 ** attempt)
                _xreadgroup_log.warning(
                    "xreadgroup_multi: connection error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1, _XREADGROUP_MAX_RETRIES, e, backoff,
                )
                time.sleep(backoff)
            else:
                _xreadgroup_log.error(
                    "xreadgroup_multi: connection error after %d retries: %s — returning empty",
                    _XREADGROUP_MAX_RETRIES, e,
                )
                return []

    out: List[StreamMsg] = []
    for stream_name, items in resp:
        s = stream_name if isinstance(stream_name, str) else stream_name.decode("utf-8", errors="ignore")
        for msg_id, fields in items:
            mid = msg_id if isinstance(msg_id, str) else msg_id.decode("utf-8", errors="ignore")
            f2: Dict[str, str] = {}
            for k, v in (fields or {}).items():
                ks = k if isinstance(k, str) else k.decode("utf-8", errors="ignore")
                vs = v if isinstance(v, str) else v.decode("utf-8", errors="ignore")
                f2[ks] = vs
            out.append(StreamMsg(stream=s, msg_id=mid, fields=f2))
    return out


def autoclaim_stale(
    r: redis.Redis,
    stream: str,
    group: str,
    consumer: str,
    min_idle_ms: int,
    start_id: str = "0-0",
    count: int = 50,
) -> List[StreamMsg]:
    """
    Перехватывает зависшие pending (после падения воркера, сетевых проблем и т.п.).
    """
    try:
        next_id, msgs, _deleted = r.xautoclaim(stream, group, consumer, min_idle_ms, start_id, count=count)
    except redis.ResponseError as e:
        if "NOGROUP" in str(e):
             try:
                 CONSUMER_GROUP_RECOVERY_ATTEMPTS.labels(stream=stream).inc()
             except Exception:
                 pass
             ensure_group(r, stream, group, start_id="0")
             next_id, msgs, _deleted = r.xautoclaim(stream, group, consumer, min_idle_ms, start_id, count=count)
        else:
             return []
    except Exception:
        return []

    out: List[StreamMsg] = []
    for msg_id, fields in (msgs or []):
        mid = msg_id if isinstance(msg_id, str) else msg_id.decode("utf-8", errors="ignore")
        f2: Dict[str, str] = {}
        for k, v in (fields or {}).items():
            ks = k if isinstance(k, str) else k.decode("utf-8", errors="ignore")
            vs = v if isinstance(v, str) else v.decode("utf-8", errors="ignore")
            f2[ks] = vs
        out.append(StreamMsg(stream=stream, msg_id=mid, fields=f2))
    return out

