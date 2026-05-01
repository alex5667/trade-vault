# services/redis_streams_runtime.py
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Set

import redis


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


def discover_streams(r: redis.Redis, patterns: List[str], scan_count: int = 5000) -> List[str]:
    """Discover stream keys matching patterns.

    P2 fix: scan_count default 500 → 5000 (10x fewer SCAN iterations).
    Added inter-batch sleep to avoid blocking Redis event loop.
    """
    streams: Set[str] = set()
    for pat in patterns:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pat, count=scan_count)
            if keys:
                pipe = r.pipeline()
                for k in keys:
                    pipe.type(k)
                types = pipe.execute()

                for k, t in zip(keys, types):
                    if isinstance(t, (bytes, bytearray)):
                        t = t.decode("utf-8", errors="ignore")
                    if t == "stream":
                        streams.add(k if isinstance(k, str) else k.decode("utf-8", errors="ignore"))
            if cursor == 0:
                break
            # P2: yield event loop between SCAN batches (5ms)
            time.sleep(0.005)
    return sorted(streams)


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
    try:
        resp = r.xreadgroup(groupname=group, consumername=consumer, streams=stream_dict, count=count, block=block_ms) or []
    except redis.ResponseError as e:
        if "NOGROUP" in str(e):
            # Auto-create group for all streams
            for s in streams:
                ensure_group(r, s, group, start_id="$")
            # Retry once
            resp = r.xreadgroup(groupname=group, consumername=consumer, streams=stream_dict, count=count, block=block_ms) or []
        else:
            raise

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

