from __future__ import annotations

from typing import Dict, List, Tuple, Optional
import time
import redis


def ensure_group(
    r: redis.Redis
    stream: str
    group: str
    mkstream: bool = True
) -> None:
    try:
        r.xgroup_create(name=stream, groupname=group, id="0-0", mkstream=mkstream)
    except redis.exceptions.ResponseError as e:
        # BUSYGROUP means it exists
        if "BUSYGROUP" in str(e):
            return
        raise

def xreadgroup_block(
    r: redis.Redis
    stream: str
    group: str
    consumer: str
    count: int = 50
    block_ms: int = 5000
) -> List[Tuple[str, Dict[str, Dict[str, str]]]]:
    # Возвращает список: [(stream, {id: {field: value}}), ...]
    # decode_responses=True => всё уже str
    return r.xreadgroup(groupname=group, consumername=consumer, streams={stream: ">"}, count=count, block=block_ms)

def xack(r: redis.Redis, stream: str, group: str, msg_id: str) -> None:
    r.xack(stream, group, msg_id)

def xadd_trim(
    r: redis.Redis
    stream: str
    fields: Dict[str, str]
    maxlen: int
) -> str:
    # approximate trim, чтобы не убить производительность
    return r.xadd(stream, fields=fields, maxlen=maxlen, approximate=True)

def sleep_jitter(base_sec: float = 1.0, max_sec: float = 5.0) -> None:
    time.sleep(min(max_sec, base_sec))
