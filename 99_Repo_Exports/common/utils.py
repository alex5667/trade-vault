# common/utils.py
"""
Common utility functions for Redis operations and JSON handling.
Used across all signal processing modules.
"""
from __future__ import annotations
import json
import os
import time
from typing import Any, Dict, List, Tuple
import redis

def now_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)

def to_json(obj: Any) -> str:
    """Serialize object to JSON string (compact format)."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def from_json(s: str) -> Any:
    """Deserialize JSON string to object."""
    return json.loads(s) if s else None

def get_redis() -> redis.Redis:
    """
    Create and return Redis client connection.
    
    Uses REDIS_URL environment variable.
    Default: redis://scanner-redis-worker-1:6379/0
    
    Returns:
        Redis client with decode_responses=True
    """
    url = os.getenv("REDIS_URL", "redis://scanner-redis-worker-1:6379/0")
    # Максимально либеральная конфигурация для стабильности
    return redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=60,  # Очень большой timeout для первого подключения  
        socket_timeout=120,  # Очень большой timeout для операций
        socket_keepalive=True,
        max_connections=50
    )

def xadd_json(
    r: redis.Redis,
    stream: str,
    obj: Dict[str, Any],
    maxlen: int | None = None
) -> str:
    """
    Add JSON-encoded message to Redis stream.
    
    Args:
        r: Redis client
        stream: Stream name
        obj: Dictionary to serialize as JSON
        maxlen: Optional max length (for trimming)
    
    Returns:
        Message ID
    """
    fields = {"data": to_json(obj)}
    args = {"maxlen": maxlen, "approximate": True} if maxlen else {}
    return r.xadd(stream, fields, **args)

def xread_latest(
    r: redis.Redis,
    streams: Dict[str, str],
    block_ms: int = 2000
):
    """
    Read latest messages from Redis streams.
    
    Args:
        r: Redis client
        streams: Dict mapping stream names to last message IDs
                 Use '>' for consumer groups, or '$' for xread start-from-new
        block_ms: Blocking timeout in milliseconds
    
    Returns:
        List of (stream_name, messages) tuples
    """
    return r.xread(streams=streams, block=block_ms, count=None)

def xrevrange_json(
    r: redis.Redis,
    stream: str,
    count: int = 100
) -> List[Dict[str, Any]]:
    """
    Read latest messages from stream in reverse order (newest first).
    
    Args:
        r: Redis client
        stream: Stream name
        count: Maximum number of messages
    
    Returns:
        List of dicts with 'id' and 'payload' keys
    """
    items = r.xrevrange(stream, max="+", min="-", count=count)
    out = []
    for _id, fields in items:
        payload = fields.get("data") or fields.get("text") or fields
        if isinstance(payload, str):
            try:
                payload = from_json(payload)
            except Exception:
                payload = {"text": payload}
        out.append({"id": _id, "payload": payload})
    return out

