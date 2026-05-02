from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

try:
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore


@dataclass
class ContextCacheEntry:
    compact_hash: str
    prompt_version: str
    policy_version: str
    hits: int
    payload_bytes: int
    eligible: bool
    cache_ref: str
    first_seen_ms: int
    last_seen_ms: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ContextCacheRegistryV1:
    """Redis-backed advisory registry for context cache candidates.

    Current implementation is intentionally provider-agnostic and safe:
      - tracks repeated compact payloads
      - marks entries eligible for provider-side context caching
      - stores a stable cache_ref placeholder so downstream services can reason about reuse

    A future phase can replace stub cache_ref values with actual Vertex cache handles.
    """,
    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._min_hits = int(os.getenv("VERTEX_CONTEXT_CACHE_MIN_HITS", "3") or 3)
        self._min_payload_bytes = int(os.getenv("VERTEX_CONTEXT_CACHE_MIN_BYTES", "2048") or 2048)
        self._ttl_sec = int(os.getenv("VERTEX_CONTEXT_CACHE_TTL_SEC", str(7 * 86400)) or (7 * 86400))
        self._r = redis.Redis.from_url(redis_url, decode_responses=True) if redis is not None else None

    @staticmethod
    def _key(compact_hash: str) -> str:
        return f"metrics:ml:context_cache:entry:{compact_hash}"

    def lookup(self, compact_hash: str) -> Optional[ContextCacheEntry]:
        if self._r is None:
            return None
        try:
            raw = self._r.hgetall(self._key(compact_hash))
            if not raw:
                return None
            return ContextCacheEntry(
                compact_hash=str(raw.get("compact_hash") or compact_hash),
                prompt_version=str(raw.get("prompt_version") or "unknown"),
                policy_version=str(raw.get("policy_version") or "unknown"),
                hits=int(raw.get("hits") or 0),
                payload_bytes=int(raw.get("payload_bytes") or 0),
                eligible=str(raw.get("eligible") or "0") == "1",
                cache_ref=str(raw.get("cache_ref") or ""),
                first_seen_ms=int(raw.get("first_seen_ms") or 0),
                last_seen_ms=int(raw.get("last_seen_ms") or 0),
            )
        except Exception:
            return None

    def observe(self, *, compact_hash: str, prompt_version: str, policy_version: str, payload_bytes: int, ts_ms: int) -> ContextCacheEntry:
        if self._r is None:
            return ContextCacheEntry(compact_hash, prompt_version, policy_version, 0, payload_bytes, False, "", ts_ms, ts_ms)
        key = self._key(compact_hash)
        try:
            p = self._r.pipeline()
            p.hsetnx(key, "compact_hash", compact_hash)
            p.hsetnx(key, "prompt_version", prompt_version)
            p.hsetnx(key, "policy_version", policy_version)
            p.hsetnx(key, "first_seen_ms", int(ts_ms))
            p.hincrby(key, "hits", 1)
            p.hset(key, mapping={
                "payload_bytes": int(payload_bytes),
                "last_seen_ms": int(ts_ms),
            })
            p.expire(key, self._ttl_sec)
            p.execute()
            entry = self.lookup(compact_hash)
            if entry is None:
                entry = ContextCacheEntry(compact_hash, prompt_version, policy_version, 1, payload_bytes, False, "", ts_ms, ts_ms)
            eligible = bool(entry.hits >= self._min_hits and payload_bytes >= self._min_payload_bytes)
            cache_ref = entry.cache_ref or (f"stub:{compact_hash}" if eligible else "")
            self._r.hset(key, mapping={
                "eligible": 1 if eligible else 0,
                "cache_ref": cache_ref,
            })
            refreshed = self.lookup(compact_hash)
            return refreshed or ContextCacheEntry(compact_hash, prompt_version, policy_version, entry.hits, payload_bytes, eligible, cache_ref, entry.first_seen_ms, ts_ms)
        except Exception:
            return ContextCacheEntry(compact_hash, prompt_version, policy_version, 0, payload_bytes, False, "", ts_ms, ts_ms)


def build_cache_observation(payload: Dict[str, Any]) -> Dict[str, Any]:
    compact_hash = str(payload.get("compact_hash") or "")
    prompt_version = str(payload.get("prompt_version") or "unknown")
    policy_version = str(payload.get("policy_version") or "unknown")
    payload_bytes = len(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"))
    return {
        "compact_hash": compact_hash,
        "prompt_version": prompt_version,
        "policy_version": policy_version,
        "payload_bytes": payload_bytes,
    }
