from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib


@dataclass(frozen=True)
class DeliveryJournalSettings:
    prefix: str = os.getenv("SIGNAL_JOURNAL_PREFIX", "sig:journal")
    ttl_sec: int = int(os.getenv("SIGNAL_JOURNAL_TTL_SEC", "172800"))  # 48h
    index_key: str = os.getenv("SIGNAL_JOURNAL_INDEX_KEY", "sig:journal:idx")


_LUA_INIT = r"""
-- KEYS[1] = journal_key
-- ARGV[1] = desired_json
-- ARGV[2] = now_ms
-- ARGV[3] = ttl_sec
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('HSET', KEYS[1], 'created_ms', ARGV[2])
  redis.call('HSET', KEYS[1], 'desired', ARGV[1])
end
redis.call('HSET', KEYS[1], 'touched_ms', ARGV[2])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
"""


class DeliveryJournal:
    """
    Delivery journal per sid:
      - desired targets list
      - delivered:* markers (diagnostics + selective replay)
      - last_error:* (optional)
    """

    def __init__(self, redis_client: Any, *, settings: DeliveryJournalSettings | None = None) -> None:
        self.redis = redis_client
        self.settings = settings or DeliveryJournalSettings()
        self._sha_init: str | None = None

    def key(self, sid: str) -> str:
        return f"{self.settings.prefix}:{sid}"

    def _idx_key(self) -> str:
        return str(self.settings.index_key)

    def _touch_index(self, sid: str, now_ms: int) -> None:
        # ZADD idx now_ms sid ; keep idx bounded best-effort
        try:
            self.redis.zadd(self._idx_key(), {sid: float(now_ms)})
        except Exception:
            return

    def drop_index(self, sid: str) -> None:
        with contextlib.suppress(Exception):
            self.redis.zrem(self._idx_key(), sid)

    def _ensure_sha_init(self) -> str:
        if self._sha_init:
            return self._sha_init
        self._sha_init = str(self.redis.script_load(_LUA_INIT))
        return self._sha_init

    def init(self, sid: str, desired_targets: list[str]) -> None:
        k = self.key(sid)
        now_ms = get_ny_time_millis()
        desired_json = json.dumps(sorted(set(desired_targets)), ensure_ascii=False, separators=(",", ":"))
        ttl = int(self.settings.ttl_sec)
        sha = self._ensure_sha_init()
        try:
            self.redis.evalsha(sha, 1, k, desired_json, str(now_ms), str(ttl))
        except Exception:
            with contextlib.suppress(Exception):
                self.redis.eval(_LUA_INIT, 1, k, desired_json, str(now_ms), str(ttl))
        self._touch_index(sid, now_ms)

    def mark_delivered(self, sid: str, target: str) -> None:
        k = self.key(sid)
        now_ms = get_ny_time_millis()
        try:
            self.redis.hset(k, f"delivered:{target}", "1")
            self.redis.hset(k, f"delivered_ms:{target}", str(now_ms))
            self.redis.hset(k, "touched_ms", str(now_ms))
            self.redis.expire(k, int(self.settings.ttl_sec))
        except Exception:
            pass
        self._touch_index(sid, now_ms)

    def record_error(self, sid: str, target: str, err: str) -> None:
        k = self.key(sid)
        now_ms = get_ny_time_millis()
        try:
            self.redis.hset(k, f"last_error:{target}", (err or "")[:4000])
            self.redis.hset(k, f"last_error_ms:{target}", str(now_ms))
            self.redis.hset(k, "touched_ms", str(now_ms))
            self.redis.expire(k, int(self.settings.ttl_sec))
        except Exception:
            pass
        self._touch_index(sid, now_ms)

    def delivered_set(self, sid: str) -> set[str]:
        k = self.key(sid)
        try:
            d: dict[str, Any] = self.redis.hgetall(k) or {}
        except Exception:
            return set()
        out: set[str] = set()
        for kk, vv in d.items():
            if isinstance(kk, (bytes, bytearray)):
                kk = kk.decode("utf-8", errors="ignore")
            if not isinstance(kk, str):
                continue
            if kk.startswith("delivered:"):
                out.add(kk.split("delivered:", 1)[1])
        return out

    def desired_targets(self, sid: str) -> list[str]:
        k = self.key(sid)
        try:
            raw = self.redis.hget(k, "desired")
        except Exception:
            return []
        if not raw:
            return []
        try:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="ignore")
            if isinstance(raw, str):
                v = json.loads(raw)
                if isinstance(v, list):
                    return [str(x) for x in v]
        except Exception:
            return []
        return []

    def is_complete(self, sid: str) -> tuple[bool, list[str], set[str]]:
        desired = self.desired_targets(sid)
        delivered = self.delivered_set(sid)
        if not desired:
            return True, desired, delivered
        return all(t in delivered for t in desired), desired, delivered
