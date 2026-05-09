from __future__ import annotations

import json
import os
import time
from typing import Any

import redis

from common.log import setup_logger
from common.transient import is_transient_error
from core.dual_redis_client import get_dual_signals_redis
from core.redis_keys import RedisStreams as RS
from core.redis_stream_consumer import SyncRedisStreamHelper
import contextlib

logger = setup_logger("SignalBridgeDispatcher")

_LUA_DUAL_XADD_AND_MARK = r"""
-- KEYS[1]=marker_key, KEYS[2]=stream
-- ARGV[1]=marker_ttl_sec, ARGV[2]=maxlen, ARGV[3]=sid, ARGV[4]=payload_json
local ttl = tonumber(ARGV[1]) or 86400
local maxlen = tonumber(ARGV[2]) or 5000
local sid = ARGV[3] or ""
local payload = ARGV[4] or ""
if payload == "" then return {1, "skip"} end
if redis.call('GET', KEYS[1]) then return {1, "dedup"} end
local t = redis.call('TIME')
local now_ms = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)
local id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', tostring(maxlen), '*', 'sid', sid, 'data', payload)
redis.call('SET', KEYS[1], tostring(now_ms), 'EX', tostring(ttl))
return {1, id}
"""

class SignalBridgeDispatcher:
    """
    Reads bridge streams from MAIN redis (consumer-group), delivers into DUAL redis atomically (Lua),
    then ACKs bridge message in MAIN redis.
    """
    def __init__(self, *, mode: str):
        self.mode = mode  # "notify" or "manual"

        self.main_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.main = redis.from_url(self.main_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=15, health_check_interval=0)

        self.dual = get_dual_signals_redis()

        self.group = os.getenv("SIGNAL_BRIDGE_GROUP", f"signals-bridge-{mode}-group")
        self.consumer = os.getenv("SIGNAL_BRIDGE_CONSUMER", f"bridge-{mode}-{os.getpid()}")

        self.bridge_notify_stream = os.getenv("SIGNAL_BRIDGE_NOTIFY_STREAM", RS.SIGNAL_BRIDGE_NOTIFY)
        self.bridge_manual_stream = os.getenv("SIGNAL_BRIDGE_MANUAL_STREAM", RS.SIGNAL_BRIDGE_MANUAL)

        self.target_notify_stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)

        self.read_count = int(os.getenv("SIGNAL_BRIDGE_READ_COUNT", "200"))
        self.read_block_ms = int(os.getenv("SIGNAL_BRIDGE_READ_BLOCK_MS", "1000"))
        self.maxlen_target = int(os.getenv("SIGNAL_BRIDGE_TARGET_MAXLEN", "5000"))
        self.marker_ttl_sec = int(os.getenv("SIGNAL_DELIVERY_TTL_SEC", "86400"))

        self.marker_prefix = os.getenv("SIGNAL_DELIVERY_MARKER_PREFIX", "signal:deliver:v1")
        self.claim_min_idle_ms = int(os.getenv("SIGNAL_BRIDGE_CLAIM_MIN_IDLE_MS", "60000"))
        self.claim_count = int(os.getenv("SIGNAL_BRIDGE_CLAIM_COUNT", "200"))
        self.claim_every_ms = int(os.getenv("SIGNAL_BRIDGE_CLAIM_EVERY_MS", "5000"))
        self._last_claim_mono = 0.0
        self._claim_cursor = "0-0"

        self._sha: str | None = None

    def _ensure_script(self) -> str:
        if self._sha:
            return self._sha
        self._sha = self.dual.script_load(_LUA_DUAL_XADD_AND_MARK)
        return self._sha

    def _marker_key(self, target: str, sid: str) -> str:
        # marker stored in DUAL redis to avoid "marker-before-delivery" loss on that side
        return f"{self.marker_prefix}:{target}:{sid}"

    def run(self) -> None:
        stream = self.bridge_notify_stream if self.mode == "notify" else self.bridge_manual_stream
        helper = SyncRedisStreamHelper(self.main, self.group, self.consumer)
        helper.ensure_group(stream, start_id="0")

        logger.info("SignalBridgeDispatcher started. mode=%s stream=%s group=%s consumer=%s", self.mode, stream, self.group, self.consumer)

        while True:
            try:
                msgs = helper.read({stream: ">"}, count=self.read_count, block=self.read_block_ms, recover_start_id="0")
                if not msgs:
                    self._maybe_claim_pending(helper, stream)
                    continue
                for _, items in msgs:
                    for msg_id, fields in items:
                        ok = self._handle_one(stream, msg_id, fields)
                        if ok:
                            with contextlib.suppress(Exception):
                                helper.ack(stream, msg_id)
            except KeyboardInterrupt:
                logger.info("SignalBridgeDispatcher stopped")
                return
            except Exception as e:
                logger.error("Bridge loop error: %s", e, exc_info=True)
                time.sleep(1)

    def _maybe_claim_pending(self, helper: SyncRedisStreamHelper, stream: str) -> None:
        now = time.monotonic()
        if (now - self._last_claim_mono) * 1000 < self.claim_every_ms:
            return
        self._last_claim_mono = now
        try:
            next_id, msgs = helper.claim_pending(stream, min_idle_ms=self.claim_min_idle_ms, start_id=self._claim_cursor, count=self.claim_count)
            if (not msgs) and (next_id == "0-0"):
                pass
            else:
                self._claim_cursor = next_id
        except Exception as e:
            if is_transient_error(e):
                return
            raise
        if not msgs:
            return
        for m in msgs:
            mid = getattr(m, "msg_id", "") or getattr(m, "id", "")
            fields = getattr(m, "fields", None) or getattr(m, "data", None) or {}
            ok = self._handle_one(stream, mid, fields)
            if ok:
                with contextlib.suppress(Exception):
                    helper.ack(stream, mid)

    def _handle_one(self, stream: str, msg_id: str, fields: dict[str, Any]) -> bool:
        raw = fields.get("data")
        if not raw:
            return True
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if not isinstance(raw, str):
            return True
        try:
            payload = json.loads(raw)
        except Exception:
            return True

        if self.mode == "notify":
            sid = str(payload.get("sid") or payload.get("meta", {}).get("sid") or payload.get("signal_id") or "")
            if not sid:
                # fallback: use msg_id, but stable is better
                sid = str(payload.get("id") or msg_id)
            marker = self._marker_key("notify", sid)
            payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            sha = self._ensure_script()
            try:
                res = self.dual.evalsha(sha, 2, marker, self.target_notify_stream, str(self.marker_ttl_sec), str(self.maxlen_target), sid, payload_json)
            except Exception:
                res = self.dual.eval(_LUA_DUAL_XADD_AND_MARK, 2, marker, self.target_notify_stream, str(self.marker_ttl_sec), str(self.maxlen_target), sid, payload_json)
            return bool(res and int(res[0]) == 1)

        # manual: payload={"stream": "...", "data": {...}}
        sid = str(payload.get("sid") or payload.get("data", {}).get("sid") or msg_id)
        target_stream = (payload.get("stream") or "")
        data = payload.get("data")
        if not target_stream or data is None:
            return True
        marker = self._marker_key("manual", sid)
        payload_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        sha = self._ensure_script()
        try:
            res = self.dual.evalsha(sha, 2, marker, target_stream, str(self.marker_ttl_sec), str(self.maxlen_target), sid, payload_json)
        except Exception:
            res = self.dual.eval(_LUA_DUAL_XADD_AND_MARK, 2, marker, target_stream, str(self.marker_ttl_sec), str(self.maxlen_target), sid, payload_json)
        return bool(res and int(res[0]) == 1)

if __name__ == "__main__":
    mode = os.getenv("SIGNAL_BRIDGE_MODE", "notify").strip().lower()
    SignalBridgeDispatcher(mode=mode).run()
