from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, Optional

from common.log import setup_logger
from common.transient import is_transient_error
from core.redis_stream_consumer import SyncRedisStreamHelper
from core.dual_redis_client import get_dual_signals_redis

logger = setup_logger("DualSignalDeliverer")


_LUA_DELIVER_ONE_AND_ACK = r"""
-- Atomic: deliver target stream + set delivery marker + done + XACK (no-loss, no parallel dupes)
--
-- KEYS[1] = outbox_stream
-- KEYS[2] = done_key        (dual:done:{msg_id})
-- KEYS[3] = marker_key      (deliver:{target}:{sid})
-- KEYS[4] = target_stream   (notify stream OR manual stream)
-- KEYS[5] = counter_key     (optional, can be "")
--
-- ARGV[1] = group
-- ARGV[2] = msg_id
-- ARGV[3] = marker_ttl_sec
-- ARGV[4] = done_ttl_sec
-- ARGV[5] = maxlen
-- ARGV[6] = every_n         (int, 1 => always)
-- ARGV[7] = is_notify       ("1" or "0")
-- ARGV[8..] = payload_kv... (k1,v1,k2,v2,...)

local function xack_only()
  redis.pcall("SET", KEYS[2], "1", "EX", ARGV[4])
  local a = redis.pcall("XACK", KEYS[1], ARGV[1], ARGV[2])
  if type(a) == "table" and a["err"] then
    return {0, "xack_fail"}
  end
  return {1, "acked"}
end

-- done fast-path
if redis.call("EXISTS", KEYS[2]) == 1 then
  return xack_only()
end

-- already delivered fast-path
if redis.call("EXISTS", KEYS[3]) == 1 then
  return xack_only()
end

-- notify gating
if ARGV[7] == "1" and tonumber(ARGV[6]) and tonumber(ARGV[6]) > 1 and KEYS[5] ~= nil and KEYS[5] ~= "" then
  local c = redis.call("INCR", KEYS[5])
  if (c % tonumber(ARGV[6])) ~= 0 then
    -- считаем "доставлено" (чтобы не ретраить бесконечно), но не отправляем
    redis.pcall("SET", KEYS[3], "1", "EX", ARGV[3])
    return xack_only()
  end
end

-- build XADD args
local args = {"XADD", KEYS[4], "MAXLEN", "~", tostring(tonumber(ARGV[5]) or 1000), "*"}
for i = 8, #ARGV do
  table.insert(args, ARGV[i])
end

local id = redis.call(unpack(args))

-- set marker; rollback XADD if marker-set fails
local ok = redis.pcall("SET", KEYS[3], "1", "EX", ARGV[3])
if type(ok) == "table" and ok["err"] then
  redis.pcall("XDEL", KEYS[4], id)
  return {0, "marker_fail_rollback"}
end

return xack_only()
"""


class DualSignalDeliverer:
    """
    Читает dual-outbox и доставляет в dual targets (notify/manual) с exactly-once:
      - deliver marker ставится ПОСЛЕ XADD (в Lua, атомарно)
      - done:{msg_id} позволяет "ack-only" при xack timeouts
      - claim_pending подбирает зависшие сообщения при смерти consumer/ack-fail
    """

    def __init__(self) -> None:
        self.dual = get_dual_signals_redis()
        self.outbox_stream = os.getenv("DUAL_SIGNAL_OUTBOX_STREAM", "stream:signals:dual-outbox")
        self.group = os.getenv("DUAL_SIGNAL_OUTBOX_GROUP", "signals-dual-outbox-group")
        self.consumer = os.getenv("DUAL_SIGNAL_OUTBOX_CONSUMER", f"dual-deliverer-{os.getpid()}")

        self.read_count = int(os.getenv("DUAL_SIGNAL_OUTBOX_READ_COUNT", "200"))
        self.read_block_ms = int(os.getenv("DUAL_SIGNAL_OUTBOX_READ_BLOCK_MS", "1000"))

        self.marker_ttl_sec = int(os.getenv("SIGNAL_DELIVERY_TTL_SEC", "86400"))
        self.done_ttl_sec = int(os.getenv("DUAL_SIGNAL_DONE_TTL_SEC", "86400"))

        self.claim_min_idle_ms = int(os.getenv("DUAL_SIGNAL_CLAIM_MIN_IDLE_MS", "60000"))
        self.claim_count = int(os.getenv("DUAL_SIGNAL_CLAIM_COUNT", "200"))
        self.claim_every_ms = int(os.getenv("DUAL_SIGNAL_CLAIM_EVERY_MS", "1000"))
        self._last_claim_ms = 0
        self._claim_start_id = "0-0"

        self.done_prefix = os.getenv("DUAL_SIGNAL_DONE_PREFIX", "dual:done:")
        self._sha: Optional[str] = None

    def _done_key(self, msg_id: str) -> str:
        return f"{self.done_prefix}{msg_id}"

    def _ensure_script(self) -> str:
        if self._sha:
            return self._sha
        self._sha = self.dual.script_load(_LUA_DELIVER_ONE_AND_ACK)
        return self._sha

    @staticmethod
    def _kv_flat(payload: Dict[str, Any]) -> list[str]:
        out: list[str] = []
        for k, v in (payload or {}).items():
            out.append(str(k))
            if isinstance(v, (bytes, bytearray)):
                out.append(v.decode("utf-8", errors="ignore"))
            else:
                out.append(str(v))
        return out

    def _deliver_one(self, msg_id: str, fields: Dict[str, Any]) -> bool:
        raw = fields.get("data")
        if not raw:
            return True
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        env = json.loads(raw) if isinstance(raw, str) else raw

        sid = str(env.get("sid") or "")
        target = str(env.get("target") or "")
        meta = env.get("meta") or {}
        payload = env.get("payload") or {}

        if not sid or target not in ("notify", "manual"):
            # bad envelope: ack to avoid PEL poison
            self.dual.xack(self.outbox_stream, self.group, msg_id)
            return True

        if target == "notify":
            stream = str(meta.get("notify_stream") or os.getenv("NOTIFY_STREAM", "notify:telegram"))
            maxlen = int(meta.get("maxlen") or 500)
            every_n = int(meta.get("every_n") or 1)
            counter_key = str(meta.get("counter_key") or os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", "notify:telegram:signal_counter"))
            marker_key = f"deliver:notify:{sid}"
            is_notify = "1"
        else:
            stream = str(meta.get("manual_stream") or "")
            if not stream:
                self.dual.xack(self.outbox_stream, self.group, msg_id)
                return True
            maxlen = int(meta.get("maxlen") or 2000)
            every_n = 1
            counter_key = ""
            marker_key = f"deliver:manual:{sid}"
            is_notify = "0"

        sha = self._ensure_script()
        kv = self._kv_flat(payload)

        res = self.dual.evalsha(
            sha,
            5,
            self.outbox_stream,
            self._done_key(msg_id),
            marker_key,
            stream,
            counter_key,
            self.group,
            msg_id,
            str(int(self.marker_ttl_sec)),
            str(int(self.done_ttl_sec)),
            str(int(maxlen)),
            str(int(every_n)),
            is_notify,
            *kv,
        )
        return bool(res) and int(res[0]) == 1

    def _maybe_claim_pending(self, helper: SyncRedisStreamHelper) -> None:
        now_ms = get_ny_time_millis()
        if now_ms - self._last_claim_ms < self.claim_every_ms:
            return
        self._last_claim_ms = now_ms

        try:
            next_id, msgs = helper.claim_pending(
                self.outbox_stream,
                min_idle_ms=self.claim_min_idle_ms,
                start_id=self._claim_start_id,
                count=self.claim_count,
            )
            if (not msgs) and (next_id == "0-0"):
                next_id = self._claim_start_id
            self._claim_start_id = next_id
        except Exception as e:
            if is_transient_error(e):
                return
            raise

        for m in msgs or []:
            try:
                self._deliver_one(m.msg_id, m.fields)
            except Exception as e:
                if is_transient_error(e):
                    break
                logger.error("dual pending fatal msg=%s: %s", m.msg_id, e, exc_info=True)

    def run(self) -> None:
        helper = SyncRedisStreamHelper(self.dual, self.group, self.consumer, recovery_start_id="0")
        helper.ensure_group(self.outbox_stream, start_id="0")
        self._ensure_script()
        logger.info("DualSignalDeliverer started stream=%s group=%s consumer=%s", self.outbox_stream, self.group, self.consumer)

        while True:
            try:
                self._maybe_claim_pending(helper)
                messages = helper.read({self.outbox_stream: ">"}, count=self.read_count, block=self.read_block_ms)
                if not messages:
                    continue
                for stream, items in messages:
                    for msg_id, fields in items:
                        try:
                            self._deliver_one(msg_id, fields)
                        except Exception as e:
                            if is_transient_error(e):
                                continue
                            logger.error("dual deliver fatal msg=%s: %s", msg_id, e, exc_info=True)
            except KeyboardInterrupt:
                logger.info("DualSignalDeliverer stopped")
                return
            except Exception as e:
                logger.error("DualSignalDeliverer loop error: %s", e, exc_info=True)
                time.sleep(1)


if __name__ == "__main__":
    DualSignalDeliverer().run()