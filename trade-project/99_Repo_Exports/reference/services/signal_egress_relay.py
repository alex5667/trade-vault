from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional, List

import redis

from core.redis_stream_consumer import SyncRedisStreamHelper
from core.redis_keys import RedisStreams as RS
from core.dual_redis_client import get_dual_signals_redis
from core.redis_safe_connect import apply_redis_connection_patches
from common.log import setup_logger
from common.transient import is_transient_error

logger = setup_logger("SignalEgressRelay")
apply_redis_connection_patches()


_LUA_NOTIFY_XADD_FIELDS_THEN_MARK = r"""
-- KEYS[1] marker_key
-- KEYS[2] dest_stream
-- KEYS[3] counter_key
-- ARGV[1] marker_ttl_sec
-- ARGV[2] maxlen
-- ARGV[3] every_n
-- ARGV[4] use_counter ("1" or "0")
-- ARGV[5..] field/value pairs for XADD
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
local every = tonumber(ARGV[3]) or 1
local use_counter = tostring(ARGV[4] or "0")
local send = 1
if every > 1 and use_counter == "1" then
  local c = redis.call('INCR', KEYS[3])
  if (c % every) ~= 0 then
    send = 0
  end
end
local id = ""
if send == 1 then
  id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', unpack(ARGV, 5, #ARGV))
end
redis.call('SETEX', KEYS[1], ARGV[1], '1')
return {1, send, id}
"""


_LUA_XADD_DATA_THEN_MARK = r"""
-- KEYS[1] marker_key
-- KEYS[2] dest_stream
-- ARGV[1] marker_ttl_sec
-- ARGV[2] maxlen
-- ARGV[3] payload_json
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
local id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', 'data', ARGV[3])
redis.call('SETEX', KEYS[1], ARGV[1], '1')
return {1, id}
"""


_LUA_ENV_FINALIZE = r"""
-- KEYS[1] = env_done_key
-- KEYS[2] = env_req_set
-- ARGV[1] = ttl_sec
-- ARGV[2] = done_prefix
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {1}
end
local req = redis.call('SMEMBERS', KEYS[2])
for _,t in ipairs(req) do
  local k = ARGV[2] .. t
  if redis.call('EXISTS', k) == 0 then
    return {0}
  end
end
redis.call('SETEX', KEYS[1], ARGV[1], '1')
return {1}
"""


class SignalEgressRelay:
    """
    Consumes MAIN egress streams and delivers into destination redis (dual) exactly-once-per-target:
      - stream:signals:egress:notify  -> notify:telegram (dual)
      - stream:signals:egress:manual  -> stream:manual-signals (dual)

    Properties:
      - main side: at-least-once (consumer-group + pending-claim)
      - dual side: idempotent exactly-once via Lua (XADD/marker on same server)
      - env finalize: updates MAIN env:done:<sid>:{notify|manual} and tries to finalize env.
    """

    def __init__(self) -> None:
        self.main_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.main = redis.from_url(self.main_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=15, health_check_interval=0)

        try:
            self.dual = get_dual_signals_redis()
        except Exception as e:
            self.dual = None
            logger.error("dual redis init failed: %s", e)

        self.egress_notify_stream = os.getenv("SIGNAL_EGRESS_NOTIFY_STREAM", "stream:signals:egress:notify")
        self.egress_manual_stream = os.getenv("SIGNAL_EGRESS_MANUAL_STREAM", "stream:signals:egress:manual")

        self.group = os.getenv("SIGNAL_EGRESS_GROUP", "signals-egress-group")
        self.consumer = os.getenv("SIGNAL_EGRESS_CONSUMER", f"relay-{os.getpid()}")

        self.read_count = int(os.getenv("SIGNAL_EGRESS_READ_COUNT", "200"))
        self.read_block_ms = int(os.getenv("SIGNAL_EGRESS_READ_BLOCK_MS", "1000"))
        self.claim_min_idle_ms = int(os.getenv("SIGNAL_EGRESS_CLAIM_MIN_IDLE_MS", "30000"))
        self.claim_count = int(os.getenv("SIGNAL_EGRESS_CLAIM_COUNT", "200"))

        self.marker_ttl_sec = int(os.getenv("SIGNAL_DELIVERY_TTL_SEC", "86400"))
        self.env_state_ttl_sec = int(os.getenv("SIGNAL_ENV_STATE_TTL_SEC", "172800"))
        self.marker_prefix = os.getenv("DELIVER_MARKER_PREFIX", "deliver:v2")  # use same namespace on dual

        self._sha_notify: Optional[str] = None
        self._sha_data: Optional[str] = None
        self._sha_finalize: Optional[str] = None

        self._pending_start: Dict[str, str] = {
            self.egress_notify_stream: "0-0",
            self.egress_manual_stream: "0-0",
        }

        self._last_diag = 0.0
        self._diag_every_sec = float(os.getenv("SIGNAL_EGRESS_DIAG_EVERY_SEC", "10"))

    # ---------- MAIN env-state keys ----------
    def _env_req_key(self, sid: str) -> str:
        return f"env:req:{sid}"

    def _env_done_key(self, sid: str) -> str:
        return f"env:done:{sid}"

    def _env_done_prefix(self, sid: str) -> str:
        return f"env:done:{sid}:"

    def _env_done_target_key(self, sid: str, target: str) -> str:
        return f"env:done:{sid}:{target}"

    # ---------- DUAL markers ----------
    def _dual_marker(self, target: str, sid: str) -> str:
        return f"{self.marker_prefix}:{target}:{sid}"

    def _ensure_sha(self, which: str) -> str:
        assert self.dual is not None
        if which == "notify":
            if self._sha_notify:
                return self._sha_notify
            self._sha_notify = self.dual.script_load(_LUA_NOTIFY_XADD_FIELDS_THEN_MARK)
            return self._sha_notify
        if which == "data":
            if self._sha_data:
                return self._sha_data
            self._sha_data = self.dual.script_load(_LUA_XADD_DATA_THEN_MARK)
            return self._sha_data
        raise ValueError(which)

    def _ensure_finalize_sha(self) -> str:
        if self._sha_finalize:
            return self._sha_finalize
        self._sha_finalize = self.main.script_load(_LUA_ENV_FINALIZE)
        return self._sha_finalize

    def _try_finalize(self, sid: str) -> bool:
        sha = self._ensure_finalize_sha()
        try:
            res = self.main.evalsha(
                sha, 2,
                self._env_done_key(sid),
                self._env_req_key(sid),
                str(self.env_state_ttl_sec),
                self._env_done_prefix(sid),
            )
        except Exception:
            res = self.main.eval(
                _LUA_ENV_FINALIZE, 2,
                self._env_done_key(sid),
                self._env_req_key(sid),
                str(self.env_state_ttl_sec),
                self._env_done_prefix(sid),
            )
        try:
            return bool(int(res[0]) == 1)
        except Exception:
            return False

    # ---------- deliveries ----------
    def _deliver_notify(self, msg: Dict[str, Any]) -> None:
        assert self.dual is not None

        sid = str(msg.get("sid") or "")
        dest_stream = str(msg.get("dest_stream") or os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM))
        payload = msg.get("payload") or {}
        every_n = int(msg.get("every_n") or 1)
        counter_key = str(msg.get("counter_key") or os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", "notify:telegram:signal_counter"))

        if not sid:
            raise ValueError("missing sid")
        if not isinstance(payload, dict) or not payload:
            # treat as no-op; still mark done (otherwise env hangs)
            self.main.setex(self._env_done_target_key(sid, "notify"), self.env_state_ttl_sec, "1")
            self._try_finalize(sid)
            return

        marker = self._dual_marker("notify", sid)
        sha = self._ensure_sha("notify")

        # Build ARGV: marker_ttl, maxlen, every_n, use_counter, field/value...
        argv: List[str] = [
            str(self.marker_ttl_sec),
            "500",
            str(max(1, every_n)),
            "1",
        ]
        for k, v in payload.items():
            argv.append(str(k))
            argv.append("" if v is None else str(v))

        try:
            res = self.dual.evalsha(sha, 3, marker, dest_stream, counter_key, *argv)
        except Exception:
            res = self.dual.eval(_LUA_NOTIFY_XADD_FIELDS_THEN_MARK, 3, marker, dest_stream, counter_key, *argv)

        # res: {0} already done OR {1, send, id}
        # Mark MAIN env notify done regardless (if dual marker exists, it's delivered/decided already).
        self.main.setex(self._env_done_target_key(sid, "notify"), self.env_state_ttl_sec, "1")
        self._try_finalize(sid)

    def _deliver_manual(self, msg: Dict[str, Any]) -> None:
        assert self.dual is not None

        sid = str(msg.get("sid") or "")
        dest_stream = str(msg.get("dest_stream") or "")
        payload = msg.get("payload")
        if not sid or not dest_stream:
            raise ValueError("missing sid/dest_stream")

        marker = self._dual_marker("manual", sid)
        sha = self._ensure_sha("data")
        payload_json = json.dumps(payload, ensure_ascii=False)

        try:
            _ = self.dual.evalsha(sha, 2, marker, dest_stream, str(self.marker_ttl_sec), "2000", payload_json)
        except Exception:
            _ = self.dual.eval(_LUA_XADD_DATA_THEN_MARK, 2, marker, dest_stream, str(self.marker_ttl_sec), "2000", payload_json)

        self.main.setex(self._env_done_target_key(sid, "manual"), self.env_state_ttl_sec, "1")
        self._try_finalize(sid)

    # ---------- loop ----------
    def _parse(self, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raw = fields.get("data")
        if not raw:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, dict):
            return raw
        return None

    def _diag(self, helper: SyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if now - self._last_diag < self._diag_every_sec:
            return
        self._last_diag = now
        try:
            p1 = helper.pending_details(self.egress_notify_stream)
            p2 = helper.pending_details(self.egress_manual_stream)
            logger.info("egress pending notify=%s manual=%s", p1, p2)
        except Exception:
            pass

    def run(self) -> None:
        if self.dual is None:
            logger.error("dual redis is None; cannot run egress relay")
            return

        helper = SyncRedisStreamHelper(self.main, self.group, self.consumer, group_start_id="0")
        helper.ensure_groups([self.egress_notify_stream, self.egress_manual_stream])
        logger.info("SignalEgressRelay started group=%s consumer=%s", self.group, self.consumer)

        streams = [self.egress_notify_stream, self.egress_manual_stream]

        while True:
            try:
                self._diag(helper)

                # 0) pending recovery
                for s in streams:
                    try:
                        nxt, pend = helper.claim_pending(
                            s,
                            min_idle_ms=self.claim_min_idle_ms,
                            start_id=self._pending_start.get(s, "0-0"),
                            count=self.claim_count,
                        )
                        if (not pend) and (nxt == "0-0"):
                            pass
                        else:
                            self._pending_start[s] = nxt
                        for m in pend or []:
                            msg_id = getattr(m, "msg_id", "") or ""
                            fields = getattr(m, "fields", {}) or {}
                            self._handle_one(s, msg_id, fields)
                            try:
                                helper.ack(s, msg_id)
                            except Exception as ack_e:
                                logger.warning("ACK failed (pending) %s %s", msg_id, ack_e)
                    except Exception as e:
                        if is_transient_error(e):
                            time.sleep(0.2)
                            continue
                        logger.warning("pending claim error stream=%s err=%s", s, e)

                # 1) read new
                res = helper.read({self.egress_notify_stream: ">", self.egress_manual_stream: ">"}, count=self.read_count, block=self.read_block_ms, recover_start_id="0")
                if not res:
                    continue
                for stream_name, items in res:
                    for msg_id, fields in items:
                        self._handle_one(stream_name, msg_id, fields)
                        try:
                            helper.ack(stream_name, msg_id)
                        except Exception as ack_e:
                            logger.warning("ACK failed %s %s", msg_id, ack_e)

            except KeyboardInterrupt:
                logger.info("SignalEgressRelay stopped")
                return
            except Exception as e:
                logger.error("Relay loop error: %s", e, exc_info=True)
                time.sleep(1)

    def _handle_one(self, stream_name: str, msg_id: str, fields: Dict[str, Any]) -> None:
        msg = self._parse(fields)
        if not msg:
            logger.warning("bad egress msg stream=%s id=%s fields=%s", stream_name, msg_id, fields)
            return
        try:
            if stream_name == self.egress_notify_stream:
                self._deliver_notify(msg)
            elif stream_name == self.egress_manual_stream:
                self._deliver_manual(msg)
            else:
                logger.warning("unknown egress stream=%s id=%s", stream_name, msg_id)
        except Exception as e:
            # do not ACK here; caller will ACK only on success. Leaving pending enables retry + dual idempotency.
            if is_transient_error(e):
                raise
            logger.warning("egress handle failed stream=%s id=%s err=%s", stream_name, msg_id, e)
            raise


if __name__ == "__main__":
    SignalEgressRelay().run()