from utils.time_utils import get_ny_time_millis
# services/signal_dispatcher.py
import json
import os
import time
from typing import Any, Dict, Optional, Tuple, List

import redis

from core.redis_stream_consumer import SyncRedisStreamHelper
from core.redis_safe_connect import apply_redis_connection_patches
from common.log import setup_logger
from common.transient import is_transient_error

logger = setup_logger("SignalDispatcher")
apply_redis_connection_patches()

_LUA_XADD_FIELDS_THEN_MARK = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = stream
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = maxlen
-- ARGV[3...] = field/value pairs
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
local id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', unpack(ARGV, 3))
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1])
if not ok then
  redis.call('XDEL', KEYS[2], id)
  return {0}
end
return {1, id}
"""

_LUA_SETEX_THEN_MARK = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = value_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = value_ttl_sec
-- ARGV[3] = value_json
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
redis.call('SETEX', KEYS[2], ARGV[2], ARGV[3])
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1])
if not ok then
  redis.call('DEL', KEYS[2])
  return {0}
end
return {1}
"""

_LUA_NOTIFY_GATE = r"""
-- KEYS[1] = marker_key
-- KEYS[2] = notify_stream
-- KEYS[3] = counter_key
-- ARGV[1] = marker_ttl_sec
-- ARGV[2] = maxlen
-- ARGV[3] = every_n
-- ARGV[4...] = field/value pairs
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0, 0}
end
local n = redis.call('INCR', KEYS[3])
local every = tonumber(ARGV[3]) or 1
local send = 1
if every > 1 and (n % every ~= 0) then
  send = 0
end
local id = nil
if send == 1 then
  id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', unpack(ARGV, 4))
end
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1])
if not ok then
  if id then redis.call('XDEL', KEYS[2], id) end
  return {0, send}
end
return {1, send}
"""


class SignalDispatcher:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

        # simplified connect
        max_retries = 5
        base_delay = 2.0
        ping_success = False
        for attempt in range(max_retries):
            if attempt > 0:
                delay = base_delay * (attempt + 1)
                logger.info("⏳ Ожидание перед повторной попыткой подключения (%s/%s): %.1fs", attempt + 1, max_retries, delay)
                time.sleep(delay)
            try:
                self.redis = redis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=15,
                    health_check_interval=0,
                )
                result = self.redis.ping()
                if result is True or result == "PONG" or result == b"PONG":
                    ping_success = True
                    break
            except Exception as e:
                logger.warning("⚠️ Ошибка подключения к Redis (попытка %s/%s): %s", attempt + 1, max_retries, e)
                try:
                    if self.redis is not None:
                        self.redis.close()
                except Exception:
                    pass
                self.redis = None
                continue

        if not ping_success or self.redis is None:
            logger.error("❌ Не удалось подключиться к Redis после %s попыток", max_retries)
            return

        logger.info("✅ Redis connections established successfully (main=%s)", self.redis_url)

        self.outbox_stream = os.getenv("SIGNAL_OUTBOX_STREAM", "stream:signals:outbox")
        self.dlq_stream = os.getenv("SIGNAL_DLQ_STREAM", "stream:signals:dlq")
        self.group = os.getenv("SIGNAL_OUTBOX_GROUP", "signals-outbox-group")
        self.consumer = os.getenv("SIGNAL_OUTBOX_CONSUMER", f"dispatcher-{os.getpid()}")
        self.read_count = int(os.getenv("SIGNAL_OUTBOX_READ_COUNT", "200"))
        self.read_block_ms = int(os.getenv("SIGNAL_OUTBOX_READ_BLOCK_MS", "1000"))
        self.max_attempts = int(os.getenv("SIGNAL_OUTBOX_MAX_ATTEMPTS", "7"))
        self.claim_min_idle_ms = int(os.getenv("SIGNAL_OUTBOX_CLAIM_MIN_IDLE_MS", "30000"))
        self.claim_count = int(os.getenv("SIGNAL_OUTBOX_CLAIM_COUNT", "200"))

        # Marker namespace (separate)
        self.marker_prefix = os.getenv("SIGNAL_DELIVERY_MARKER_PREFIX", "deliver:v2")
        self.marker_ttl_sec = int(os.getenv("SIGNAL_DELIVERY_TTL_SEC", "86400"))

        # pending recovery (ACK fail / consumer death)
        self.claim_min_idle_ms = int(os.getenv("SIGNAL_OUTBOX_CLAIM_MIN_IDLE_MS", "30000"))
        self.claim_count = int(os.getenv("SIGNAL_OUTBOX_CLAIM_COUNT", "200"))
        self._pending_start_id = "0-0"

        # attempts per outbox msg_id (no re-enqueue)
        self._attempt_prefix = os.getenv("SIGNAL_OUTBOX_ATTEMPT_PREFIX", "outbox:attempt:v1")
        self._attempt_ttl_sec = int(os.getenv("SIGNAL_OUTBOX_ATTEMPT_TTL_SEC", "86400"))

        # diag/janitor
        self._diag_every_sec = float(os.getenv("SIGNAL_OUTBOX_DIAG_EVERY_SEC", "10"))
        self._last_diag = 0.0
        self._janitor_enabled = os.getenv("SIGNAL_DISPATCHER_JANITOR", "0") == "1"
        self._janitor_every_sec = float(os.getenv("SIGNAL_DISPATCHER_JANITOR_EVERY_SEC", "60"))
        self._last_janitor = 0.0
        self._janitor_scan_count = int(os.getenv("SIGNAL_DISPATCHER_JANITOR_SCAN_COUNT", "200"))

        self.notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
        self.notify_signal_counter_key = os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", "notify:telegram:signal_counter")
        try:
            self.notify_signal_every_n = max(1, int(os.getenv("NOTIFY_SIGNAL_EVERY_N", "1")))
        except ValueError:
            self.notify_signal_every_n = 1

        self._sha_cache: Dict[Tuple[int, str], str] = {}

    def _attempt_key(self, msg_id: str) -> str:
        return f"{self._attempt_prefix}:{msg_id}"

    def _incr_attempt(self, msg_id: str) -> int:
        k = self._attempt_key(msg_id)
        try:
            n = int(self.redis.incr(k))
            if n == 1:
                self.redis.expire(k, self._attempt_ttl_sec)
            return n
        except Exception:
            return 1

    def _marker_key(self, target: str, sid: str) -> str:
        return f"{self.marker_prefix}:{target}:{sid}"

    def _ensure_sha(self, client: Any, name: str, script: str) -> str:
        key = (id(client), name)
        sha = self._sha_cache.get(key)
        if sha:
            return sha
        sha = client.script_load(script)
        self._sha_cache[key] = sha
        return sha

    def _eval(self, client: Any, name: str, script: str, numkeys: int, *args: Any) -> Any:
        sha = self._ensure_sha(client, name, script)
        try:
            return client.evalsha(sha, numkeys, *args)
        except Exception:
            return client.eval(script, numkeys, *args)

    def _diag(self, helper: SyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if now - self._last_diag < self._diag_every_sec:
            return
        self._last_diag = now
        try:
            info = helper.pending_details(self.outbox_stream)
            pending = int(info.get("pending", 0) or 0)
            cons = info.get("consumers") or []
            logger.info("outbox pending=%d consumers=%s", pending, cons)
        except Exception:
            pass

    def _janitor(self) -> None:
        if not self._janitor_enabled:
            return
        now = time.monotonic()
        if now - self._last_janitor < self._janitor_every_sec:
            return
        self._last_janitor = now
        # фиксируем "orphan без TTL": выставляем TTL или удаляем
        try:
            cursor = 0
            scanned = 0
            pattern = f"{self.marker_prefix}:*"
            while scanned < self._janitor_scan_count:
                cursor, keys = self.redis.scan(cursor=cursor, match=pattern, count=10000)
                for k in keys or []:
                    scanned += 1
                    try:
                        ttl = int(self.redis.ttl(k))
                        if ttl < 0:
                            # -1 no ttl, -2 missing
                            self.redis.expire(k, self.marker_ttl_sec)
                    except Exception:
                        continue
                    if scanned >= self._janitor_scan_count:
                        break
                if cursor == 0:
                    break
        except Exception:
            pass

    def _xadd_idempotent(self, client: Any, *, target: str, sid: str, stream: str, fields: Dict[str, Any], maxlen: int) -> bool:
        """
        B) фикс: marker ставится ПОСЛЕ XADD в одном Lua, с rollback на marker-fail.
        Возвращает True если доставили (или уже было доставлено).
        """
        fv: List[str] = []
        for k, v in (fields or {}).items():
            fv.append(str(k))
            fv.append(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        res = self._eval(
            client,
            "xadd_fields_then_mark",
            _LUA_XADD_FIELDS_THEN_MARK,
            2,
            self._marker_key(target, sid),
            stream,
            str(self.marker_ttl_sec),
            str(maxlen),
            *fv,
        )
        return bool(res and int(res[0]) in (0, 1))

    def _setex_idempotent(self, client: Any, *, target: str, sid: str, key: str, value_json: str, ttl_sec: int) -> bool:
        res = self._eval(
            client,
            "setex_then_mark",
            _LUA_SETEX_THEN_MARK,
            2,
            self._marker_key(target, sid),
            key,
            str(self.marker_ttl_sec),
            str(int(ttl_sec)),
            value_json,
        )
        return bool(res and int(res[0]) in (0, 1))

    def _notify_idempotent(self, client: Any, *, sid: str, payload: Dict[str, Any]) -> bool:
        fv: List[str] = []
        for k, v in (payload or {}).items():
            fv.append(str(k))
            fv.append(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        res = self._eval(
            client,
            "notify_gate",
            _LUA_NOTIFY_GATE,
            3,
            self._marker_key("notify", sid),
            self.notify_stream,
            self.notify_signal_counter_key,
            str(self.marker_ttl_sec),
            str(500),
            str(self.notify_signal_every_n),
            *fv,
        )
        return bool(res and int(res[0]) in (0, 1))

    def _parse_envelope(self, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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

    def _handle_one(self, msg_id: str, fields: Dict[str, Any]) -> bool:
        env = self._parse_envelope(fields)
        if not env:
            logger.warning("Bad envelope (no data) msg=%s fields=%s", msg_id, fields)
            self._send_dlq(msg_id, fields, reason="bad_envelope")
            self.redis.xack(self.outbox_stream, self.group, msg_id)
            return True
        attempt = int(env.get("attempt", 0))
        sid = str(env.get("sid") or "")
        if not sid:
            self._send_dlq(msg_id, fields, reason="missing_sid")
            self.redis.xack(self.outbox_stream, self.group, msg_id)
            return True
        try:
            self._deliver_all(env)
            return True
        except Exception as exc:
            attempt += 1
            env["attempt"] = attempt
            env["last_error"] = str(exc)
            if attempt >= self.max_attempts:
                logger.error("DLQ after max attempts sid=%s msg=%s err=%s", sid, msg_id, exc)
                self._send_dlq(msg_id, env, reason="max_attempts")
                self.redis.xack(self.outbox_stream, self.group, msg_id)
                return True
            # Re-enqueue with incremented attempt, ack old to avoid stuck pending
            self.redis.xadd(self.outbox_stream, {"data": json.dumps(env, ensure_ascii=False)}, maxlen=20000, approximate=True)
            self.redis.xack(self.outbox_stream, self.group, msg_id)
            logger.warning("Re-enqueued sid=%s attempt=%d err=%s", sid, attempt, exc)
            return True

    def _send_dlq(self, msg_id: str, data: Any, reason: str) -> None:
        payload = {
            "ts": get_ny_time_millis(),
            "reason": reason,
            "orig_id": msg_id,
            "data": data,
        }
        try:
            self.redis.xadd(self.dlq_stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=200000, approximate=True)
        except Exception as exc:
            logger.error("Failed to write DLQ: %s", exc, exc_info=True)

    # ----------------- loop -----------------
    def _process_pending(self, helper: SyncRedisStreamHelper) -> None:
        try:
            next_id, msgs = helper.claim_pending(
                self.outbox_stream,
                min_idle_ms=self.claim_min_idle_ms,
                start_id=self._pending_start_id,
                count=self.claim_count,
            )
            if (not msgs) and (next_id == "0-0"):
                # keep start_id to avoid scan reset storms
                pass
            else:
                self._pending_start_id = next_id
        except Exception as e:
            if is_transient_error(e):
                time.sleep(0.2)
                return
            raise

        for m in msgs or []:
            msg_id = getattr(m, "msg_id", "") or ""
            fields = getattr(m, "fields", {}) or {}
            if not msg_id:
                continue
            ack_now = False
            try:
                ack_now = self._handle_one(msg_id, fields)
            except Exception as exc:
                if is_transient_error(exc):
                    continue
                logger.error("pending handle error msg=%s err=%s", msg_id, exc, exc_info=True)
                continue
            if ack_now:
                try:
                    helper.ack(self.outbox_stream, msg_id)
                except Exception as ack_e:
                    # do not re-enqueue; pending-claim will retry ack later
                    logger.warning("ACK failed (pending) %s: %s", msg_id, ack_e)

    def run(self) -> None:
        if self.redis is None:
            logger.error("❌ Redis client is None, cannot start dispatcher")
            return

        helper = SyncRedisStreamHelper(self.redis, self.group, self.consumer)
        # C) outbox group must start from "0" always (no loss on recovery)
        helper.ensure_group(self.outbox_stream, start_id="0")
        logger.info("SignalDispatcher started. stream=%s group=%s consumer=%s", self.outbox_stream, self.group, self.consumer)
        while True:
            try:
                self._diag(helper)
                self._janitor()

                # Pending recovery (ACK-fail / consumer-death)
                self._process_pending(helper)

                messages = helper.read(
                    {self.outbox_stream: ">"},
                    count=self.read_count,
                    block=self.read_block_ms,
                    recover_start_id="0",  # C) recovery must also be "0"
                )
                if not messages:
                    continue
                for stream, items in messages:
                    for msg_id, fields in items:
                        try:
                            ack_now = self._handle_one(msg_id, fields)
                        except Exception as exc:
                            if is_transient_error(exc):
                                continue
                            logger.error("Failed to handle outbox msg %s: %s", msg_id, exc, exc_info=True)
                            ack_now = False
                        if ack_now:
                            try:
                                helper.ack(stream, msg_id)
                            except Exception as exc:
                                logger.warning("ACK failed %s: %s", msg_id, exc)
                        else:
                            # keep pending; will be recovered by XAUTOCLAIM
                            continue
            except KeyboardInterrupt:
                logger.info("SignalDispatcher stopped")
                return
            except Exception as exc:
                logger.error("Dispatcher loop error: %s", exc, exc_info=True)
                time.sleep(1)

    def _deliver_all(self, env: Dict[str, Any]) -> None:
        targets = env.get("targets") or {}
        meta = env.get("meta") or {}
        sid = str(env["sid"])
        dual_client = self.dual_redis or self.simple_redis or self.redis
        simple_client = self.simple_redis or self.redis

        # 1) notify:telegram (dual_redis) + every_n gating
        notify_payload = targets.get("notify")
        if notify_payload and dual_client:
            ok = self._notify_idempotent(dual_client, sid=sid, payload=notify_payload)
            if not ok:
                raise RuntimeError("notify_delivery_failed")
        elif notify_payload:
            logger.warning("notify payload skipped: no available Redis client")

        # 2) strategy stream (signals:{strategy}:{symbol})
        signal_stream = str(meta.get("signal_stream") or "")
        signal_payload = targets.get("signal_stream_payload")
        if signal_stream and signal_payload and simple_client:
            ok = self._xadd_idempotent(
                simple_client,
                target="signal_stream",
                sid=sid,
                stream=signal_stream,
                fields={"data": json.dumps(signal_payload, ensure_ascii=False)},
                maxlen=1000,
            )
            if not ok:
                raise RuntimeError("signal_stream_delivery_failed")
        elif signal_stream and signal_payload:
            logger.warning("signal stream payload skipped: no available Redis client")

        # 3) audit stream
        audit_stream = str(meta.get("audit_stream") or "")
        audit_payload = targets.get("audit_payload")
        if audit_stream and audit_payload and self.redis:
            ok = self._xadd_idempotent(
                self.redis,
                target="audit",
                sid=sid,
                stream=audit_stream,
                fields={"data": json.dumps(audit_payload, ensure_ascii=False)},
                maxlen=200000,
            )
            if not ok:
                raise RuntimeError("audit_delivery_failed")
        elif audit_stream and audit_payload:
            logger.warning("audit stream payload skipped: no available Redis client")

        # 4) manual stream
        manual_stream = str(meta.get("manual_stream") or "")
        manual_payload = targets.get("manual_payload")
        if manual_stream and manual_payload and dual_client:
            ok = self._xadd_idempotent(
                dual_client,
                target="manual",
                sid=sid,
                stream=manual_stream,
                fields={"data": json.dumps(manual_payload, ensure_ascii=False)},
                maxlen=2000,
            )
            if not ok:
                raise RuntimeError("manual_delivery_failed")
        elif manual_stream and manual_payload:
            logger.warning("manual stream payload skipped: no available Redis client")

        # 5) snapshot
        snap_key = str(meta.get("snap_key") or "")
        snap_ttl = int(meta.get("snap_ttl") or 21600)
        snap_payload = targets.get("snapshot")
        if snap_key and snap_payload and self.redis:
            ok = self._setex_idempotent(
                self.redis,
                target="snapshot",
                sid=sid,
                key=snap_key,
                value_json=json.dumps(snap_payload, ensure_ascii=False),
                ttl_sec=snap_ttl,
            )
            if not ok:
                raise RuntimeError("snapshot_delivery_failed")
        elif snap_key and snap_payload:
            logger.warning("snapshot payload skipped: no available Redis client")


if __name__ == "__main__":
    dispatcher = SignalDispatcher()
    dispatcher.run()