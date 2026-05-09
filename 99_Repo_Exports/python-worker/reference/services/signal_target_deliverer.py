import json
import os
import random
import time
from typing import Any

import redis
from redis.exceptions import ConnectionError, TimeoutError

from common.log import setup_logger
from core.dual_redis_client import get_dual_signals_redis
from core.redis_client import get_redis
from core.redis_stream_consumer import SyncRedisStreamHelper
from utils.time_utils import get_ny_time_millis

logger = setup_logger("SignalTargetDeliverer")

_LUA_XADD_WITH_INFLIGHT_DONE = r"""
-- KEYS[1] = inflight_key
-- KEYS[2] = done_key
-- KEYS[3] = stream_key
-- ARGV[1] = inflight_ttl_ms
-- ARGV[2] = done_ttl_sec
-- ARGV[3] = maxlen
-- ARGV[4] = payload_json
if redis.call('EXISTS', KEYS[2]) == 1 then return {0} end
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'PX', ARGV[1])
if not ok then return {-3} end
local id = redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], '*', 'data', ARGV[4])
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
redis.call('DEL', KEYS[1])
return {1, id}
"""

_LUA_SETEX_WITH_INFLIGHT_DONE = r"""
-- KEYS[1] = inflight_key
-- KEYS[2] = done_key
-- KEYS[3] = value_key
-- ARGV[1] = inflight_ttl_ms
-- ARGV[2] = done_ttl_sec
-- ARGV[3] = value_ttl_sec
-- ARGV[4] = payload_json
if redis.call('EXISTS', KEYS[2]) == 1 then return {0} end
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'PX', ARGV[1])
if not ok then return {-3} end
redis.call('SETEX', KEYS[3], ARGV[3], ARGV[4])
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
redis.call('DEL', KEYS[1])
return {1}
"""

_LUA_NOTIFY_EVERY_N = r"""
-- KEYS[1] = inflight_key
-- KEYS[2] = done_key
-- KEYS[3] = counter_key
-- KEYS[4] = notify_stream
-- ARGV[1] = inflight_ttl_ms
-- ARGV[2] = done_ttl_sec
-- ARGV[3] = every_n
-- ARGV[4] = maxlen
-- ARGV[5] = payload_json (field 'data')
if redis.call('EXISTS', KEYS[2]) == 1 then return {0} end
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'PX', ARGV[1])
if not ok then return {-3} end
local n = redis.call('INCR', KEYS[3])
local every_n = tonumber(ARGV[3]) or 1
if every_n > 1 and (n % every_n) ~= 0 then
  redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
  redis.call('DEL', KEYS[1])
  return {2, n}
end
local id = redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[4], '*', 'data', ARGV[5])
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
redis.call('DEL', KEYS[1])
return {1, id, n}
"""

_LUA_POP_DUE_RETRY = r"""
-- KEYS[1] = retry_zset
-- ARGV[1] = now_ms
-- ARGV[2] = task_key_prefix   (full prefix, e.g. "signal:task:notify:")
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
if #ids == 0 then return nil end
local sid = ids[1]
redis.call('ZREM', KEYS[1], sid)
local tkey = ARGV[2] .. sid
local data = redis.call('GET', tkey)
return {sid, data}
"""


class _Backoff:
    def __init__(self, base: float = 0.25, cap: float = 5.0):
        self.base = base
        self.cap = cap
        self.n = 0
    def reset(self) -> None:
        self.n = 0
    def next_sleep(self) -> float:
        self.n += 1
        t = min(self.cap, self.base * (2 ** min(self.n, 8)))
        return t + random.random() * 0.05


def _is_transient(e: Exception) -> bool:
    if isinstance(e, (ConnectionError, TimeoutError, OSError)):
        return True
    s = (str(e) or "").lower()
    return any(t in s for t in ("timeout", "timed out", "connection", "reset", "broken pipe", "busy loading", "loading the dataset"))


class SignalTargetDeliverer:
    """
    Deliverer для одного таргета.
    Читает stream:signals:tasks:{target} и выполняет доставку с exactly-once на таргете:
      - deliver:{target}:{sid} (done marker, TTL=24h)
      - deliver:inflight:{target}:{sid} (lock, TTL=30s)
      - claim_pending для recovery
    """

    def __init__(self, target: str):
        self.target = target
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.redis = redis.from_url(self.redis_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=15, health_check_interval=0)

        self.group = os.getenv("SIGNAL_TASKS_GROUP", f"signals-tasks-{target}")
        self.consumer = os.getenv("SIGNAL_TASKS_CONSUMER", f"{target}-{os.getpid()}")
        self.stream = os.getenv(f"SIGNAL_TASKS_STREAM_{target.upper()}", f"stream:signals:tasks:{target}")
        self.dlq_stream = os.getenv("SIGNAL_DLQ_STREAM", "stream:signals:dlq")

        self.read_count = int(os.getenv("SIGNAL_TASKS_READ_COUNT", "200"))
        self.read_block_ms = int(os.getenv("SIGNAL_TASKS_READ_BLOCK_MS", "1000"))
        self.max_attempts = int(os.getenv("SIGNAL_TASKS_MAX_ATTEMPTS", "7"))

        self.done_ttl_sec = int(os.getenv("SIGNAL_DELIVERY_TTL_SEC", "86400"))
        self.inflight_ttl_ms = int(os.getenv("SIGNAL_TARGET_INFLIGHT_TTL_MS", "30000"))

        # "ещё выше": retry-delay queue (ZSET) вместо ре-энкью в stream
        self.retry_zset = os.getenv(f"SIGNAL_RETRY_ZSET_{target.upper()}", f"zset:signals:retry:{target}")
        self.task_key_prefix = os.getenv(
            f"SIGNAL_TASK_KEY_PREFIX_{target.upper()}",
            f"signal:task:{target}:",
        )
        self.retry_base_ms = int(os.getenv("SIGNAL_RETRY_BASE_MS", "250"))
        self.retry_cap_ms = int(os.getenv("SIGNAL_RETRY_CAP_MS", "30000"))
        self.retry_jitter_ms = int(os.getenv("SIGNAL_RETRY_JITTER_MS", "75"))
        self.retry_batch = int(os.getenv("SIGNAL_RETRY_BATCH", "200"))

        self.claim_min_idle_ms = int(os.getenv("SIGNAL_TASKS_CLAIM_MIN_IDLE_MS", "60000"))
        self.claim_count = int(os.getenv("SIGNAL_TASKS_CLAIM_COUNT", "200"))

        # target-specific deps
        try:
            self.dual_redis = get_dual_signals_redis()
        except Exception:
            self.dual_redis = None
        try:
            self.simple_redis = get_redis()
        except Exception:
            self.simple_redis = None

        self.notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
        self.notify_counter_key = os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", "notify:telegram:signal_counter")
        self.notify_every_n = max(1, int(os.getenv("NOTIFY_SIGNAL_EVERY_N", "1")))

        self._sha_xadd: str | None = None
        self._sha_setex: str | None = None
        self._sha_notify: str | None = None
        self._sha_pop_retry: str | None = None

        self._backoff = _Backoff()
        self._start_id = "0-0"

    def _done_key(self, sid: str) -> str:
        return f"deliver:{self.target}:{sid}"

    def _inflight_key(self, sid: str) -> str:
        return f"deliver:inflight:{self.target}:{sid}"

    def _ensure_scripts(self) -> None:
        if not self._sha_xadd:
            self._sha_xadd = self.redis.script_load(_LUA_XADD_WITH_INFLIGHT_DONE)
        if not self._sha_setex:
            self._sha_setex = self.redis.script_load(_LUA_SETEX_WITH_INFLIGHT_DONE)
        if not self._sha_notify:
            self._sha_notify = self.redis.script_load(_LUA_NOTIFY_EVERY_N)
        if not self._sha_pop_retry:
            self._sha_pop_retry = self.redis.script_load(_LUA_POP_DUE_RETRY)

    def _evalsha_fallback(self, sha: str, script: str, numkeys: int, *args: Any) -> Any:
        try:
            return self.redis.evalsha(sha, numkeys, *args)
        except Exception:
            return self.redis.eval(script, numkeys, *args)

    def _send_dlq(self, reason: str, data: Any) -> None:
        payload = {"ts": get_ny_time_millis(), "reason": reason, "target": self.target, "data": data}
        try:
            self.redis.xadd(self.dlq_stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=200000, approximate=True)
        except Exception as e:
            logger.error("DLQ write failed: %s", e, exc_info=True)

    def _parse_task(self, fields: dict[str, Any]) -> dict[str, Any] | None:
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

    def _task_key(self, sid: str) -> str:
        return f"{self.task_key_prefix}{sid}"

    def _compute_retry_delay_ms(self, attempt: int) -> int:
        # exp backoff: base * 2^attempt, capped, + jitter
        a = max(0, int(attempt))
        raw = min(self.retry_cap_ms, int(self.retry_base_ms * (2 ** min(a, 10))))
        jitter = random.randint(0, max(0, self.retry_jitter_ms))
        return int(raw + jitter)

    def _schedule_retry(self, task: dict[str, Any], *, attempt: int, err: Exception) -> None:
        """
        Delay-queue retry:
          - store latest task JSON in key signal:task:{target}:{sid} (EX = done_ttl)
          - ZADD retry_zset score=now+delay member=sid
        """
        sid = (task.get("sid") or "")
        if not sid:
            raise ValueError("missing_sid_for_retry")
        task2 = dict(task)
        task2["attempt"] = int(attempt)
        task2["last_error"] = str(err)
        delay_ms = self._compute_retry_delay_ms(task2["attempt"])
        now_ms = get_ny_time_millis()
        due_ms = now_ms + delay_ms

        tkey = self._task_key(sid)
        pipe = self.redis.pipeline(transaction=True)
        pipe.setex(tkey, int(self.done_ttl_sec), json.dumps(task2, ensure_ascii=False, separators=(",", ":")))
        pipe.zadd(self.retry_zset, {sid: float(due_ms)})
        pipe.execute()

    def _pop_due_retry(self, now_ms: int) -> dict[str, Any] | None:
        """
        Atomic pop from ZSET (single item):
          ZRANGEBYSCORE ... LIMIT 0 1 + ZREM inside Lua, then GET task key.
        Returns parsed task dict or None.
        """
        self._ensure_scripts()
        if not self._sha_pop_retry:
            return None
        res = self._evalsha_fallback(
            self._sha_pop_retry,
            _LUA_POP_DUE_RETRY,
            1,
            self.retry_zset,
            str(int(now_ms)),
            self.task_key_prefix,
        )
        if not res or not isinstance(res, (list, tuple)) or len(res) < 2:
            return None
        _sid, raw = res[0], res[1]
        if not raw:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, dict):
            return raw
        return None

    def _drain_due_retries(self) -> int:
        """
        Process up to SIGNAL_RETRY_BATCH due retries per tick.
        Returns number processed.
        """
        processed = 0
        now_ms = get_ny_time_millis()
        for _ in range(max(1, int(self.retry_batch))):
            task = self._pop_due_retry(now_ms)
            if not task:
                break
            try:
                self._deliver_or_schedule(task)
            except Exception as e:
                # last resort: if even scheduling fails, backoff and stop draining
                delay = self._backoff.next_sleep()
                logger.warning("drain retry failed target=%s err=%s (backoff=%.2fs)", self.target, e, delay)
                time.sleep(delay)
                break
            processed += 1
        return processed

    def _deliver_or_schedule(self, task: dict[str, Any]) -> None:
        """
        One delivery attempt for a task dict (from stream or retry-queue).
        On transient error -> schedule via ZSET (no stream spam).
        On non-transient -> DLQ + best-effort mark done.
        """
        attempt = int(task.get("attempt", 0))
        if attempt >= self.max_attempts:
            self._send_dlq("max_attempts", task)
            sid = (task.get("sid") or "")
            if sid:
                try:
                    self.redis.set(self._done_key(sid), "1", ex=self.done_ttl_sec, nx=True)
                except Exception:
                    pass
            return

        try:
            _ = self._deliver(task)
            self._backoff.reset()
            return
        except Exception as e:
            if _is_transient(e):
                # schedule retry; increment attempt
                self._schedule_retry(task, attempt=attempt + 1, err=e)
                return
            # non-transient
            self._send_dlq("non_transient", {"task": task, "err": str(e)})
            sid = (task.get("sid") or "")
            if sid:
                try:
                    self.redis.set(self._done_key(sid), "1", ex=self.done_ttl_sec, nx=True)
                except Exception:
                    pass
            return

    def _deliver(self, task: dict[str, Any]) -> int:
        """
        Returns:
          1 -> delivered
          0 -> already delivered (dedup)
          2 -> intentionally skipped (notify gating)
        Raises on transient/non-transient errors.
        """
        self._ensure_scripts()

        sid = (task.get("sid") or "")
        if not sid:
            raise ValueError("missing_sid")
        payload = task.get("payload") or {}

        done = self._done_key(sid)
        inflight = self._inflight_key(sid)

        if self.target == "notify":
            notify = payload.get("notify")
            if not notify:
                raise ValueError("missing_notify_payload")
            client = self.dual_redis or self.simple_redis or self.redis
            res = client.evalsha(
                self._sha_notify,
                4,
                inflight, done,
                self.notify_counter_key, self.notify_stream,
                str(int(self.inflight_ttl_ms)),
                str(int(self.done_ttl_sec)),
                str(int(self.notify_every_n)),
                str(500),
                json.dumps(notify, ensure_ascii=False, separators=(",", ":")),
            )
            code = int(res[0]) if res else 0
            if code == -3:
                raise TimeoutError("target_busy")
            if code == 2:
                return 2
            return 1 if code == 1 else 0

        if self.target in ("signal_stream", "audit", "manual"):
            stream = (payload.get("stream") or "")
            data = payload.get("data")
            if not stream or data is None:
                raise ValueError("missing_stream_or_data")

            # which redis?
            if self.target == "signal_stream":
                client = self.simple_redis or self.redis
                maxlen = int(os.getenv("SIGNAL_STREAM_MAXLEN", "1000"))
            elif self.target == "manual":
                client = self.dual_redis or self.simple_redis or self.redis
                maxlen = int(os.getenv("MANUAL_STREAM_MAXLEN", "2000"))
            else:
                client = self.redis
                maxlen = int(os.getenv("AUDIT_STREAM_MAXLEN", "200000"))

            res = client.evalsha(
                self._sha_xadd,
                3,
                inflight, done, stream,
                str(int(self.inflight_ttl_ms)),
                str(int(self.done_ttl_sec)),
                str(int(maxlen)),
                json.dumps(data, ensure_ascii=False, separators=(",", ":")),
            )
            code = int(res[0]) if res else 0
            if code == -3:
                raise TimeoutError("target_busy")
            return 1 if code == 1 else 0

        if self.target == "snapshot":
            key = (payload.get("key") or "")
            ttl = int(payload.get("ttl") or 21600)
            data = payload.get("data")
            if not key or data is None:
                raise ValueError("missing_snapshot_fields")
            res = self.redis.evalsha(
                self._sha_setex,
                3,
                inflight, done, key,
                str(int(self.inflight_ttl_ms)),
                str(int(self.done_ttl_sec)),
                str(int(ttl)),
                json.dumps(data, ensure_ascii=False, separators=(",", ":")),
            )
            code = int(res[0]) if res else 0
            if code == -3:
                raise TimeoutError("target_busy")
            return 1 if code == 1 else 0

        raise ValueError(f"unknown_target:{self.target}")

    def run(self) -> None:
        helper = SyncRedisStreamHelper(self.redis, self.group, self.consumer)
        # tasks must start from "0" (durable; no-loss on recreation)
        helper.ensure_group(self.stream, start_id="0")
        logger.info("SignalTargetDeliverer started target=%s stream=%s group=%s consumer=%s", self.target, self.stream, self.group, self.consumer)

        while True:
            try:
                # -1) "ещё выше": drain delay-queue first (smooth retries, no stream spam)
                try:
                    drained = self._drain_due_retries()
                    if drained:
                        logger.debug("drained retries target=%s n=%d", self.target, drained)
                except Exception as e:
                    delay = self._backoff.next_sleep()
                    logger.warning("retry-drain error target=%s err=%s (backoff=%.2fs)", self.target, e, delay)
                    time.sleep(delay)

                # 0) pending recovery
                try:
                    next_id, pending = helper.claim_pending(
                        self.stream, min_idle_ms=self.claim_min_idle_ms,
                        start_id=self._start_id, count=self.claim_count,
                    )
                    if (not pending) and (next_id == "0-0"):
                        pass
                    else:
                        self._start_id = next_id
                    if pending:
                        for m in pending:
                            if self._handle_one(m.msg_id, m.fields, helper):
                                helper.ack(self.stream, m.msg_id)
                except Exception as e:
                    delay = self._backoff.next_sleep()
                    logger.warning("pending-claim error target=%s err=%s (backoff=%.2fs)", self.target, e, delay)
                    time.sleep(delay)

                msgs = helper.read({self.stream: ">"}, count=self.read_count, block=self.read_block_ms, create_start_id="0")
                if not msgs:
                    self._backoff.reset()
                    continue
                for _, items in msgs:
                    for msg_id, fields in items:
                        ok = self._handle_one(msg_id, fields, helper)
                        if ok:
                            try:
                                helper.ack(self.stream, msg_id)
                            except Exception as e:
                                logger.warning("ACK failed msg=%s err=%s", msg_id, e)
                                # leave pending; claim_pending will recover
            except KeyboardInterrupt:
                logger.info("SignalTargetDeliverer stopped target=%s", self.target)
                return
            except Exception as e:
                delay = self._backoff.next_sleep()
                logger.error("loop error target=%s err=%s (backoff=%.2fs)", self.target, e, delay, exc_info=True)
                time.sleep(delay)

    def _handle_one(self, msg_id: str, fields: dict[str, Any], helper: SyncRedisStreamHelper) -> bool:
        task = self._parse_task(fields)
        if not task:
            self._send_dlq("bad_task", {"msg_id": msg_id, "fields": fields})
            return True

        try:
            # single place for retry policy: deliver_or_schedule()
            self._deliver_or_schedule(task)
            return True
        except Exception as e:
            # if even scheduling failed, leave pending (claim_pending will recover)
            delay = self._backoff.next_sleep()
            logger.warning("handle_one failed target=%s err=%s (backoff=%.2fs)", self.target, e, delay)
            time.sleep(delay)
            return False


if __name__ == "__main__":
    target = os.getenv("SIGNAL_TARGET", "").strip()
    if not target:
        raise SystemExit("SIGNAL_TARGET env is required (notify|signal_stream|audit|manual|snapshot)")
    SignalTargetDeliverer(target).run()
