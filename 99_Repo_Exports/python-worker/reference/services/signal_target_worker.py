from utils.time_utils import get_ny_time_millis
import json
import os
import time
import random
from typing import Any, Dict, Optional


from common.log import setup_logger

logger = setup_logger("SignalTargetWorker")

_LUA_CLAIM_DUE = r"""
-- Durable claim with visibility timeout (no task loss on worker crash).
-- KEYS[1] = due_zset
-- KEYS[2] = inflight_zset
-- ARGV[1] = now_ms
-- ARGV[2] = visibility_ms
-- ARGV[3] = task_key_prefix (signal:task:{target}:)
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
if #ids == 0 then return nil end
local sid = ids[1]
redis.call('ZREM', KEYS[1], sid)
local deadline = tonumber(ARGV[1]) + tonumber(ARGV[2])
redis.call('ZADD', KEYS[2], deadline, sid)
local tkey = ARGV[3] .. sid
local data = redis.call('GET', tkey)
return {sid, data, deadline}
"""

_LUA_REQUEUE_EXPIRED = r"""
-- Requeue expired inflight tasks back to due (or drop if already done).
-- KEYS[1] = inflight_zset
-- KEYS[2] = due_zset
-- ARGV[1] = now_ms
-- ARGV[2] = limit
-- ARGV[3] = retry_delay_ms
-- ARGV[4] = done_key_prefix (deliver:done:{target}:)
-- ARGV[5] = task_key_prefix (signal:task:{target}:)
local sids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #sids == 0 then return {0} end
local moved = 0
local now = tonumber(ARGV[1])
local delay = tonumber(ARGV[3])
for i=1,#sids do
  local sid = sids[i]
  redis.call('ZREM', KEYS[1], sid)
  local done_key = ARGV[4] .. sid
  if redis.call('EXISTS', done_key) == 1 then
    local tkey = ARGV[5] .. sid
    redis.call('DEL', tkey)
  else
    redis.call('ZADD', KEYS[2], now + delay, sid)
    moved = moved + 1
  end
end
return {moved}
"""

_LUA_SCHEDULE_RETRY = r"""
-- Atomic retry scheduling: update task + move from inflight -> due.
-- KEYS[1] = task_key
-- KEYS[2] = due_zset
-- KEYS[3] = inflight_zset
-- ARGV[1] = ttl_sec
-- ARGV[2] = due_ms
-- ARGV[3] = sid
-- ARGV[4] = task_json
redis.call('SETEX', KEYS[1], ARGV[1], ARGV[4])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
redis.call('ZREM', KEYS[3], ARGV[3])
return {1}
"""

_LUA_COMPLETE = r"""
-- Best-effort cleanup after success/skip: remove inflight + delete task payload.
-- KEYS[1] = inflight_zset
-- KEYS[2] = task_key
-- ARGV[1] = sid
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return {1}
"""

_LUA_XADD_DELIVER = r"""
-- Atomic per-target exactly-once:
-- 1) if done -> skip
-- 2) set inflight NX PX
-- 3) XADD
-- 4) set done NX EX
-- 5) DEL inflight
-- KEYS[1] = done_key
-- KEYS[2] = inflight_key
-- KEYS[3] = stream
-- ARGV[1] = inflight_ttl_ms
-- ARGV[2] = done_ttl_sec
-- ARGV[3] = maxlen
-- ARGV[4] = approx ("1"/"0")
-- ARGV[5] = fields_json (dict)

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {2}
end
local ok = redis.call('SET', KEYS[2], '1', 'NX', 'PX', ARGV[1])
if not ok then
  return {0}
end
local fields = cjson.decode(ARGV[5])
local id = nil
if ARGV[4] == '1' then
  id = redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], '*', unpack(redis.call('HGETALL', '___dummy')))
end
-- Redis Lua doesn't have dict->argv for XADD; we pass flattened in argv below in python version.
return {9}
"""

_LUA_XADD_FLAT_DELIVER = r"""
-- KEYS[1] = done_key
-- KEYS[2] = inflight_key
-- KEYS[3] = stream
-- ARGV[1] = inflight_ttl_ms
-- ARGV[2] = done_ttl_sec
-- ARGV[3] = maxlen
-- ARGV[4] = approx ("1"/"0")
-- ARGV[5..] = flattened fields (k1,v1,k2,v2,...)

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {2}
end
local ok = redis.call('SET', KEYS[2], '1', 'NX', 'PX', ARGV[1])
if not ok then
  return {0}
end

local id = nil
if ARGV[4] == '1' then
  id = redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], '*', unpack(ARGV, 5))
else
  id = redis.call('XADD', KEYS[3], '*', unpack(ARGV, 5))
end

redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[2])
redis.call('DEL', KEYS[2])
return {1, id}
"""

_LUA_SETEX_DELIVER = r"""
-- KEYS[1] = done_key
-- KEYS[2] = inflight_key
-- KEYS[3] = value_key
-- ARGV[1] = inflight_ttl_ms
-- ARGV[2] = done_ttl_sec
-- ARGV[3] = setex_ttl_sec
-- ARGV[4] = value

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {2}
end
local ok = redis.call('SET', KEYS[2], '1', 'NX', 'PX', ARGV[1])
if not ok then
  return {0}
end

redis.call('SETEX', KEYS[3], ARGV[3], ARGV[4])
redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[2])
redis.call('DEL', KEYS[2])
return {1}
"""

_LUA_NOTIFY_DELIVER = r"""
-- KEYS[1] = done_key
-- KEYS[2] = inflight_key
-- KEYS[3] = notify_stream
-- KEYS[4] = counter_key
-- ARGV[1] = inflight_ttl_ms
-- ARGV[2] = done_ttl_sec
-- ARGV[3] = maxlen
-- ARGV[4] = approx ("1"/"0")
-- ARGV[5] = every_n
-- ARGV[6..] = flattened notify fields

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {2}
end
local ok = redis.call('SET', KEYS[2], '1', 'NX', 'PX', ARGV[1])
if not ok then
  return {0}
end

local send = 1
local cnt = redis.call('INCR', KEYS[4])
local every_n = tonumber(ARGV[5])
if every_n and every_n > 1 then
  if (cnt % every_n) ~= 0 then
    send = 0
  end
end

if send == 1 then
  if ARGV[4] == '1' then
    redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], '*', unpack(ARGV, 6))
  else
    redis.call('XADD', KEYS[3], '*', unpack(ARGV, 6))
  end
end

redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[2])
redis.call('DEL', KEYS[2])
return {1, send, cnt}
"""


def _is_transient(e: Exception) -> bool:
    s = (str(e) or "").lower()
    return any(t in s for t in ("timeout", "timed out", "connection", "reset", "broken pipe", "busy loading", "loading the dataset"))


class _Backoff:
    def __init__(self, base: float = 0.25, cap: float = 5.0):
        self.base = base
        self.cap = cap
        self.n = 0

    def reset(self) -> None:
        self.n = 0

    def next_sleep(self) -> float:
        self.n = min(self.n + 1, 10)
        return min(self.cap, self.base * (2 ** (self.n - 1)))


from core.redis_client import get_redis

class SignalTargetWorker:
    """
    Читает due_zset (zset:signals:due:{target}), берет task JSON из signal:task:{target}:{sid}
    и доставляет в конечный target атомарными Lua-скриптами.
    """

    def __init__(self, target: str):
        self.target = target
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.redis = get_redis()

        self.task_ttl_sec = int(os.getenv("SIGNAL_DELIVERY_TTL_SEC", "86400"))
        # lock ttl for parallel-safety during deliver lua
        self.inflight_ttl_ms = int(os.getenv("SIGNAL_TARGET_INFLIGHT_TTL_MS", "30000"))
        # visibility for claimed task (durable claim). must be >= worst-case delivery time.
        self.visibility_ms = int(os.getenv("SIGNAL_TARGET_VISIBILITY_MS", "60000"))
        self.requeue_scan_limit = int(os.getenv("SIGNAL_REQUEUE_SCAN_LIMIT", "200"))
        self.requeue_min_delay_ms = int(os.getenv("SIGNAL_REQUEUE_MIN_DELAY_MS", "250"))
        self.requeue_every_ms = int(os.getenv("SIGNAL_REQUEUE_EVERY_MS", "1000"))
        self._last_requeue_ms = 0

        # routing layout must match SignalDispatcher
        self.task_key_prefix = os.getenv("SIGNAL_TASK_KEY_PREFIX", "signal:task") + f":{target}:"
        self.due_zset = os.getenv("SIGNAL_DUE_ZSET_PREFIX", "zset:signals:due") + f":{target}"
        self.inflight_zset = os.getenv("SIGNAL_INFLIGHT_ZSET_PREFIX", "zset:signals:inflight") + f":{target}"
        self.done_prefix = f"deliver:done:{target}:"

        # retry settings
        self.max_attempts = int(os.getenv("SIGNAL_TASKS_MAX_ATTEMPTS", "7"))
        self.retry_base_ms = int(os.getenv("SIGNAL_RETRY_BASE_MS", "250"))
        self.retry_cap_ms = int(os.getenv("SIGNAL_RETRY_CAP_MS", "30000"))
        self.retry_jitter_ms = int(os.getenv("SIGNAL_RETRY_JITTER_MS", "75"))
        self.batch = int(os.getenv("SIGNAL_DUE_BATCH", "200"))

        # notify
        self.notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
        self.notify_counter_key = os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", "notify:telegram:signal_counter")
        try:
            self.notify_every_n = max(1, int(os.getenv("NOTIFY_SIGNAL_EVERY_N", "1")))
        except Exception:
            self.notify_every_n = 1

        # dlq
        self.dlq_stream = os.getenv("SIGNAL_DLQ_STREAM", "stream:signals:dlq")

        self._sha_pop: Optional[str] = None
        self._sha_xadd: Optional[str] = None
        self._sha_setex: Optional[str] = None
        self._sha_notify: Optional[str] = None
        self._sha_requeue: Optional[str] = None
        self._sha_retry: Optional[str] = None
        self._sha_complete: Optional[str] = None

        self._backoff = _Backoff()

    def _ensure_scripts(self) -> None:
        if not self._sha_pop:
            self._sha_pop = self.redis.script_load(_LUA_CLAIM_DUE)
        if not self._sha_xadd:
            self._sha_xadd = self.redis.script_load(_LUA_XADD_FLAT_DELIVER)
        if not self._sha_setex:
            self._sha_setex = self.redis.script_load(_LUA_SETEX_DELIVER)
        if not self._sha_notify:
            self._sha_notify = self.redis.script_load(_LUA_NOTIFY_DELIVER)
        if not self._sha_requeue:
            self._sha_requeue = self.redis.script_load(_LUA_REQUEUE_EXPIRED)
        if not self._sha_retry:
            self._sha_retry = self.redis.script_load(_LUA_SCHEDULE_RETRY)
        if not self._sha_complete:
            self._sha_complete = self.redis.script_load(_LUA_COMPLETE)

    def _done_key(self, sid: str) -> str:
        return f"deliver:done:{self.target}:{sid}"

    def _inflight_key(self, sid: str) -> str:
        return f"deliver:inflight:{self.target}:{sid}"

    def _schedule_retry(self, sid: str, task: Dict[str, Any], attempt: int, err: Exception) -> None:
        # Atomic retry: update task payload + move inflight->due (no task loss).
        a = max(0, int(attempt))
        delay = min(self.retry_cap_ms, int(self.retry_base_ms * (2 ** min(a, 10)))) + random.randint(0, max(0, self.retry_jitter_ms))
        due_ms = get_ny_time_millis() + int(delay)
        task2 = dict(task)
        task2["attempt"] = a
        task2["last_error"] = str(err)
        tkey = self.task_key_prefix + sid
        payload = json.dumps(task2, ensure_ascii=False, separators=(",", ":"))
        self._ensure_scripts()
        try:
            self.redis.evalsha(
                self._sha_retry,
                3,
                tkey,
                self.due_zset,
                self.inflight_zset,
                str(int(self.task_ttl_sec)),
                str(int(due_ms)),
                sid,
                payload,
            )  # type: ignore
        except Exception:
            self.redis.eval(
                _LUA_SCHEDULE_RETRY,
                3,
                tkey,
                self.due_zset,
                self.inflight_zset,
                str(int(self.task_ttl_sec)),
                str(int(due_ms)),
                sid,
                payload,
            )

    def _send_dlq(self, reason: str, data: Any) -> None:
        payload = {"ts": get_ny_time_millis(), "reason": reason, "target": self.target, "data": data}
        try:
            self.redis.xadd(self.dlq_stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=200000, approximate=True)
        except Exception as e:
            logger.error("DLQ write failed: %s", e, exc_info=True)

    def _claim_due(self, now_ms: int) -> Optional[Dict[str, Any]]:
        self._ensure_scripts()
        try:
            res = self.redis.evalsha(
                self._sha_pop,
                2,
                self.due_zset,
                self.inflight_zset,
                str(int(now_ms)),
                str(int(self.visibility_ms)),
                self.task_key_prefix,
            )  # type: ignore
        except Exception:
            res = self.redis.eval(
                _LUA_CLAIM_DUE,
                2,
                self.due_zset,
                self.inflight_zset,
                str(int(now_ms)),
                str(int(self.visibility_ms)),
                self.task_key_prefix,
            )
        if not res or not isinstance(res, (list, tuple)) or len(res) < 2:
            return None
        sid = str(res[0] or "")
        raw = res[1]
        if not sid or not raw:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            task = json.loads(raw)
        elif isinstance(raw, dict):
            task = raw
        else:
            return None
        task["sid"] = sid
        return task

    def _requeue_expired_inflight(self, now_ms: int) -> int:
        self._ensure_scripts()
        try:
            res = self.redis.evalsha(
                self._sha_requeue,
                2,
                self.inflight_zset,
                self.due_zset,
                str(int(now_ms)),
                str(int(self.requeue_scan_limit)),
                str(int(self.requeue_min_delay_ms)),
                self.done_prefix,
                self.task_key_prefix,
            )  # type: ignore
        except Exception:
            res = self.redis.eval(
                _LUA_REQUEUE_EXPIRED,
                2,
                self.inflight_zset,
                self.due_zset,
                str(int(now_ms)),
                str(int(self.requeue_scan_limit)),
                str(int(self.requeue_min_delay_ms)),
                self.done_prefix,
                self.task_key_prefix,
            )
        try:
            return int(res[0]) if res else 0
        except Exception:
            return 0

    def _complete_cleanup(self, sid: str) -> None:
        self._ensure_scripts()
        tkey = self.task_key_prefix + sid
        try:
            self.redis.evalsha(self._sha_complete, 2, self.inflight_zset, tkey, sid)  # type: ignore
        except Exception:
            try:
                self.redis.eval(_LUA_COMPLETE, 2, self.inflight_zset, tkey, sid)
            except Exception:
                pass

    def _deliver_xadd(self, sid: str, stream: str, fields: Dict[str, Any], maxlen: int, approx: bool) -> bool:
        self._ensure_scripts()
        done = self._done_key(sid)
        inflight = self._inflight_key(sid)
        flat: list[str] = []
        for k, v in (fields or {}).items():
            flat.append(str(k))
            if isinstance(v, str):
                flat.append(v)
            else:
                flat.append(json.dumps(v, ensure_ascii=False))

        args = [
            str(int(self.inflight_ttl_ms)),
            str(int(self.task_ttl_sec)),
            str(int(maxlen)),
            "1" if approx else "0",
            *flat,
        ]
        try:
            res = self.redis.evalsha(self._sha_xadd, 3, done, inflight, stream, *args)  # type: ignore
        except Exception:
            res = self.redis.eval(_LUA_XADD_FLAT_DELIVER, 3, done, inflight, stream, *args)
        if not res:
            return False
        code = int(res[0])
        # 1 = delivered, 2 = already done, 0 = inflight busy
        if code in (1, 2):
            self._complete_cleanup(sid)
            return True
        # inflight busy: keep task in inflight_zset until visibility expires
        return False

    def _deliver_setex(self, sid: str, key: str, ttl: int, value: str) -> bool:
        self._ensure_scripts()
        done = self._done_key(sid)
        inflight = self._inflight_key(sid)
        try:
            res = self.redis.evalsha(
                self._sha_setex,
                3,
                done, inflight, key,
                str(int(self.inflight_ttl_ms)),
                str(int(self.task_ttl_sec)),
                str(int(ttl)),
                value,
            )  # type: ignore
        except Exception:
            res = self.redis.eval(
                _LUA_SETEX_DELIVER,
                3,
                done, inflight, key,
                str(int(self.inflight_ttl_ms)),
                str(int(self.task_ttl_sec)),
                str(int(ttl)),
                value,
            )
        if not res:
            return False
        code = int(res[0])
        if code in (1, 2):
            self._complete_cleanup(sid)
            return True
        return False

    def _deliver_notify(self, sid: str, payload: Dict[str, Any]) -> bool:
        self._ensure_scripts()
        done = self._done_key(sid)
        inflight = self._inflight_key(sid)
        flat: list[str] = []
        for k, v in (payload or {}).items():
            flat.append(str(k))
            flat.append(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        try:
            res = self.redis.evalsha(
                self._sha_notify,
                4,
                done, inflight, self.notify_stream, self.notify_counter_key,
                str(int(self.inflight_ttl_ms)),
                str(int(self.task_ttl_sec)),
                "500",
                "1",
                str(int(self.notify_every_n)),
                *flat,
            )  # type: ignore
        except Exception:
            res = self.redis.eval(
                _LUA_NOTIFY_DELIVER,
                4,
                done, inflight, self.notify_stream, self.notify_counter_key,
                str(int(self.inflight_ttl_ms)),
                str(int(self.task_ttl_sec)),
                "500",
                "1",
                str(int(self.notify_every_n)),
                *flat,
            )
        if not res:
            return False
        code = int(res[0])
        if code in (1, 2):
            self._complete_cleanup(sid)
            return True
        return False

    def _process_one(self, task: Dict[str, Any]) -> None:
        sid = str(task.get("sid") or "")
        op = str(task.get("op") or "")
        attempt = int(task.get("attempt", 0) or 0)
        if not sid:
            self._send_dlq("missing_sid", task)
            return
        if attempt >= self.max_attempts:
            self._send_dlq("max_attempts", task)
            # mark done to avoid endless
            try:
                self.redis.set(self._done_key(sid), "1", ex=self.task_ttl_sec, nx=True)
            except Exception:
                pass
            self._complete_cleanup(sid)
            return

        try:
            if op == "notify_xadd":
                ok = self._deliver_notify(sid, task.get("payload") or {})
            elif op == "xadd":
                ok = self._deliver_xadd(
                    sid,
                    str(task.get("stream") or ""),
                    task.get("fields") or {},
                    int(task.get("maxlen") or 1000),
                    bool(task.get("approx", True)),
                )
            elif op == "setex":
                ok = self._deliver_setex(
                    sid,
                    str(task.get("key") or ""),
                    int(task.get("ttl") or 3600),
                    str(task.get("value") or ""),
                )
            else:
                self._send_dlq("unknown_op", task)
                ok = True

            if ok:
                self._backoff.reset()
                return
            # If deliver returned False, it is either lock-busy OR transient failure already handled by exception.
            # For lock-busy we DO NOT increment attempt and DO NOT move back to due:
            # keep in inflight_zset; it will be retried when visibility expires/requeued.
            return
        except Exception as e:
            if _is_transient(e):
                self._schedule_retry(sid, task, attempt + 1, e)
                return
            self._send_dlq("non_transient", {"task": task, "err": str(e)})
            try:
                self.redis.set(self._done_key(sid), "1", ex=self.task_ttl_sec, nx=True)
            except Exception:
                pass
            self._complete_cleanup(sid)

    def run(self) -> None:
        logger.info(
            "TargetWorker started target=%s redis=%s due_zset=%s inflight_zset=%s visibility_ms=%s",
            self.target, self.redis_url, self.due_zset, self.inflight_zset, self.visibility_ms
        )
        while True:
            try:
                now_ms = get_ny_time_millis()
                # periodic requeue of expired inflight claims (durable recovery)
                if now_ms - self._last_requeue_ms >= self.requeue_every_ms:
                    self._last_requeue_ms = now_ms
                    moved = self._requeue_expired_inflight(now_ms)
                    if moved:
                        logger.warning("Requeued expired inflight target=%s moved=%d", self.target, moved)

                processed = 0
                for _ in range(max(1, self.batch)):
                    task = self._claim_due(now_ms)
                    if not task:
                        break
                    self._process_one(task)
                    processed += 1
                if processed == 0:
                    time.sleep(0.05)
            except KeyboardInterrupt:
                logger.info("TargetWorker stopped target=%s", self.target)
                return
            except Exception as e:
                d = self._backoff.next_sleep()
                if _is_transient(e):
                    logger.warning("TargetWorker loop transient error target=%s err=%s (backoff=%.2fs)", self.target, e, d)
                else:
                    logger.error("TargetWorker loop error target=%s err=%s (backoff=%.2fs)", self.target, e, d, exc_info=True)
                time.sleep(d)


if __name__ == "__main__":
    target = os.getenv("SIGNAL_TARGET", "").strip()
    if not target:
        raise SystemExit("Set SIGNAL_TARGET=notify|signal_stream|audit|manual|snapshot")
    SignalTargetWorker(target).run()
