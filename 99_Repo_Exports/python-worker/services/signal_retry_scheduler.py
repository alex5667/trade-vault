from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""
Optional dedicated retry pump (if you prefer separating concerns).

If you run N dispatchers and want less variability in retry timing,
you can run exactly 1 scheduler instance (or many with leases - safe).
"""

import os
import random
import time

import redis

from common.log import setup_logger
from common.transient_errors import is_transient_error

logger = setup_logger("SignalRetryScheduler")

_LUA_PUMP_RETRY_DUE_TO_OUTBOX = r"""
-- KEYS[1] = retry_schedule_zset
-- KEYS[2] = outbox_stream
-- ARGV[1] = now_ms
-- ARGV[2] = limit
-- ARGV[3] = lease_prefix
-- ARGV[4] = lease_ttl_ms
-- ARGV[5] = outbox_maxlen
-- Returns: {moved, missing_payload, leased_skip}

local now_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local lease_prefix = ARGV[3]
local lease_ttl_ms = tonumber(ARGV[4])
local outbox_maxlen = ARGV[5]

local moved = 0
local missing_payload = 0
local leased_skip = 0

local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_ms, 'LIMIT', 0, limit)
for i, payload_key in ipairs(due) do
  local lease_key = lease_prefix .. ':' .. payload_key
  local ok = redis.call('SET', lease_key, '1', 'NX', 'PX', lease_ttl_ms)
  if not ok then
    leased_skip = leased_skip + 1
  else
    local payload_json = redis.call('GET', payload_key)
    if not payload_json then
      -- orphan schedule entry -> cleanup
      redis.call('ZREM', KEYS[1], payload_key)
      redis.call('DEL', lease_key)
      missing_payload = missing_payload + 1
    else
      -- enqueue back to outbox
      redis.call('XADD', KEYS[2], 'MAXLEN', '~', outbox_maxlen, '*', 'data', payload_json)
      -- cleanup
      redis.call('DEL', payload_key)
      redis.call('ZREM', KEYS[1], payload_key)
      redis.call('DEL', lease_key)
      moved = moved + 1
    end
  end
end
return {moved, missing_payload, leased_skip}
"""


class SignalRetryScheduler:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.redis = redis.from_url(self.redis_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=15)

        self.retry_schedule_zset = os.getenv("SIGNAL_OUTBOX_RETRY_ZSET", RS.SIGNAL_OUTBOX_RETRY_SCHEDULE)
        self.outbox_stream = os.getenv("SIGNAL_OUTBOX_STREAM", RS.SIGNAL_OUTBOX)
        self.outbox_maxlen = int(os.getenv("SIGNAL_OUTBOX_MAXLEN", "20000"))
        self.retry_lease_prefix = os.getenv("SIGNAL_OUTBOX_RETRY_LEASE_PREFIX", "signals:outbox:retry:lease")
        self.lease_ttl_ms = int(os.getenv("SIGNAL_SID_LEASE_TTL_MS", "30000"))
        self.batch = int(os.getenv("SIGNAL_OUTBOX_RETRY_PUMP_BATCH", "500"))
        self.interval_ms = int(os.getenv("SIGNAL_OUTBOX_RETRY_PUMP_EVERY_MS", "200"))

        self._sha: str | None = None

    def _ensure(self) -> str:
        if self._sha:
            return self._sha
        self._sha = self.redis.script_load(_LUA_PUMP_RETRY_DUE_TO_OUTBOX)
        return self._sha  # type: ignore

    def pump_once(self) -> None:
        now_ms = get_ny_time_millis()
        sha = self._ensure()
        try:
            res = self.redis.evalsha(
                sha, 2,
                self.retry_schedule_zset, self.outbox_stream,
                str(now_ms), str(self.batch), self.retry_lease_prefix, str(self.lease_ttl_ms), str(self.outbox_maxlen),
            )
        except Exception:
            res = self.redis.eval(
                _LUA_PUMP_RETRY_DUE_TO_OUTBOX, 2,
                self.retry_schedule_zset, self.outbox_stream,
                str(now_ms), str(self.batch), self.retry_lease_prefix, str(self.lease_ttl_ms), str(self.outbox_maxlen),
            )
        if res and isinstance(res, (list, tuple)) and len(res) >= 3:
            moved, missing, leased = int(res[0] or 0), int(res[1] or 0), int(res[2] or 0)
            if moved:
                logger.info("retry pump moved=%d missing=%d leased_skip=%d", moved, missing, leased)

    def run(self) -> None:
        logger.info("SignalRetryScheduler started zset=%s outbox=%s", self.retry_schedule_zset, self.outbox_stream)
        while True:
            try:
                self.pump_once()
            except KeyboardInterrupt:
                logger.info("SignalRetryScheduler stopped")
                return
            except Exception as e:
                if is_transient_error(e):
                    time.sleep(0.2 + random.random() * 0.8)
                else:
                    logger.error("retry pump error: %s", e, exc_info=True)
                    time.sleep(1.0)
            time.sleep(max(0.01, self.interval_ms / 1000.0))


if __name__ == "__main__":
    SignalRetryScheduler().run()
