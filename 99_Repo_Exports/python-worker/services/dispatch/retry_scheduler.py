import json
import time
import contextlib
from typing import Any

from services.dispatcher.delivery_helpers import DeliveryHelpers
from utils.time_utils import get_ny_time_millis


class RetryScheduler:
    def __init__(self, config: Any, redis_client: Any, lua_scripts: Any, dlq_writer: Any, target_router: Any, ctr: dict[str, int]):
        self.config = config
        self.redis = redis_client
        self.lua_scripts = lua_scripts
        self.dlq_writer = dlq_writer
        self.target_router = target_router
        self.ctr = ctr
        self._last_retry_drain = 0.0

    def retry_delay_ms(self, attempt: int) -> int:
        return DeliveryHelpers.calculate_retry_delay(
            attempt - 1,  # DeliveryHelpers uses 0-indexed attempts
            base_ms=self.config.retry_base_ms,
            max_ms=self.config.retry_max_ms,
            jitter_ms=self.config.retry_jitter_ms
        )

    def schedule_target_retry(self, *, target: str, sid: str, env: dict[str, Any], attempt: int, last_error: str) -> None:
        if attempt >= self.config.ack_retry_max: # reusing ack_retry_max config or should it be max_attempts?
            # Target-specific DLQ is more useful than generic DLQ here:
            with contextlib.suppress(Exception):
                self.dlq_writer.send_target_dlq(
                    target=str(target),
                    sid=str(sid),
                    env=env if isinstance(env, dict) else {},
                    reason="target_max_attempts",
                    err=str(last_error),
                )
            return
            
        delay = self.retry_delay_ms(attempt)

        # Next level: retry dedup per (target,sid) to prevent ZSET explosions.
        try:
            dk = DeliveryHelpers.retry_dedup_key(self.config.retry_dedup_prefix, target, sid)
            ok = self.redis.set(dk, "1", nx=True, px=int(delay) + 1000)
            if not ok:
                self.ctr["retry_dedup_hit"] += 1
                return
        except Exception:
            pass

        payload = {
            "sid": sid,
            "target": target,
            "attempt": attempt,
            "ts_ms": get_ny_time_millis(),
            "env": env,
            "last_error": last_error,
        }
        member = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        score = get_ny_time_millis() + delay
        self.redis.zadd(self.config.retry_zset, {member: score})

    def drain_retries_best_effort(self) -> None:
        now = time.monotonic()
        if (now - self._last_retry_drain) * 1000 < self.config.retry_drain_every_ms:
            return
        self._last_retry_drain = now
        try:
            now_ms = get_ny_time_millis()
            items = self.lua_scripts.execute("zpop_due", keys=[self.config.retry_zset], args=[str(now_ms), str(self.config.retry_pop_limit)])
        except Exception:
            return
        if not items:
            return
        for raw in items:
            try:
                obj = json.loads(raw)
                sid = (obj.get("sid") or "")
                target = (obj.get("target") or "")
                attempt = int(obj.get("attempt") or 0)
                env = obj.get("env") or {}
                if not sid or not target or not isinstance(env, dict):
                    continue
                # Assuming target_router has deliver_targets_with_retry
                self.target_router.deliver_targets_with_retry(env, sid, targets=[target], base_attempts={"__forced__": attempt})
            except Exception:
                continue
