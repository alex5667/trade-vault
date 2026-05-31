import time
from typing import Any
from core.redis_stream_consumer import AsyncRedisStreamHelper
from services.dispatcher.observability import sd_fail_open


class DispatchMetrics:
    def __init__(self, config: Any, redis_client: Any, logger: Any, ctr: dict[str, int]):
        self.config = config
        self.redis = redis_client
        self.logger = logger
        self.ctr = ctr
        self._last_metrics_mono = 0.0
        self._last_diag = 0.0
        self._last_diag_mono = 0.0

    async def pending_oldest_idle_ms(self) -> int:
        try:
            # XPENDING returns raw list in async redis too
            rows = await self.redis.execute_command("XPENDING", self.config.outbox_stream, self.config.group, "-", "+", 1)
        except Exception:
            return -1
        if not isinstance(rows, list) or not rows:
            return 0
        r = rows[0]
        if not isinstance(r, (list, tuple)) or len(r) < 3:
            return -1
        try:
            return int(r[2])
        except Exception:
            return -1

    async def pending_by_consumer(self, limit: int = 50) -> dict[str, int]:
        try:
            rows = await self.redis.execute_command("XPENDING", self.config.outbox_stream, self.config.group, "-", "+", int(limit))
        except Exception:
            return {}
        out: dict[str, int] = {}
        if isinstance(rows, list):
            for r in rows:
                if not isinstance(r, (list, tuple)) or len(r) < 2:
                    continue
                consumer = str(r[1])
                out[consumer] = out.get(consumer, 0) + 1
        return out

    async def emit_metrics(self, helper: AsyncRedisStreamHelper) -> None:
        try:
            outbox_len = int(await self.redis.xlen(self.config.outbox_stream))
        except Exception:
            outbox_len = -1
        try:
            pending = int(await helper.pending_len(self.config.outbox_stream))
        except Exception:
            pending = -1

        by_consumer = await self.pending_by_consumer(limit=50)
        oldest_idle = await self.pending_oldest_idle_ms()

        self.logger.info(
            "outbox metrics: len=%s pending=%s oldest_idle_ms=%s read_count=%d block_ms=%d claim_idle_ms=%d ctr=%s pending_by_consumer=%s",
            outbox_len,
            pending,
            oldest_idle,
            self.config.read_count,
            self.config.read_block_ms,
            self.config.claim_min_idle_ms,
            dict(self.ctr),
            by_consumer,
        )

    async def diag(self, helper: AsyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if now - self._last_diag < self.config.outbox_diag_every_sec:
            return
        self._last_diag = now
        try:
            info = await helper.pending_details(self.config.outbox_stream)  # type: ignore
            pending = int(info.get("pending", 0) or 0)  # type: ignore
            cons = info.get("consumers") or []
            oldest_idle = await helper.pending_oldest_idle_ms(self.config.outbox_stream)  # type: ignore
            self.logger.info(  # type: ignore
                "outbox pending=%d oldest_idle_ms=%d consumers=%s ctr=%s",
                pending,
                oldest_idle,
                cons,
                dict(list(self.ctr.items())[:20]),
            )
        except Exception as e:
            def _incr(key: str) -> None:
                self.ctr[key] += 1
            sd_fail_open(
                self.logger,
                key="outbox_pending_by_consumer_metrics_error",
                err=e,
                incr_fn=_incr,
                metric_key=f"{self.config.metrics_prefix}:outbox_pending_by_consumer_metrics_errors_total",
            )

    async def maybe_log_diagnostics(self, helper: AsyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if now - self._last_diag_mono < float(self.config.diag_every_sec):
            return
        self._last_diag_mono = now
        try:
            p = await helper.pending_len(self.config.outbox_stream)
            by = await helper.pending_by_consumer(self.config.outbox_stream)  # type: ignore
            self.logger.info("outbox_pending=%s pending_by_consumer=%s", p, dict(by) if by else {})  # type: ignore
        except Exception:
            pass

    async def tick_metrics(self, helper: AsyncRedisStreamHelper) -> None:
        now = time.monotonic()
        if now - self._last_metrics_mono >= float(self.config.metrics_every_sec):
            self._last_metrics_mono = now
            await self.emit_metrics(helper)
