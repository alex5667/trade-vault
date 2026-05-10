from __future__ import annotations

"""
Integration module for bad time quarantine with Redis stream and Prometheus metrics.

This module wires up TickTimeGuard + BadTimeQuarantine with:
- Redis stream for sampled bad time payloads
- Prometheus metrics
- Signal quality impact tracking
"""

import asyncio
import json
import logging
import os
from typing import Any

from common.tick_time import SanitizeResult, TickTimeGuard, TickTimePolicy
from common.time_quarantine import BadTimeQuarantine, BadTimeQuarantinePolicy
from services.orderflow.metrics import (
    tick_time_hard_drop_total,
    tick_time_quarantine_active_gauge,
    tick_time_quarantine_enabled_total,
    tick_time_quarantine_score_gauge,
    tick_time_recovery_passed_total,
    tick_time_soft_event_total,
    tick_time_state_freeze_total,
)
from utils.task_manager import safe_create_task
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("tick_time_quarantine")


class TickTimeQuarantineIntegration:
    """
    Integration wrapper for TickTimeGuard + BadTimeQuarantine with Redis stream and metrics.
    """

    def __init__(
        self,
        symbol: str,
        redis_client: Any | None = None,
        *,
        sample_rate: float = 0.01,  # 1% sampling for Redis stream
        stream_name: str | None = None,
        stream_maxlen: int = 50000,
    ):
        self.symbol = symbol
        self.redis_client = redis_client
        self.sample_rate = float(sample_rate)
        self.stream_name = stream_name or os.getenv(
            "BAD_TIME_QUARANTINE_STREAM", "stream:tick_time:quarantine"
        )
        self.stream_maxlen = int(stream_maxlen)

        # Initialize TickTimeGuard
        policy = TickTimePolicy(
            max_future_ms=int(os.getenv("TICK_TIME_MAX_FUTURE_MS", "500")),
            max_past_ms=int(os.getenv("TICK_TIME_MAX_PAST_MS", "5000")),
            max_reorder_ms=int(os.getenv("TICK_TIME_MAX_REORDER_MS", "1500")),
            clamp_soft_future=os.getenv("TICK_TIME_CLAMP_SOFT_FUTURE", "1").lower()
            in {"1", "true", "yes"},
            allow_soft_reorder=os.getenv("TICK_TIME_ALLOW_SOFT_REORDER", "1").lower()
            in {"1", "true", "yes"},
        )
        self.tick_time_guard = TickTimeGuard(policy)

        # Initialize BadTimeQuarantine with metrics callback
        quarantine_policy = BadTimeQuarantinePolicy()
        self.quarantine = BadTimeQuarantine(
            policy=quarantine_policy, inc=self._inc_metric
        )

        # Track last update for metrics
        self._last_metrics_update_ms = 0
        self._metrics_update_interval_ms = 1000  # Update metrics every 1s

    def _inc_metric(self, name: str, delta: int = 1) -> None:
        """Callback for BadTimeQuarantine to increment Prometheus metrics."""
        try:
            if name == "tick.time.quarantine.enabled":
                tick_time_quarantine_enabled_total.labels(
                    symbol=self.symbol, reason="streak_or_score"
                ).inc(delta)
            elif name.startswith("tick.time.hard_drop"):
                reason = name.replace("tick.time.hard_drop.", "").replace(
                    "tick.time.hard_drop", "unknown"
                )
                tick_time_hard_drop_total.labels(symbol=self.symbol, reason=reason).inc(
                    delta
                )
            elif name.startswith("tick.time.soft_event"):
                flag = name.replace("tick.time.soft_event.", "").replace(
                    "tick.time.soft_event", "unknown"
                )
                tick_time_soft_event_total.labels(symbol=self.symbol, flag=flag).inc(
                    delta
                )
            elif name == "tick.time.state_freeze.enabled":
                tick_time_state_freeze_total.labels(symbol=self.symbol).inc(delta)
            elif name == "tick.time.recovery.passed":
                tick_time_recovery_passed_total.labels(symbol=self.symbol).inc(delta)
        except Exception as e:
            logger.debug("Failed to increment metric %s: %r", name, e)

    def _should_sample(self, ts_ms: int) -> bool:
        """Deterministic sampling based on timestamp."""
        try:
            if self.sample_rate <= 0:
                return False
            if self.sample_rate >= 1:
                return True
            return (int(ts_ms) % 10000) < int(self.sample_rate * 10000)
        except Exception:
            return False

    async def _publish_to_redis_stream(
        self, payload: dict[str, Any], now_ms: int
    ) -> None:
        """Publish sampled bad time payload to Redis stream (fail-open)."""
        if not self.redis_client:
            return
        if not self._should_sample(now_ms):
            return

        try:
            fields = {
                "symbol": self.symbol,
                "ts_ms": str(now_ms),
                "payload": json.dumps(payload, ensure_ascii=False),
            }
            await self.redis_client.xadd(
                self.stream_name, fields, maxlen=self.stream_maxlen, approximate=True
            )
        except Exception as e:
            logger.debug("Failed to publish to Redis stream: %r", e)

    def sanitize_and_track(
        self, ts: Any, *, now_ms: int | None = None
    ) -> SanitizeResult | None:
        """
        Sanitize timestamp and track bad time events.
        Returns SanitizeResult or None if ts cannot be parsed.
        """
        now_ms = int(now_ms) if now_ms is not None else get_ny_time_millis()

        # Sanitize timestamp
        ts_res = self.tick_time_guard.sanitize_ts_ms(ts, now_ms=now_ms)

        if ts_res is None:
            # Cannot parse ts at all -> hard drop
            self.quarantine.on_hard_drop("bad_ts", now_ms)
            # Fire-and-forget Redis publish (best-effort, non-blocking)
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    safe_create_task(
                        self._publish_to_redis_stream(
                            {
                                "reason": "bad_ts",
                                "ts": str(ts),
                                "now_ms": now_ms,
                                "error": "cannot_parse",
                            },
                            now_ms,
                        )
                    )
            except Exception:
                pass  # Fail-open: don't block on Redis publish
            return None

        if ts_res.drop_reason:
            # Hard drop (future/past/reorder_hard etc)
            self.quarantine.on_hard_drop(str(ts_res.drop_reason), now_ms)
            # Fire-and-forget Redis publish
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    safe_create_task(
                        self._publish_to_redis_stream(
                            {
                                "reason": str(ts_res.drop_reason),
                                "ts_ms": ts_res.ts_ms,
                                "now_ms": now_ms,
                                "flags": ts_res.flags,
                            },
                            now_ms,
                        )
                    )
            except Exception:
                pass
        else:
            # Apply normalized ts
            if ts_res.flags:
                # Soft events (clamped/reorder_soft)
                for flag in ts_res.flags:
                    self.quarantine.on_soft_event(str(flag))
                    if self._should_sample(now_ms):
                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                safe_create_task(
                                    self._publish_to_redis_stream(
                                        {
                                            "reason": "soft_event",
                                            "flag": str(flag),
                                            "ts_ms": ts_res.ts_ms,
                                            "now_ms": now_ms,
                                        },
                                        now_ms,
                                    )
                                )
                        except Exception:
                            pass
            else:
                # OK tick
                self.quarantine.on_ok_tick()

        # Update metrics periodically
        self._update_metrics(now_ms)

        return ts_res

    def _update_metrics(self, now_ms: int) -> None:
        """Update Prometheus gauges (throttled)."""
        if (now_ms - self._last_metrics_update_ms) < self._metrics_update_interval_ms:
            return

        try:
            self._last_metrics_update_ms = now_ms
            is_quarantined = self.quarantine.is_quarantined(now_ms)
            tick_time_quarantine_active_gauge.labels(symbol=self.symbol).set(
                1 if is_quarantined else 0
            )
            tick_time_quarantine_score_gauge.labels(symbol=self.symbol).set(
                float(self.quarantine.score)
            )
        except Exception as e:
            logger.debug("Failed to update metrics: %r", e)

    def is_quarantined(self, now_ms: int) -> bool:
        """Check if currently quarantined."""
        return self.quarantine.is_quarantined(now_ms)

    def should_suppress_processing(self, now_ms: int) -> bool:
        """Check if processing should be suppressed (state freeze / recovery)."""
        return self.quarantine.should_suppress_processing(now_ms)

    @property
    def quarantine_score(self) -> float:
        """Current quarantine score."""
        return float(self.quarantine.score)

    @property
    def hard_streak(self) -> int:
        """Current hard drop streak."""
        return int(self.quarantine.hard_streak)

