from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
Signal Bus over Redis Streams.

Handles publishing signals/plans/exec-events to Redis Streams
for consumption by MT5/NestJS workers.
"""


import json
from typing import Any

import redis.asyncio as redis

from .context import SignalContext
from .models import ExecutionPlan


class SignalBus:
    """
    Bus over Redis Streams:
      - publishing signals / plans / exec-events;
      - NestJS / MT5 workers can read their respective streams.
    """

    def __init__(self, redis_url: str):
        self._r = redis.from_url(redis_url, decode_responses=True)

        self.key_detected = "stream:signals:detected"
        self.key_plans = RS.SIGNAL_PLANS
        self.key_exec_events = "stream:signals:exec_events"
        self.key_performance = "stream:signals:performance"

    # --- publishing signals ---

    async def publish_detected(self, ctx: SignalContext) -> str:
        payload = ctx.to_dict()
        data = {
            "signal_id": ctx.signal_id,
            "symbol": ctx.symbol,
            "setup_type": ctx.setup_type,
            "side": ctx.side.value,
            "ts_signal": ctx.ts_signal.isoformat(),
            "payload": json.dumps(payload),
        }
        msg_id = await self._r.xadd(self.key_detected, data, maxlen=50000, approximate=True)
        return msg_id

    async def publish_plan(self, ctx: SignalContext, plan: ExecutionPlan) -> str:
        payload = {
            "ctx": ctx.to_dict(),
            "plan": self._plan_to_dict(plan),
        }
        data = {
            "signal_id": ctx.signal_id,
            "symbol": ctx.symbol,
            "setup_type": ctx.setup_type,
            "side": ctx.side.value,
            "ts_signal": ctx.ts_signal.isoformat(),
            "payload": json.dumps(payload),
        }
        msg_id = await self._r.xadd(self.key_plans, data, maxlen=50000, approximate=True)
        return msg_id

    async def publish_exec_event(
        self,
        signal_id: str,
        symbol: str,
        event_type: str,
        ts_iso: str,
        price: float,
        extra: dict[str, Any] | None = None,
    ) -> str:
        extra = extra or {}
        data = {
            "signal_id": signal_id,
            "symbol": symbol,
            "event_type": event_type,
            "ts": ts_iso,
            "price": str(price),
            "extra": json.dumps(extra),
        }
        msg_id = await self._r.xadd(self.key_exec_events, data, maxlen=50000, approximate=True)
        return msg_id

    async def publish_performance(self, perf_dict: dict[str, Any]) -> str:
        """
        Optionally: publish brief summary of signal outcome,
        so NestJS/dashboard can listen for results.
        """
        signal_id = perf_dict["signal_id"]
        symbol = perf_dict["symbol"]
        data = {
            "signal_id": signal_id,
            "symbol": symbol,
            "payload": json.dumps(perf_dict),
        }
        msg_id = await self._r.xadd(self.key_performance, data, maxlen=50000, approximate=True)
        return msg_id

    @staticmethod
    def _plan_to_dict(plan: ExecutionPlan) -> dict[str, Any]:
        return {
            "signal_id": plan.signal_id,
            "symbol": plan.symbol,
            "setup_type": plan.setup_type,
            "side": plan.side.value,
            "ts_signal": plan.ts_signal.isoformat(),
            "price_at_signal": plan.price_at_signal,
            "entry_zone_low": plan.entry_zone_low,
            "entry_zone_high": plan.entry_zone_high,
            "stop_price": plan.stop_price,
            "tp_levels": plan.tp_levels,
            "partials": plan.partials,
            "pos_risk_R": plan.pos_risk_R,
            "risk_usd": plan.risk_usd,
            "position_size": plan.position_size,
            "expiry_bars": plan.expiry_bars,
            "created_at": plan.created_at.isoformat(),
            "meta": plan.meta,
        }
