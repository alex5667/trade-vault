"""
SignalService: Unified node in Python that:

- Receives ticks/bar + detects signals
- Builds ExecutionPlan
- Writes to Timescale
- Publishes to Redis
- Registers signal in PerformanceTracker
"""

from __future__ import annotations


from .context import SignalContext
from .execution_planner import ExecutionPlanner
from .performance_tracker import SignalPerformanceTracker
from .repository import SignalRepository
from .bus import SignalBus
from .models import Bar1m


class SignalService:
    """
    This is the "node" in Python that:
      - receives ticks/bar + calculates signals;
      - builds ExecutionPlan;
      - writes to Timescale;
      - publishes to Redis;
      - registers signal in PerformanceTracker.
    """

    def __init__(
        self,
        repo: SignalRepository,
        planner: ExecutionPlanner,
        tracker: SignalPerformanceTracker,
        bus: SignalBus,
    ):
        self.repo = repo
        self.planner = planner
        self.tracker = tracker
        self.bus = bus

    # called when your detector finds a setup
    async def on_new_signal(self, ctx: SignalContext) -> None:
        # 1) build plan
        plan = self.planner.build_plan(ctx)
        if plan is None:
            return

        # 2) write raw signal + plan to Timescale
        self.repo.insert_signal(ctx)
        self.repo.insert_execution_plan(plan)

        # 3) register signal in performance-tracker
        self.tracker.register_signal(ctx, plan)

        # 4) publish to Redis so MT5/NestJS can execute
        await self.bus.publish_detected(ctx)
        await self.bus.publish_plan(ctx, plan)

    # called from 1m-bar generator
    def on_bar(self, symbol: str, bar: Bar1m) -> None:
        self.tracker.on_bar(symbol, bar)

    # called from MT5-bridge / execution-engine (via Redis, HTTP — doesn't matter)
    async def on_exec_event(self, signal_id: str, symbol: str, event_type: str, ts, price: float) -> None:
        self.tracker.on_execution_event(signal_id, event_type, ts, price)

        # Also publish to Redis so other services can react
        await self.bus.publish_exec_event(
            signal_id=signal_id,
            symbol=symbol,
            event_type=event_type,
            ts_iso=ts.isoformat(),
            price=price,
        )
