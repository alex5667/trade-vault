# signal_execution_service.py
"""
Signal execution functionality extracted from base_orderflow_handler.py
"""

from __future__ import annotations

from typing import Optional, Dict, Any
import time

from contexts import OrderflowSignalContext
from signals.outbox_utils import PublishResult
# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)


class SignalExecutionService:
    """
    Service for signal generation and execution handling.
    """

    def __init__(self, symbol: str, signal_generator: Any, config_manager: Any):
        self.symbol = symbol
        self.signal_generator = signal_generator
        self.config_manager = config_manager
        self.logger = setup_logger(f"SignalExecutionService:{symbol}")

        # Initialize execution components (will be set by handler if available)
        self._execution_planner = None
        self._signal_repo = None
        self._signal_bus = None
        self._performance_tracker = None

    def set_execution_components(self, execution_planner: Any = None, signal_repo: Any = None
                                signal_bus: Any = None, performance_tracker: Any = None) -> None:
        """Set execution components for signal handling."""
        self._execution_planner = execution_planner
        self._signal_repo = signal_repo
        self._signal_bus = signal_bus
        self._performance_tracker = performance_tracker


    def _handle_execution_for_signal(self, sig_ctx: OrderflowSignalContext, *, msg_id: Optional[str] = None) -> None:
        """
        Handle execution planning and setup for signal.
        IMPORTANT: вызывать только после реальной публикации (sent=True, dedup=False).
        """
        try:
            # Get execution plan if available
            execution_planner = getattr(self, '_execution_planner', None)
            if execution_planner:
                # msg_id помогает идемпотентности downstream
                try:
                    plan = execution_planner.create_plan(sig_ctx, msg_id=msg_id)
                except TypeError:
                    plan = execution_planner.create_plan(sig_ctx)
                if plan:
                    # Save execution plan
                    signal_repo = getattr(self, '_signal_repo', None)
                    if signal_repo:
                        try:
                            signal_repo.save_execution_plan(sig_ctx.symbol, plan, msg_id=msg_id)
                        except TypeError:
                            signal_repo.save_execution_plan(sig_ctx.symbol, plan)

                    # Publish execution event
                    signal_bus = getattr(self, '_signal_bus', None)
                    if signal_bus:
                        signal_bus.publish_execution_plan(sig_ctx.symbol, plan)

            self.logger.debug(f"Execution handled for signal: {sig_ctx.symbol}")

        except Exception as e:
            self.logger.warning(f"Failed to handle execution for signal: {e}")

    def process_signal_context(self, ctx: OrderflowSignalContext) -> PublishResult:
        """
        Process signal context through all stages.
        """
        # Генерация и публикация сигнала (единый источник истины)
        result: PublishResult = self.signal_generator.generate(ctx)

        # Execution только для реально опубликованного (не rejected, не dedup)
        if result.sent and (not result.dedup):
            self._handle_execution_for_signal(ctx, msg_id=getattr(result, "msg_id", None))

        return result


    def get_execution_stats(self) -> Dict[str, Any]:
        """Get execution statistics."""
        try:
            performance_tracker = getattr(self, '_performance_tracker', None)
            if performance_tracker:
                return performance_tracker.get_stats(self.symbol)
        except Exception as e:
            self.logger.warning(f"Failed to get execution stats: {e}")

        return {
            'total_signals': 0
            'executed_signals': 0
            'success_rate': 0.0
            'avg_execution_time_ms': 0.0
        }
