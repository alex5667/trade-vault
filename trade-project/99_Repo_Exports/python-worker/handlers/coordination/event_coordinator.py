from utils.time_utils import get_ny_time_millis

"""
Event coordination for handler pipeline.

Extracted from BaseOrderFlowHandler to follow Single Responsibility Principle.
Manages:
- Bar close event handling
- Context building coordination
- Pivot updates
- Session and calibration attachment
"""

import logging
from collections.abc import Callable
from typing import Any


class EventCoordinator:
    """
    Coordinates events and context building for handler pipeline.
    
    Responsibilities:
    - Handle bar close events
    - Coordinate context building
    - Ensure pivots are up-to-date
    - Attach session and calibration data
    - Emit health metrics
    
    Delegates actual processing to injected services.
    """

    def __init__(
        self,
        symbol: str,
        data_processor: Any,
        cache_service: Any,
        session_service: Any,
        calibration_service: Any,
        health_metrics: Any | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize event coordinator.
        
        Args:
            symbol: Trading symbol
            data_processor: OrderFlowDataProcessor instance
            cache_service: CacheService instance
            session_service: SessionService instance
            calibration_service: CalibrationService instance
            health_metrics: Optional health metrics tracker
            logger: Optional logger instance
        """
        self.symbol = symbol
        self._data_processor = data_processor
        self._cache_service = cache_service
        self._session = session_service
        self._calibration = calibration_service
        self.health_metrics = health_metrics
        self.logger = logger or logging.getLogger(__name__)

        # Callbacks for health metrics
        self._emit_health_callback: Callable | None = None

    def set_health_callback(self, callback: Callable) -> None:
        """
        Set callback for emitting health metrics.
        
        Args:
            callback: Function(health_metrics, symbol, ctx) -> None
        """
        self._emit_health_callback = callback

    def on_bar_closed(self, bar: object) -> Any | None:
        """
        Handle 1-minute bar close event.
        
        This is the single entry point for bar-based signal generation.
        Updates pivots and builds signal context.
        
        Args:
            bar: Bar object with ts_open, ts_close, etc.
            
        Returns:
            Signal context or None if building failed
        """
        try:
            # Get bar timestamp - prefer close time for bar-signals
            event_ts_ms = self._get_bar_timestamp(bar)

            # Ensure pivots are up-to-date (daily check inside CacheService)
            self._ensure_pivots(event_ts_ms)

            # Get pivots from cache
            pivots = self._get_pivots()

            # Build signal context via data processor
            ctx = self._data_processor.build_signal_ctx(pivots=pivots)

            # Fix event timestamp to bar close time
            if hasattr(ctx, "ts"):
                ctx.ts = event_ts_ms

            # Attach session fields to context
            self._session.attach_to_ctx(ctx)

            # Apply calibration before signal processing
            self._calibration.calibrate_context(ctx)

            # Emit health metrics after context is built
            self._emit_health_metrics(ctx)

            return ctx

        except Exception as e:
            self.logger.warning("Failed to build context on bar close: %s", e)
            self._emit_health_error()
            return None

    def _get_bar_timestamp(self, bar: object) -> int:
        """Extract timestamp from bar object."""
        return int(
            getattr(bar, "ts_close", 0)
            or (int(getattr(bar, "ts_open", 0) or 0) + 60_000)
            or get_ny_time_millis()
        )

    def _ensure_pivots(self, event_ts_ms: int) -> None:
        """Ensure pivots are up-to-date for given timestamp."""
        try:
            self._cache_service.ensure_pivots_bundle(event_ts_ms)
        except Exception as e:
            self.logger.debug("ensure_pivots_bundle failed: %s", e)

    def _get_pivots(self) -> Any | None:
        """Get pivots from cache service."""
        try:
            return self._cache_service.get_pivots_bundle()
        except Exception as e:
            self.logger.debug("Failed to get pivots bundle: %s", e)
            return None

    def _emit_health_metrics(self, ctx: Any) -> None:
        """Emit health metrics for built context."""
        if self.health_metrics is None or self._emit_health_callback is None:
            return

        try:
            self._emit_health_callback(
                self.health_metrics,
                symbol=self.symbol,
                ctx=ctx
            )
        except Exception as e:
            self.logger.debug("health_metrics.on_tick failed: %s", e)

    def _emit_health_error(self) -> None:
        """Emit health error metric."""
        if self.health_metrics is None:
            return

        try:
            # Call health metrics error handler if available
            if hasattr(self.health_metrics, 'on_signal_bar_failed'):
                self.health_metrics.on_signal_bar_failed(
                    self.symbol,
                    tf="1m",
                    reason="build_ctx_failed"
                )
        except Exception:
            pass  # Fail-open
