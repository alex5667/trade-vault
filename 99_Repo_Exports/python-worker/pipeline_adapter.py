# pipeline_adapter.py
"""
Pipeline adapters for unified signal processing.
Extracted from handlers/base_orderflow_handler.py to improve modularity.
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from contexts import OrderflowSignalContext, BarSample
    from signals.unified_pipeline import UnifiedSignalPipeline, PipelineSignalContext


class PipelineAdapter:
    """Adapter for unified signal pipeline integration."""

    def __init__(self, unified_pipeline: Optional["UnifiedSignalPipeline"] = None):
        self._unified_pipeline = unified_pipeline

    def _build_signal_context(self, bar: "BarSample") -> "PipelineSignalContext":
        """
        Строит PipelineSignalContext из BarSample для unified pipeline.
        """
        # This would create a proper PipelineSignalContext
        # Simplified implementation
        from signals.unified_pipeline import PipelineSignalContext

        return PipelineSignalContext(
            symbol="unknown",  # Would be set from context
            ts=bar.ts,
            price=(bar.high + bar.low) / 2.0,  # Mid price
            volume=bar.volume,
            confidence=0.5,
            metadata={
                "bar_high": bar.high,
                "bar_low": bar.low,
                "source": "orderflow"
            }
        )

    def _generate_signals_unified(self, bar: "BarSample") -> list:
        """
        Генерирует сигналы через unified pipeline.
        """
        if not self._unified_pipeline:
            return []

        # Build context and generate signals
        pipeline_ctx = self._build_signal_context(bar)

        try:
            signals = self._unified_pipeline.process_signal(pipeline_ctx)
            return signals or []
        except Exception:
            return []

    def _build_orderflow_context_from_bar(
        self,
        bar: "BarSample",
        additional_context: Optional[dict] = None
    ) -> "OrderflowSignalContext":
        """
        Строит OrderflowSignalContext из BarSample.
        """
        from contexts import OrderflowSignalContext

        # Calculate basic metrics from bar
        price = (bar.high + bar.low) / 2.0
        z_delta = 0.0  # Would be calculated from ATR and position

        # Weak progress detection
        range_size = bar.high - bar.low
        weak_progress_raw = range_size / max(price * 0.001, 0.0001)  # Rough ATR estimate
        weak_progress = weak_progress_raw < 10.0  # Threshold

        ctx = OrderflowSignalContext(
            symbol="unknown",  # Would be set properly
            ts=int(bar.ts),
            price=price,
            z_delta=z_delta,
            weak_progress=weak_progress,
            weak_progress_raw=weak_progress_raw,
            volume=bar.volume
        )

        # Add additional context if provided
        if additional_context:
            for key, value in additional_context.items():
                if hasattr(ctx, key):
                    setattr(ctx, key, value)

        return ctx

    def _build_signal_from_context(
        self,
        pipeline_ctx: "PipelineSignalContext",
        bar: "BarSample"
    ):
        """
        Строит финальный Signal из PipelineSignalContext и BarSample.
        """
        # This would create a proper Signal object
        # Simplified implementation
        return {
            "symbol": pipeline_ctx.symbol,
            "ts": pipeline_ctx.ts,
            "price": pipeline_ctx.price,
            "direction": "buy" if pipeline_ctx.confidence > 0.5 else "sell",
            "confidence": pipeline_ctx.confidence,
            "source": "unified_pipeline"
        }
