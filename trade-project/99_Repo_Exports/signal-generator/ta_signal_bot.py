# signal_generator.py
"""
Signal generation functionality extracted from base_orderflow_handler.py
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List
import time

from contexts import OrderflowSignalContext
# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)


class SignalGenerator:
    """
    Service for generating and publishing signals.
    """

    def __init__(self, symbol: str, config: Any, outbox: Any):
        self.symbol = symbol
        self.config = config
        self.outbox = outbox
        self.logger = setup_logger(f"SignalGenerator:{symbol}")

        # Signal generation settings
        self.min_trades_breakout = int(getattr(config, 'min_trades_breakout', 20))
        self.burst_ratio_min = float(getattr(config, 'burst_ratio_min', 1.6))
        self.fano_min = float(getattr(config, 'fano_min', 1.5))
        self.flip_ratio_max = float(getattr(config, 'flip_ratio_max', 0.70))
        self.imbalance_min = float(getattr(config, 'imbalance_min', 0.20))

    def _burst_gate_ok(self, ctx: OrderflowSignalContext) -> bool:
        """Check burst quality gate."""
        burst_stats = getattr(ctx, 'burst_stats', None)
        if not burst_stats:
            return True  # No burst data, allow

        trade_count = getattr(burst_stats, 'trade_count_bucket', 0)
        burst_ratio = getattr(burst_stats, 'burst_ratio', 0.0)
        fano = getattr(burst_stats, 'fano_counts', 0.0)
        flip_ratio = getattr(burst_stats, 'flip_ratio', 1.0)

        return (
            trade_count >= self.min_trades_breakout
            and burst_ratio >= self.burst_ratio_min
            and fano >= self.fano_min
            and flip_ratio <= self.flip_ratio_max
        )

    def _exec_quality_ok(self, ctx: OrderflowSignalContext, impulse_side: str) -> bool:
        """Check execution quality."""
        # Check burst gate
        if not self._burst_gate_ok(ctx):
            return False

        # Check OBI imbalance
        obi = getattr(ctx, 'obi', 0.0)
        imbalance_threshold = self.imbalance_min

        if impulse_side == "buy" and obi < imbalance_threshold:
            return False
        elif impulse_side == "sell" and obi > -imbalance_threshold:
            return False

        return True

    def _cooldown_ok(self, kind: str, level_key: str, ts: int) -> bool:
        """Check cooldown for signal generation."""
        # Placeholder - would check Redis for last signal time
        return True

    def _mark_cooldown(self, kind: str, level_key: str, ts: int) -> None:
        """Mark cooldown timestamp."""
        # Placeholder - would set Redis key
        pass

    def _compute_confidence(
        self,
        ctx: OrderflowSignalContext,
        signal_type: str,
    ) -> tuple[Optional[float], Optional[Dict[str, float]]]:
        """Compute signal confidence."""
        # Basic confidence calculation
        confidence_components = {}

        # OBI component
        obi_conf = min(abs(ctx.obi) / 0.5, 1.0) if hasattr(ctx, 'obi') else 0.5
        confidence_components['obi'] = obi_conf

        # Delta component
        delta_conf = min(abs(ctx.z_delta) / 2.0, 1.0) if hasattr(ctx, 'z_delta') else 0.5
        confidence_components['delta'] = delta_conf

        # Burst quality component
        burst_conf = 1.0 if self._burst_gate_ok(ctx) else 0.3
        confidence_components['burst'] = burst_conf

        # Combined confidence
        weights = {'obi': 0.4, 'delta': 0.4, 'burst': 0.2}
        confidence = sum(
            confidence_components.get(k, 0.5) * w
            for k, w in weights.items()
        )

        return confidence, confidence_components

    def _custom_signal_conditions(self, ctx: OrderflowSignalContext) -> Optional[Dict[str, Any]]:
        """Check custom signal conditions."""
        # Placeholder for custom conditions
        return None

    def _apply_experiment_filters(self, ctx: OrderflowSignalContext, signal_type: str) -> bool:
        """Apply experiment-specific filters."""
        # Placeholder
        return True

    def _apply_baseline_filters(self, ctx: OrderflowSignalContext, signal_type: str) -> bool:
        """Apply baseline filters."""
        # Basic filters
        if hasattr(ctx, 'z_delta') and abs(ctx.z_delta) < 1.0:
            return False

        if hasattr(ctx, 'passes_thresholds') and not ctx.passes_thresholds:
            return False

        return True

    def _apply_experimental_filters(self, ctx: OrderflowSignalContext, signal_type: str) -> bool:
        """Apply experimental filters."""
        return True

    def _generate_signals(self, ctx: OrderflowSignalContext) -> bool:
        """Generate signals from context."""
        # Check basic conditions
        if not self._apply_baseline_filters(ctx, "unknown"):
            return False

        if not self._apply_experimental_filters(ctx, "unknown"):
            return False

        # Check custom conditions
        custom = self._custom_signal_conditions(ctx)
        if custom is None:
            return False

        # Determine signal direction
        direction = 0
        if hasattr(ctx, 'z_delta'):
            if ctx.z_delta > 1.5:
                direction = 1  # Long
            elif ctx.z_delta < -1.5:
                direction = -1  # Short

        if direction == 0:
            return False

        # Generate signal
        signal_type = "breakout" if abs(ctx.z_delta or 0) > 2.0 else "sweep"
        strength = min(abs(ctx.z_delta or 0) / 3.0, 1.0)

        # Emit signal
        self._emit_signal(ctx, direction, signal_type, strength)

        return True

    def _emit_signal(
        self,
        ctx: OrderflowSignalContext,
        direction: int,
        signal_type: str,
        strength: float,
    ) -> None:
        """Emit signal."""
        confidence, breakdown = self._compute_confidence(ctx, signal_type)

        envelope = {
            "symbol": ctx.symbol,
            "ts": ctx.ts,
            "direction": direction,
            "signal_type": signal_type,
            "strength": strength,
            "confidence": confidence,
            "breakdown": breakdown,
            "context": {
                "price": ctx.price,
                "z_delta": getattr(ctx, 'z_delta', 0.0),
                "obi": getattr(ctx, 'obi', 0.0),
            }
        }

        # Publish to outbox
        try:
            self.outbox.publish(envelope)
            self.logger.info(f"Signal emitted: {envelope}")
        except Exception as e:
            self.logger.error(f"Failed to publish signal: {e}")

    def _publish_signal(
        self,
        label: str,
        side: str,
        signal_type: str,
        strength: float,
        reason: str,
        ctx: OrderflowSignalContext,
        confidence_value: Optional[float] = None,
        entry_tag: str = "",
    ) -> Any:
        """Publish signal with full context."""
        # This is a wrapper around _emit_signal
        direction = 1 if side.lower() == "long" else -1

        envelope = {
            "symbol": ctx.symbol,
            "ts": ctx.ts,
            "direction": direction,
            "signal_type": signal_type,
            "strength": strength,
            "confidence": confidence_value,
            "reason": reason,
            "entry_tag": entry_tag,
            "context": {
                "price": ctx.price,
                "regime": getattr(ctx, 'regime', 'unknown'),
                "liquidity_score": getattr(ctx, 'liquidity_score', None),
                "geometry_score": getattr(ctx, 'geometry_score', None),
            }
        }

        # Add experiment context if available
        if hasattr(ctx, 'experiment_id'):
            envelope["experiment"] = {
                "id": ctx.experiment_id,
                "variant": ctx.experiment_variant,
            }

        try:
            result = self.outbox.publish(envelope)
            return type('PublishResult', (), {'sent': True, 'dedup': False})()
        except Exception as e:
            self.logger.error(f"Failed to publish signal: {e}")
            return type('PublishResult', (), {'sent': False, 'dedup': False})()
