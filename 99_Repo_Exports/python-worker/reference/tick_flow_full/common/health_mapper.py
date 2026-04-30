from typing import Any, Dict, Optional
import math

class HealthMetricsMapper:
    """
    Centralized logic for extracting health metrics from OrderflowSignalContext.
    Prevents tight coupling and fragile getattr chains in handlers.
    """

    @staticmethod
    def _to_float_or_nan(x: Any) -> float:
        try:
            return float(x)
        except (TypeError, ValueError):
            return float("nan")

    @staticmethod
    def _to_opt_float(x: Any) -> Optional[float]:
        if x is None:
            return None
        try:
            val = float(x)
            if math.isnan(val):
                return None
            return val
        except (TypeError, ValueError):
            return None

    @classmethod
    def extract(cls, symbol: str, ctx: Any) -> Dict[str, Any]:
        """
        Safely extract metrics from context, handling missing attributes gracefully.
        """
        # Base metrics (usually present)
        l2_age_ms = getattr(ctx, "l2_age_ms", float("nan"))
        z_score = getattr(ctx, "z_score", 0.0)
        obi = getattr(ctx, "obi", 0.0)
        obi_20 = getattr(ctx, "obi_20", 0.0)
        obi_sustained = getattr(ctx, "obi_sustained", False)

        # Optional/Complex fields
        spread_bps = cls._to_float_or_nan(getattr(ctx, "spread_bps", None))
        
        eta_fill_ms = cls._to_opt_float(getattr(ctx, "eta_fill_ms", None))
        burst_ratio = cls._to_opt_float(getattr(ctx, "burst_ratio", None))
        imbalance_min = cls._to_opt_float(getattr(ctx, "imbalance_min", None))
        
        # L2 staleness
        l2_is_stale = bool(getattr(ctx, "l2_is_stale", True))

        return dict(
            symbol=symbol
            l2_age_ms=l2_age_ms
            z_score=z_score
            obi=obi
            obi_20=obi_20
            obi_sustained=obi_sustained
            spread_bps=spread_bps
            eta_fill_ms=eta_fill_ms
            burst_ratio=burst_ratio
            imbalance_min=imbalance_min
            l2_is_stale=l2_is_stale
        )
