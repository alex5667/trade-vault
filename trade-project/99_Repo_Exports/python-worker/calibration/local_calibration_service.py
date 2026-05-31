# calibration/local_calibration_service.py

from __future__ import annotations

from typing import Any, Protocol

from common.log import setup_logger
from local_calibration.store import LocalCalibrationStore as LCStoreV2
from local_calibration.store import eval_local_quantile


class SupportsCalibrationContext(Protocol):
    """Protocol for objects that support calibration"""
    symbol: str
    ts_event_ms: int
    # Metrics to calibrate - using existing interface
    metrics: dict[str, Any]
    calibrated: dict[str, Any]
    session: str | None = None
    regime_label: str | None = None


class LocalCalibrationService:
    """
    Service for applying local calibration to orderflow metrics.

    Uses the existing LCStoreV2 interface for compatibility with
    the current calibration system.
    """

    def __init__(self, store: LCStoreV2, config: dict[str, Any] | None = None):
        self._store = store
        self._cfg = config or self._default_config()
        self.logger = setup_logger("LocalCalibrationService")

    def _default_config(self) -> dict[str, Any]:
        return {
            "default_extreme_z": 2.0,  # Default z-score threshold for extreme values
            "metrics_to_calibrate": [
                "deltaSpike_z",
                "obi",
                "absorption_score",
                "liquidity_score",
                "weak_progress",
                "atr_quantile",
            ]
        }

    def update_store(self, ctx: SupportsCalibrationContext) -> None:
        """
        This method is not needed for the existing LCStoreV2 interface.
        The store is populated externally (from database or periodic jobs).
        """
        pass  # LCStoreV2 is read-only in this context

    def apply_calibration(self, ctx: SupportsCalibrationContext) -> None:
        """
        Apply local calibration to key metrics using the existing LCStoreV2 interface.

        This matches the logic from BaseOrderFlowHandler._apply_local_calibration
        """
        if self._store is None:
            return

        # Calibrate each metric
        for metric_name in self._cfg["metrics_to_calibrate"]:
            self._apply_metric_calibration(ctx, metric_name)

    def _apply_metric_calibration(
        self,
        ctx: SupportsCalibrationContext,
        metric_name: str,
        *,
        default_extreme_z: float = 2.0,
    ) -> None:
        """
        Calibrate a single metric using local calibration data.
        Direct copy of the logic from BaseOrderFlowHandler.
        """
        raw_value = ctx.metrics.get(metric_name)
        if raw_value is None:
            return

        # Use LCStoreV2 interface
        cfg = self._store.get_metric_cfg(
            ctx.symbol, ctx.session or "mixed", ctx.regime_label or "mixed", metric_name
        )
        if cfg:
            quantile = eval_local_quantile(cfg.cdf_points, raw_value)
            is_extreme = abs(raw_value) >= cfg.threshold
        else:
            quantile = 0.5  # neutral
            is_extreme = abs(raw_value) >= default_extreme_z
            cfg = None

        ctx.calibrated[metric_name] = {
            "value": raw_value,
            "is_extreme": is_extreme,
            "threshold": cfg.threshold if cfg else default_extreme_z,
            "quantile": quantile,
            "p50": cfg.q90 if cfg else None,  # Using q90 as approximation for p50
            "p75": cfg.q95 if cfg else None,  # Using q95 as approximation for p75
            "p90": cfg.q98 if cfg else None,  # Using q98 as approximation for p90
        }

    def get_calibration_stats(self, symbol: str, session: str = "mixed", regime: str = "mixed") -> dict[str, Any]:
        """Get calibration statistics for a symbol/session/regime combination"""
        stats = {}

        for metric_name in self._cfg["metrics_to_calibrate"]:
            cfg = self._store.get_metric_cfg(symbol, session, regime, metric_name)
            if cfg:
                stats[metric_name] = {
                    "q90": cfg.q90,
                    "q95": cfg.q95,
                    "q98": cfg.q98,
                    "threshold": cfg.threshold,
                    "count_samples": cfg.count_samples,
                    "cdf_points_count": len(cfg.cdf_points),
                }
            else:
                stats[metric_name] = None

        return stats
