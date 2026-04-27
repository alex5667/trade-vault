"""
Calibration Service

Применяет локальную калибровку метрик на основе исторических данных.
"""

from typing import Any
from signals.unified_pipeline import SignalContext


class CalibrationService:
    """
    Сервис для применения локальной калибровки метрик.
    """

    def __init__(self, calibration_store=None):
        """
        Args:
            calibration_store: Хранилище калибровочных данных (опционально)
        """
        self.calibration_store = calibration_store

    def apply_local_calibration(self, ctx: SignalContext) -> None:
        """
        Применяет локальную калибровку к ключевым метрикам в SignalContext.
        """
        if self.calibration_store is None:
            return

        # Метрики для калибровки
        metrics_to_calibrate = [
            "delta_spike_z",
            "obi",
            "weak_progress",
            "atr_quantile",
        ]

        for metric_name in metrics_to_calibrate:
            self._apply_metric_calibration(ctx, metric_name)

    def _apply_metric_calibration(self, ctx: SignalContext, metric_name: str) -> None:
        """
        Применяет калибровку к конкретной метрике.
        """
        if self.calibration_store is None:
            return

        # Получаем сырое значение метрики из orderflow контекста
        raw_value = self._get_metric_value(ctx.of, metric_name)
        if raw_value is None:
            return

        try:
            # Применяем локальную калибровку
            calibrated_value = self.calibration_store.eval_local_quantile(
                symbol=ctx.symbol,
                metric=metric_name,
                value=raw_value,
                session=ctx.session,
                regime=ctx.regime.regime_type if ctx.regime else "unknown"
            )

            # Сохраняем калиброванное значение
            self._set_calibrated_metric(ctx, metric_name, calibrated_value)

        except Exception:
            # В случае ошибки пропускаем калибровку
            pass

    def _get_metric_value(self, of_ctx, metric_name: str) -> Any:
        """
        Извлекает значение метрики из OrderflowContext.
        """
        # Маппинг имен метрик на поля в OrderflowContext
        metric_mapping = {
            "delta_spike_z": "z_delta",
            "obi": "obi",
            "weak_progress": "weak_progress",
            "atr_quantile": "atr_q_14",
        }

        field_name = metric_mapping.get(metric_name)
        if field_name:
            return getattr(of_ctx, field_name, None)

        return None

    def _set_calibrated_metric(self, ctx: SignalContext, metric_name: str, value: float) -> None:
        """
        Сохраняет калиброванное значение метрики в SignalContext.
        """
        # Маппинг для сохранения калиброванных значений
        calibrated_mapping = {
            "delta_spike_z": "delta_spike_z_local_q",
            "obi": "obi_local_q",
            "weak_progress": "weak_progress_local_q",
            "atr_quantile": "atr_local_q",
        }

        attr_name = calibrated_mapping.get(metric_name)
        if attr_name:
            setattr(ctx, attr_name, value)
