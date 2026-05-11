"""
Batch Processor для массовой обработки данных с GPU ускорением.

Используется для обработки больших объемов данных батчами,
с автоматическим использованием GPU если доступен.
"""

import logging
from collections.abc import Callable
from typing import Any

import numpy as np

# ✅ GPU Support
try:
    from services.gpu_compute_service import get_gpu_service
    GPU_SERVICE_AVAILABLE = True
except ImportError:
    GPU_SERVICE_AVAILABLE = False
    get_gpu_service = None

log = logging.getLogger(__name__)


class BatchProcessor:
    """
    Процессор для батч-обработки данных с GPU ускорением.
    
    Автоматически группирует данные в батчи и использует GPU
    для массовых вычислений.
    """

    def __init__(self, batch_size: int = 1000, use_gpu: bool = True):
        """
        Инициализация процессора.
        
        Args:
            batch_size: Размер батча для обработки
            use_gpu: Использовать GPU если доступен
        """
        self.batch_size = batch_size
        self.use_gpu = use_gpu
        self.gpu_service = None

        if use_gpu and GPU_SERVICE_AVAILABLE:
            try:
                self.gpu_service = get_gpu_service()  # type: ignore
                if self.gpu_service and self.gpu_service.is_gpu_available():
                    log.info("🚀 Batch processor: GPU acceleration enabled")
                else:
                    log.info("📊 Batch processor: GPU not available, using CPU")
            except Exception as e:
                log.warning(f"⚠️ Batch processor: GPU initialization failed: {e}")
                self.gpu_service = None

    def process_candles_batch(
        self,
        candles: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Обрабатывает батч свечей с GPU ускорением.
        
        Args:
            candles: Список словарей с данными свечей
        
        Returns:
            Список обработанных свечей с метриками
        """
        if not candles:
            return []

        # Используем GPU сервис для массовых вычислений
        if self.gpu_service and self.gpu_service.is_gpu_available():
            try:
                metrics = self.gpu_service.process_candles_batch(candles)

                # Добавляем метрики к каждой свече
                results = []
                for i, candle in enumerate(candles):
                    result = candle.copy()
                    result.update({
                        'buy_vol': float(metrics['buy_vols'][i]),
                        'sell_vol': float(metrics['sell_vols'][i]),
                        'delta': float(metrics['deltas'][i]),
                        'cvd': float(metrics['cvd'][i]),
                        'z_delta': float(metrics['z_deltas'][i]),
                        'atr': float(metrics['atr'][i]),
                        'body_atr': float(metrics['body_atr'][i]),
                        'delta_ratio': float(metrics['delta_ratio'][i])
                    })
                    results.append(result)

                return results
            except Exception as e:
                log.warning(f"⚠️ GPU batch processing failed: {e}, falling back to CPU")

        # CPU fallback: обрабатываем по одной свече
        results = []
        for candle in candles:
            # Простая обработка без GPU
            result = candle.copy()
            result.update({
                'buy_vol': 0.0,
                'sell_vol': 0.0,
                'delta': 0.0,
                'cvd': 0.0,
                'z_delta': 0.0,
                'atr': 0.0,
                'body_atr': 0.0,
                'delta_ratio': 0.0
            })
            results.append(result)

        return results

    def process_in_batches(
        self,
        data: list[Any],
        processor: Callable[[list[Any]], list[Any]]
    ) -> list[Any]:
        """
        Обрабатывает данные батчами.
        
        Args:
            data: Список данных для обработки
            processor: Функция обработки батча
        
        Returns:
            Список обработанных данных
        """
        results = []

        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            processed = processor(batch)
            results.extend(processed)

            if (i // self.batch_size + 1) % 10 == 0:
                log.info(f"📊 Processed {i + len(batch)}/{len(data)} items...")

        return results

    def compute_deltas_batch(
        self,
        volumes: list[float],
        taker_buy_volumes: list[float] | None = None
    ) -> tuple:
        """
        Вычисляет дельты для батча объемов.
        
        Args:
            volumes: Список объемов
            taker_buy_volumes: Список объемов покупок (опционально)
        
        Returns:
            Tuple (buy_vols, sell_vols, deltas)
        """
        volumes_arr = np.array(volumes, dtype=np.float32)
        taker_buy_arr = None
        if taker_buy_volumes:
            taker_buy_arr = np.array(taker_buy_volumes, dtype=np.float32)

        if self.gpu_service and self.gpu_service.is_gpu_available():
            try:
                buy_vols, sell_vols, deltas = self.gpu_service.compute_delta_batch(  # type: ignore
                    volumes_arr, taker_buy_arr
                )
                return buy_vols.tolist(), sell_vols.tolist(), deltas.tolist()
            except Exception as e:
                log.warning(f"⚠️ GPU delta computation failed: {e}")

        # CPU fallback
        if taker_buy_arr is not None:
            buy_vols = np.maximum(taker_buy_arr, 0.0)
            sell_vols = np.maximum(volumes_arr - buy_vols, 0.0)
            deltas = buy_vols - sell_vols
        else:
            buy_vols = volumes_arr * 0.5
            sell_vols = volumes_arr * 0.5
            deltas = np.zeros_like(volumes_arr)

        return buy_vols.tolist(), sell_vols.tolist(), deltas.tolist()

    def compute_z_scores_batch(
        self,
        values: list[float],
        window: int = 300
    ) -> list[float]:
        """
        Вычисляет z-scores для батча значений.
        
        Args:
            values: Список значений
            window: Размер окна
        
        Returns:
            Список z-scores
        """
        values_arr = np.array(values, dtype=np.float32)

        if self.gpu_service and self.gpu_service.is_gpu_available():
            try:
                z_scores = self.gpu_service.compute_z_scores(values_arr, window=window)  # type: ignore
                return z_scores.tolist()
            except Exception as e:
                log.warning(f"⚠️ GPU z-score computation failed: {e}")

        # CPU fallback
        z_scores = np.zeros(len(values), dtype=np.float32)
        for i in range(window - 1, len(values)):
            window_data = values_arr[i - window + 1:i + 1]
            mean = np.mean(window_data)
            std = np.std(window_data)
            if std > 1e-9:
                z_scores[i] = (values_arr[i] - mean) / std

        return z_scores.tolist()


def get_batch_processor(batch_size: int = 1000, use_gpu: bool = True) -> BatchProcessor:
    """
    Получить глобальный экземпляр batch processor.
    
    Args:
        batch_size: Размер батча
        use_gpu: Использовать GPU
    
    Returns:
        BatchProcessor instance
    """
    return BatchProcessor(batch_size=batch_size, use_gpu=use_gpu)


