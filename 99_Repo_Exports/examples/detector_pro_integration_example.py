# -*- coding: utf-8 -*-
"""
Пример интеграции MicrostructureSpikeDetectorPro в существующий хаб.
Показывает параллельное использование legacy и pro детекторов.
"""

from dataclasses import dataclass
from typing import Optional
import logging
import redis

from core.microstructure_spike_detector import MicrostructureSpikeDetector, SpikeConfig
from core.microstructure_spike_detector_pro import MicrostructureSpikeDetectorPro, ProConfig


@dataclass
class CombinedMetrics:
    """Комбинированные метрики от обоих детекторов"""
    z_delta: float
    z_speed: float
    z_range: float
    svbp_imbalance: float
    trigger: bool
    extreme: bool
    dir_up: Optional[bool]
    source: str  # 'legacy' | 'pro' | 'combined'
    trades_count: int


class HybridMicrostructureDetector:
    """
    Гибридный детектор, использующий оба подхода:
    - Legacy (суррогатная дельта) - всегда работает
    - Pro (реальная дельта по принтам) - когда доступны принты
    
    Автоматически выбирает лучший источник данных.
    """
    
    def __init__(
        self,
        legacy_config: Optional[SpikeConfig] = None,
        pro_config: Optional[ProConfig] = None,
        min_trades_for_pro: int = 5
    ):
        self.detector_legacy = MicrostructureSpikeDetector(
            legacy_config or SpikeConfig()
        )
        self.detector_pro = MicrostructureSpikeDetectorPro(
            pro_config or ProConfig()
        )
        self.min_trades_for_pro = min_trades_for_pro
    
    def update_tick(self, bid: float, ask: float, ts_ms: Optional[int] = None) -> None:
        """Обновление по тику - кормим оба детектора"""
        self.detector_legacy.update(bid, ask, volume=1.0, delta_hint=None, ts_ms=ts_ms)
        self.detector_pro.update_tick(bid, ask, ts_ms)
    
    def on_trade(self, price: float, qty: float, side: str, ts_ms: Optional[int] = None) -> None:
        """Обработка принта - только для Pro детектора"""
        self.detector_pro.on_trade(price, qty, side, ts_ms)
    
    def get_metrics(self) -> CombinedMetrics:
        """
        Возвращает комбинированные метрики.
        Приоритет отдаётся Pro-детектору, если достаточно принтов.
        """
        metrics_legacy = self.detector_legacy._last_metrics
        metrics_pro = self.detector_pro.metrics()
        
        trades_count = metrics_pro['trades_in_window']
        
        # Выбор источника данных
        if trades_count >= self.min_trades_for_pro:
            # Достаточно принтов - используем Pro
            return CombinedMetrics(
                z_delta=metrics_pro['z_delta'],
                z_speed=metrics_pro['z_speed'],
                z_range=metrics_pro['z_range'],
                svbp_imbalance=metrics_pro['svbp_imbalance'],
                trigger=metrics_pro['trigger'],
                extreme=metrics_pro['extreme'],
                dir_up=metrics_pro['dir_up'],
                source='pro',
                trades_count=trades_count
            )
        else:
            # Недостаточно принтов - фолбэк на Legacy
            return CombinedMetrics(
                z_delta=metrics_legacy.get('z_delta', 0.0),
                z_speed=metrics_legacy.get('z_speed', 0.0),
                z_range=metrics_legacy.get('z_range', 0.0),
                svbp_imbalance=0.0,  # не доступно в legacy
                trigger=metrics_legacy.get('trigger', False),
                extreme=metrics_legacy.get('extreme', False),
                dir_up=metrics_legacy.get('dir_up'),
                source='legacy',
                trades_count=trades_count
            )
    
    def get_combined_metrics(self, weight_pro: float = 0.7) -> CombinedMetrics:
        """
        Возвращает взвешенную комбинацию метрик от обоих детекторов.
        
        Args:
            weight_pro: вес Pro-детектора (0.0-1.0), legacy = 1 - weight_pro
        """
        metrics_legacy = self.detector_legacy._last_metrics
        metrics_pro = self.detector_pro.metrics()
        
        trades_count = metrics_pro['trades_in_window']
        
        # Динамический вес на основе количества принтов
        if trades_count < self.min_trades_for_pro:
            # Мало принтов - больше веса legacy
            actual_weight_pro = min(0.3, trades_count / self.min_trades_for_pro)
        else:
            actual_weight_pro = weight_pro
        
        weight_legacy = 1.0 - actual_weight_pro
        
        # Взвешенное объединение
        z_delta = (
            metrics_pro['z_delta'] * actual_weight_pro +
            metrics_legacy.get('z_delta', 0.0) * weight_legacy
        )
        z_speed = (
            metrics_pro['z_speed'] * actual_weight_pro +
            metrics_legacy.get('z_speed', 0.0) * weight_legacy
        )
        z_range = (
            metrics_pro['z_range'] * actual_weight_pro +
            metrics_legacy.get('z_range', 0.0) * weight_legacy
        )
        
        # Направление - приоритет Pro при достаточном количестве принтов
        if trades_count >= self.min_trades_for_pro:
            dir_up = metrics_pro['dir_up']
        else:
            dir_up = metrics_legacy.get('dir_up')
        
        return CombinedMetrics(
            z_delta=z_delta,
            z_speed=z_speed,
            z_range=z_range,
            svbp_imbalance=metrics_pro['svbp_imbalance'],
            trigger=metrics_pro['trigger'] or metrics_legacy.get('trigger', False),
            extreme=metrics_pro['extreme'] or metrics_legacy.get('extreme', False),
            dir_up=dir_up,
            source='combined',
            trades_count=trades_count
        )


# ============================================================================
# Пример использования в реальном хабе
# ============================================================================

class ExampleSignalHub:
    """Пример хаба с гибридным детектором"""
    
    def __init__(self, r: redis.Redis, logger: logging.Logger):
        self.r = r
        self.log = logger
        
        # Создаём гибридный детектор
        self.detector = HybridMicrostructureDetector(
            legacy_config=SpikeConfig(
                z_delta_thr=3.0,
                z_extreme_thr=4.5,
                speed_z_thr=3.0
            ),
            pro_config=ProConfig(
                z_delta_thr=3.0,
                z_extreme_thr=4.5,
                speed_z_thr=3.0,
                price_step=0.1,  # XAUUSD point
                lookback_sec=60
            ),
            min_trades_for_pro=5
        )
    
    def on_tick(self, bid: float, ask: float, ts_ms: int) -> None:
        """Обработчик тиков"""
        self.detector.update_tick(bid, ask, ts_ms)
    
    def on_trade(self, price: float, qty: float, side: str, ts_ms: int) -> None:
        """Обработчик принтов из фида"""
        self.detector.on_trade(price, qty, side, ts_ms)
    
    def generate_signal(self) -> Optional[dict]:
        """Генерация сигнала на основе гибридных метрик"""
        
        # Получаем метрики (автоматический выбор лучшего источника)
        metrics = self.detector.get_metrics()
        
        # Или используем взвешенную комбинацию
        # metrics = self.detector.get_combined_metrics(weight_pro=0.7)
        
        self.log.info(
            f"Metrics: z_delta={metrics.z_delta:.2f}, "
            f"z_speed={metrics.z_speed:.2f}, "
            f"svbp_imb={metrics.svbp_imbalance:.2f}, "
            f"source={metrics.source}, trades={metrics.trades_count}"
        )
        
        # Скоринг
        confidence = 0.0
        reason_parts = []
        
        if metrics.trigger:
            confidence += 0.35
            reason_parts.append(f"zΔ={metrics.z_delta:.2f}, zSpeed={metrics.z_speed:.2f}")
        
        if metrics.extreme:
            confidence += 0.15
            reason_parts.append("extreme")
        
        # SVbP доступен только при использовании Pro-детектора
        if abs(metrics.svbp_imbalance) > 0.3:
            confidence += 0.25
            direction = "buy" if metrics.svbp_imbalance > 0 else "sell"
            reason_parts.append(f"SVbP {direction} imb={metrics.svbp_imbalance:.2f}")
        
        # Добавляем бонус за использование реальных принтов
        if metrics.source == 'pro':
            confidence += 0.05
            reason_parts.append(f"real_delta (trades={metrics.trades_count})")
        
        confidence = min(1.0, confidence)
        
        if confidence < 0.6 or metrics.dir_up is None:
            return None
        
        return {
            "side": "LONG" if metrics.dir_up else "SHORT",
            "confidence": confidence,
            "reason": "; ".join(reason_parts),
            "metrics": {
                "z_delta": metrics.z_delta,
                "z_speed": metrics.z_speed,
                "svbp_imbalance": metrics.svbp_imbalance,
                "source": metrics.source,
                "trades_count": metrics.trades_count
            }
        }


# ============================================================================
# Тест
# ============================================================================

if __name__ == "__main__":
    import time
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("test")
    
    # Создаём детектор
    detector = HybridMicrostructureDetector(
        pro_config=ProConfig(price_step=0.1),
        min_trades_for_pro=5
    )
    
    print("=== Тест гибридного детектора ===\n")
    
    # Фаза 1: только тики (должен использовать legacy)
    print("Фаза 1: Только тики")
    for i in range(50):
        bid = 2650.0 + i * 0.01
        ask = bid + 0.2
        detector.update_tick(bid, ask, ts_ms=int(time.time() * 1000))
    
    metrics = detector.get_metrics()
    print(f"  Source: {metrics.source}")
    print(f"  Z-delta: {metrics.z_delta:.2f}")
    print(f"  Trades: {metrics.trades_count}\n")
    
    # Фаза 2: добавляем принты (должен переключиться на pro)
    print("Фаза 2: Тики + принты")
    for i in range(50, 100):
        bid = 2650.0 + i * 0.01
        ask = bid + 0.2
        detector.update_tick(bid, ask, ts_ms=int(time.time() * 1000))
        
        # Добавляем принты
        if i % 3 == 0:
            detector.on_trade(
                price=bid + 0.1,
                qty=1.5,
                side='buy' if i % 2 == 0 else 'sell',
                ts_ms=int(time.time() * 1000)
            )
    
    metrics = detector.get_metrics()
    print(f"  Source: {metrics.source}")
    print(f"  Z-delta: {metrics.z_delta:.2f}")
    print(f"  SVbP imbalance: {metrics.svbp_imbalance:.2f}")
    print(f"  Trades: {metrics.trades_count}\n")
    
    # Фаза 3: взвешенная комбинация
    print("Фаза 3: Взвешенная комбинация")
    metrics_combined = detector.get_combined_metrics(weight_pro=0.7)
    print(f"  Source: {metrics_combined.source}")
    print(f"  Z-delta: {metrics_combined.z_delta:.2f}")
    print(f"  Z-speed: {metrics_combined.z_speed:.2f}")
    print(f"  Trigger: {metrics_combined.trigger}")
    print(f"  Direction: {'UP' if metrics_combined.dir_up else 'DOWN' if metrics_combined.dir_up is False else 'NEUTRAL'}")
    
    print("\n✅ Тест завершён успешно!")

