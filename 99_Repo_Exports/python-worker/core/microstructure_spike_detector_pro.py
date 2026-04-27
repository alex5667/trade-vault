# -*- coding: utf-8 -*-
"""
MicrostructureSpikeDetectorPro - True bid/ask delta detector using real trade prints.

Отличия от базового детектора:
- Использует реальные принты/сделки (price, qty, side='buy'|'sell')
- Считает истинную дельту по агрессорам
- Z-score по отдельным окнам для bid и ask потоков
- SVBP (Stacked Volume by Price) analysis
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from collections import deque
import time


@dataclass
class ProConfig:
    """Конфигурация Pro детектора."""
    window_seconds: float = 30.0
    z_delta_thr: float = 3.0
    z_extreme_thr: float = 4.5
    z_speed_thr: float = 2.5
    svbp_bins: int = 20
    min_trades: int = 10


class MicrostructureSpikeDetectorPro:
    """
    Pro версия детектора с true bid/ask delta.
    
    Фидится реальными принтами через on_trade(price, qty, side, ts_ms).
    """
    
    def __init__(self, cfg: ProConfig):
        self.cfg = cfg
        
        # Trade history: (ts_ms, price, qty, side)
        self.trades: deque = deque(maxlen=10000)
        
        # Tick snapshots: (ts_ms, bid, ask)
        self.ticks: deque = deque(maxlen=1000)
        
        # Aggregated volumes by side
        self.buy_vol = 0.0
        self.sell_vol = 0.0
        
        # Last update timestamp
        self.last_ts = 0
        
        # Metrics cache
        self._metrics_cache = {}
    
    def on_trade(
        self,
        price: float,
        qty: float,
        side: str,  # 'buy' | 'sell' (агрессор)
        ts_ms: Optional[int] = None
    ) -> None:
        """
        Feed реальный принт/сделку.
        
        Args:
            price: Цена сделки
            qty: Объём
            side: 'buy' (покупатель агрессор) | 'sell' (продавец агрессор)
            ts_ms: Timestamp в миллисекундах (optional)
        """
        ts = ts_ms or get_ny_time_millis()
        self.trades.append((ts, price, qty, side.lower()))
        self.last_ts = ts
        
        # Update aggregated volumes
        if side.lower() == 'buy':
            self.buy_vol += qty
        else:
            self.sell_vol += qty
        
        # Invalidate cache
        self._metrics_cache = {}
    
    def update_tick(self, bid: float, ask: float, ts_ms: Optional[int] = None) -> None:
        """
        Обновить снапшот bid/ask (опционально, для context).
        
        Args:
            bid: Лучший бид
            ask: Лучший аск
            ts_ms: Timestamp в миллисекундах (optional)
        """
        ts = ts_ms or get_ny_time_millis()
        self.ticks.append((ts, bid, ask))
        self.last_ts = ts
    
    def metrics(self) -> Dict[str, any]:
        """
        Рассчитать метрики на текущем окне.
        
        Returns:
            {
                'z_delta': float,  # Z-score дельты buy-sell
                'z_speed': float,  # Z-score скорости принтов
                'z_range': float,  # Z-score диапазона цен
                'delta': float,    # Raw delta (buy_vol - sell_vol)
                'buy_vol': float,
                'sell_vol': float,
                'trades_count': int,
                'svbp_imbalance': float,  # Stacked Volume by Price imbalance
                'svbp_top': dict,  # Top price levels
                'dir_up': Optional[bool],  # True=LONG, False=SHORT, None=neutral
                'trigger': bool,  # Триггер на основе z_delta
                'extreme': bool,  # Экстремальный spike
            }
        """
        if self._metrics_cache:
            return self._metrics_cache
        
        # Get trades in window
        window_trades = self._get_window_trades()
        
        if len(window_trades) < self.cfg.min_trades:
            return self._empty_metrics()
        
        # Calculate true delta
        buy_vol = sum(qty for _, _, qty, side in window_trades if side == 'buy')
        sell_vol = sum(qty for _, _, qty, side in window_trades if side == 'sell')
        delta = buy_vol - sell_vol
        
        # Z-scores
        z_delta = self._z_score_delta(window_trades)
        z_speed = self._z_score_speed(window_trades)
        z_range = self._z_score_range(window_trades)
        
        # SVBP analysis
        svbp_imb, svbp_top = self._analyze_svbp(window_trades)
        
        # Direction
        dir_up = None
        if abs(z_delta) >= self.cfg.z_delta_thr:
            dir_up = z_delta > 0
        
        # Triggers
        trigger = abs(z_delta) >= self.cfg.z_delta_thr
        extreme = abs(z_delta) >= self.cfg.z_extreme_thr
        
        metrics = {
            'z_delta': round(z_delta, 3),
            'z_speed': round(z_speed, 3),
            'z_range': round(z_range, 3),
            'delta': round(delta, 2),
            'buy_vol': round(buy_vol, 2),
            'sell_vol': round(sell_vol, 2),
            'trades_count': len(window_trades),
            'svbp_imbalance': round(svbp_imb, 3),
            'svbp_top': svbp_top,
            'dir_up': dir_up,
            'trigger': trigger,
            'extreme': extreme,
        }
        
        self._metrics_cache = metrics
        return metrics
    
    # ========== INTERNALS ==========
    
    def _get_window_trades(self) -> List[Tuple]:
        """Get trades within window."""
        if not self.trades or not self.last_ts:
            return []
        
        cutoff = self.last_ts - (self.cfg.window_seconds * 1000)
        return [t for t in self.trades if t[0] >= cutoff]
    
    def _z_score_delta(self, trades: List[Tuple]) -> float:
        """Calculate z-score of buy-sell delta."""
        if len(trades) < 3:
            return 0.0
        
        # Rolling deltas
        deltas = []
        chunk_size = max(1, len(trades) // 5)
        
        for i in range(0, len(trades), chunk_size):
            chunk = trades[i:i + chunk_size]
            buy = sum(qty for _, _, qty, side in chunk if side == 'buy')
            sell = sum(qty for _, _, qty, side in chunk if side == 'sell')
            deltas.append(buy - sell)
        
        if len(deltas) < 2:
            return 0.0
        
        # Z-score последнего чанка
        mean = sum(deltas) / len(deltas)
        std = (sum((d - mean) ** 2 for d in deltas) / len(deltas)) ** 0.5
        
        if std < 0.001:
            return 0.0
        
        return (deltas[-1] - mean) / std
    
    def _z_score_speed(self, trades: List[Tuple]) -> float:
        """Calculate z-score of trades per second."""
        if len(trades) < 5:
            return 0.0
        
        # Split into chunks and count trades/sec
        chunk_size = max(1, len(trades) // 5)
        speeds = []
        
        for i in range(0, len(trades), chunk_size):
            chunk = trades[i:i + chunk_size]
            if len(chunk) < 2:
                continue
            dt_sec = (chunk[-1][0] - chunk[0][0]) / 1000.0
            if dt_sec > 0:
                speeds.append(len(chunk) / dt_sec)
        
        if len(speeds) < 2:
            return 0.0
        
        mean = sum(speeds) / len(speeds)
        std = (sum((s - mean) ** 2 for s in speeds) / len(speeds)) ** 0.5
        
        if std < 0.001:
            return 0.0
        
        return (speeds[-1] - mean) / std
    
    def _z_score_range(self, trades: List[Tuple]) -> float:
        """Calculate z-score of price range."""
        if len(trades) < 5:
            return 0.0
        
        # Split into chunks and calculate ranges
        chunk_size = max(1, len(trades) // 5)
        ranges = []
        
        for i in range(0, len(trades), chunk_size):
            chunk = trades[i:i + chunk_size]
            if len(chunk) < 2:
                continue
            prices = [p for _, p, _, _ in chunk]
            ranges.append(max(prices) - min(prices))
        
        if len(ranges) < 2:
            return 0.0
        
        mean = sum(ranges) / len(ranges)
        std = (sum((r - mean) ** 2 for r in ranges) / len(ranges)) ** 0.5
        
        if std < 0.001:
            return 0.0
        
        return (ranges[-1] - mean) / std
    
    def _analyze_svbp(self, trades: List[Tuple]) -> Tuple[float, Dict]:
        """
        Stacked Volume by Price analysis.
        
        Returns:
            (imbalance: -1..+1, top_levels: dict)
        """
        if len(trades) < self.cfg.min_trades:
            return 0.0, {}
        
        # Get price range
        prices = [p for _, p, _, _ in trades]
        min_p, max_p = min(prices), max(prices)
        
        if max_p - min_p < 0.01:
            return 0.0, {}
        
        # Bin prices
        bin_size = (max_p - min_p) / self.cfg.svbp_bins
        bins = {}
        
        for _, price, qty, side in trades:
            bin_idx = int((price - min_p) / bin_size)
            bin_idx = min(bin_idx, self.cfg.svbp_bins - 1)
            
            if bin_idx not in bins:
                bins[bin_idx] = {'buy': 0.0, 'sell': 0.0}
            
            if side == 'buy':
                bins[bin_idx]['buy'] += qty
            else:
                bins[bin_idx]['sell'] += qty
        
        # Calculate imbalance
        total_buy = sum(b['buy'] for b in bins.values())
        total_sell = sum(b['sell'] for b in bins.values())
        
        if total_buy + total_sell < 0.01:
            return 0.0, {}
        
        imbalance = (total_buy - total_sell) / (total_buy + total_sell)
        
        # Top 3 levels by volume
        sorted_bins = sorted(
            bins.items(),
            key=lambda x: x[1]['buy'] + x[1]['sell'],
            reverse=True
        )[:3]
        
        top_levels = {
            f"level_{i}": {
                'price_bin': bin_idx,
                'buy': round(data['buy'], 2),
                'sell': round(data['sell'], 2),
            }
            for i, (bin_idx, data) in enumerate(sorted_bins)
        }
        
        return imbalance, top_levels
    
    def _empty_metrics(self) -> Dict:
        """Return empty metrics when insufficient data."""
        return {
            'z_delta': 0.0,
            'z_speed': 0.0,
            'z_range': 0.0,
            'delta': 0.0,
            'buy_vol': 0.0,
            'sell_vol': 0.0,
            'trades_count': 0,
            'svbp_imbalance': 0.0,
            'svbp_top': {},
            'dir_up': None,
            'trigger': False,
            'extreme': False,
        }
