# -*- coding: utf-8 -*-
"""
Тесты для orderbook_l2_metrics.py
"""
import pytest
from signals.orderbook_l2_metrics import (
    compute_l2_metrics,
    L2Metrics,
)

def test_compute_l2_metrics_basic():
    """Тест базового расчёта L2-метрик"""
    book = {
        "ts": 1732881234567,
        "bids": [
            [100.0, 1.0],
            [99.5, 2.0],
            [99.0, 1.5],
            [98.5, 1.0],
            [98.0, 0.5],
        ],
        "asks": [
            [100.5, 0.8],
            [101.0, 1.2],
            [101.5, 1.0],
            [102.0, 0.9],
            [102.5, 0.6],
        ],
    }
    
    metrics = compute_l2_metrics(book, k_small=5, k_large=5)
    
    assert metrics is not None
    assert metrics.ts == 1732881234567
    assert metrics.best_bid == 100.0
    assert metrics.best_ask == 100.5
    assert metrics.mid == 100.25
    
    # Spread = 0.5 / 100.25 * 10000 ≈ 49.88 bps
    assert abs(metrics.spread_bps - 49.875) < 0.1
    
    # Depth (k_small=5, так что берём первые 5 уровней)
    assert metrics.depth_bid_5 == 6.0  # 1+2+1.5+1+0.5
    assert metrics.depth_ask_5 == 4.5  # 0.8+1.2+1+0.9+0.6
    
    # OBI_5 = (6 - 4.5) / (6 + 4.5) = 1.5 / 10.5 ≈ 0.1428
    assert abs(metrics.obi_5 - 0.1428) < 0.01

def test_compute_l2_metrics_empty():
    """Тест обработки пустого book"""
    assert compute_l2_metrics({}) is None
    assert compute_l2_metrics(None) is None
    assert compute_l2_metrics({"bids": [], "asks": []}) is None

def test_compute_l2_metrics_wall_detection():
    """Тест детекции wall"""
    book = {
        "bids": [
            [100.0, 1.0],
            [99.9, 10.0],  # Wall: 10x больше медианы
            [99.8, 1.0],
        ],
        "asks": [
            [100.1, 1.0],
            [100.2, 1.0],
            [100.3, 1.0],
        ],
    }
    
    metrics = compute_l2_metrics(book, k_small=3, wall_mult=3.0, wall_max_dist_bps=50.0)
    
    assert metrics is not None
    assert metrics.wall_bid is True  # Wall detected
    assert metrics.wall_ask is False  # No wall

def test_compute_l2_metrics_microprice():
    """Тест расчёта microprice"""
    book = {
        "bids": [[100.0, 10.0]],  # Большой объём на bid
        "asks": [[100.5, 1.0]],   # Малый объём на ask
    }
    
    metrics = compute_l2_metrics(book, k_small=1, k_large=1)
    
    assert metrics is not None
    # Microprice должна быть ближе к bid (больший вес)
    assert metrics.microprice_20 < metrics.mid
    assert metrics.microprice_shift_bps_20 < 0  # Negative shift

def test_compute_l2_metrics_mixed_types():
    """Тест обработки смешанных типов (строки/числа) как в Binance"""
    book = {
        "bids": [
            ["100.0", "1.0"],
            [99.0, 2.0]
        ],
        "asks": [
            ["101.0", "1.0"],
            [102.0, 2.0]
        ]
    }
    metrics = compute_l2_metrics(book, k_small=2, k_large=2)
    assert metrics is not None
    assert metrics.best_bid == 100.0
    assert metrics.depth_bid_5 == 3.0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

