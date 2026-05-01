#!/usr/bin/env python3
"""
Пример использования Signal Family Baseline Job.

Запуск:
    cd python-worker
    python -m regime.example_usage
"""

import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from regime.baseline_job import SignalFamilyBaselineJob
from regime.models import SignalExecRow
from regime.baseline_utils import compute_family_baseline


def example_baseline_calculation():
    """Пример расчета baseline для тестовых данных."""
    print("=== Пример расчета baseline ===\n")

    # Пример данных (в реальности читаются из БД)
    sample_signals = [
        SignalExecRow(signal_id=1, symbol="BTCUSDT", family="volatility_spike",
                      opened_at=datetime(2024, 1, 1), result_r=0.5),
        SignalExecRow(signal_id=2, symbol="BTCUSDT", family="volatility_spike",
                      opened_at=datetime(2024, 1, 2), result_r=-0.3),
        SignalExecRow(signal_id=3, symbol="BTCUSDT", family="volatility_spike",
                      opened_at=datetime(2024, 1, 3), result_r=0.8),
        SignalExecRow(signal_id=4, symbol="BTCUSDT", family="volatility_spike",
                      opened_at=datetime(2024, 1, 4), result_r=0.2),
        SignalExecRow(signal_id=5, symbol="BTCUSDT", family="volatility_spike",
                      opened_at=datetime(2024, 1, 5), result_r=-0.1),
        SignalExecRow(signal_id=6, symbol="BTCUSDT", family="volatility_spike",
                      opened_at=datetime(2024, 1, 6), result_r=0.6),
        SignalExecRow(signal_id=7, symbol="BTCUSDT", family="volatility_spike",
                      opened_at=datetime(2024, 1, 7), result_r=0.4),
    ]

    print(f"Сигналы: {len(sample_signals)}")
    for s in sample_signals:
        print(f"  {s.opened_at.date()}: {s.result_r:+.2f}R")

    # Расчет baseline
    window_size = 3
    baselines = compute_family_baseline(sample_signals, window_size)

    print(f"\nBaseline (окно {window_size} сигналов):")
    for metric_name, quantiles in baselines.items():
        if quantiles.sample_size == 0:
            print(f"  {metric_name}: недостаточно данных")
            continue

        print(f"  {metric_name}:")
        print(f"    samples: {quantiles.sample_size}")
        print(f"    p10: {quantiles.p10:.3f}")
        print(f"    p50: {quantiles.p50:.3f}")
        print(f"    p90: {quantiles.p90:.3f}")


def example_job_run():
    """Пример запуска джоба."""
    print("\n=== Пример запуска джоба ===\n")

    # Настройки
    dsn = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/db")

    print(f"DSN: {dsn}")
    print("Запуск baseline джоба...")

    try:
        job = SignalFamilyBaselineJob(
            dsn=dsn,
            window_size=50,
            horizon_days=180,
        )
        job.run()
        print("✅ Джоб выполнен успешно")
    except Exception as e:
        print(f"❌ Ошибка выполнения джоба: {e}")


if __name__ == "__main__":
    example_baseline_calculation()
    # example_job_run()  # Раскомментировать для запуска на реальной БД
