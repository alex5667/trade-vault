from __future__ import annotations

"""
Пересчет p99 latency для metrics:of_gate (build vs ml).

Анализирует последние N событий из metrics:of_gate и вычисляет:
- p50/p95/p99 для build latency (latency_us)
- p50/p95/p99 для ML latency (ml_latency_us)

Использование:
    python3 -m tools.analyze_of_gate_latency_p99 --count 2000
    python3 -m tools.analyze_of_gate_latency_p99 --count 5000 --redis-url redis://redis-worker-1:6379/0
"""


import argparse
import os
from typing import Any

import numpy as np
import redis


def _f(x: Any, d: float = 0.0) -> float:
    """Безопасное преобразование в float."""
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    """Безопасное преобразование в int."""
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


def pctl(a: list[float], q: float) -> float:
    """Вычисление перцентиля."""
    if not a:
        return 0.0
    a_arr = np.array(a)
    return float(np.percentile(a_arr, q))


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Пересчет p99 latency для metrics:of_gate (build vs ml)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  # Анализ последних 2000 событий
  python3 -m tools.analyze_of_gate_latency_p99 --count 2000

  # Анализ последних 5000 событий с кастомным Redis
  python3 -m tools.analyze_of_gate_latency_p99 --count 5000 --redis-url redis://localhost:6379/0

Вывод:
  - p50/p95/p99 для build latency (latency_us)
  - p50/p95/p99 для ML latency (ml_latency_us)
        """
    )
    ap.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        help="Redis URL (default: REDIS_URL env or redis://redis-worker-1:6379/0)",
    )
    ap.add_argument(
        "--stream",
        default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"),
        help="Metrics stream name (default: OF_GATE_METRICS_STREAM env or metrics:of_gate)",
    )
    ap.add_argument(
        "--count",
        type=int,
        default=2000,
        help="Количество последних событий для анализа (default: 2000)",
    )
    args = ap.parse_args()

    # Подключение к Redis
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    # Чтение последних событий
    items = r.xrevrange(args.stream, count=args.count)

    if not items:
        print(f"❌ Нет событий в stream {args.stream}")
        return

    print(f"\n{'='*80}")
    print(f"АНАЛИЗ ЛАТЕНТНОСТИ ИЗ {args.stream}")
    print(f"{'='*80}\n")
    print(f"Всего событий: {len(items)}\n")

    # Сбор данных о латентности
    lat_build: list[float] = []
    lat_ml: list[float] = []

    for msg_id, fields in items:
        # Build latency (latency_us)
        try:
            lat_build_val = _f(fields.get("latency_us", 0) or 0, 0.0)
            if lat_build_val > 0:
                lat_build.append(lat_build_val)
        except Exception:
            pass

        # ML latency (ml_latency_us)
        try:
            lat_ml_val = _f(fields.get("ml_latency_us", 0) or 0, 0.0)
            if lat_ml_val > 0:
                lat_ml.append(lat_ml_val)
        except Exception:
            pass

    # Вычисление перцентилей для build latency
    print(f"{'─'*80}")
    print("BUILD LATENCY (latency_us)")
    print(f"{'─'*80}")
    if lat_build:
        n_build = len(lat_build)
        p50_build = pctl(lat_build, 50)
        p95_build = pctl(lat_build, 95)
        p99_build = pctl(lat_build, 99)
        avg_build = np.mean(lat_build)
        max_build = np.max(lat_build)

        print(f"  Количество записей: {n_build}")
        print(f"  Средняя:           {avg_build:.2f} мкс")
        print(f"  p50:               {p50_build:.2f} мкс ({p50_build/1000:.2f} мс)")
        print(f"  p95:               {p95_build:.2f} мкс ({p95_build/1000:.2f} мс)")
        print(f"  p99:               {p99_build:.2f} мкс ({p99_build/1000:.2f} мс)")
        print(f"  Максимум:          {max_build:.2f} мкс ({max_build/1000:.2f} мс)")
    else:
        print("  ⚠️  Нет данных о build latency")

    print()

    # Вычисление перцентилей для ML latency
    print(f"{'─'*80}")
    print("ML LATENCY (ml_latency_us)")
    print(f"{'─'*80}")
    if lat_ml:
        n_ml = len(lat_ml)
        p50_ml = pctl(lat_ml, 50)
        p95_ml = pctl(lat_ml, 95)
        p99_ml = pctl(lat_ml, 99)
        avg_ml = np.mean(lat_ml)
        max_ml = np.max(lat_ml)

        print(f"  Количество записей: {n_ml}")
        print(f"  Средняя:           {avg_ml:.2f} мкс")
        print(f"  p50:               {p50_ml:.2f} мкс ({p50_ml/1000:.2f} мс)")
        print(f"  p95:               {p95_ml:.2f} мкс ({p95_ml/1000:.2f} мс)")
        print(f"  p99:               {p99_ml:.2f} мкс ({p99_ml/1000:.2f} мс)")
        print(f"  Максимум:          {max_ml:.2f} мкс ({max_ml/1000:.2f} мс)")
    else:
        print("  ⚠️  Нет данных о ML latency")

    print()

    # Сравнение и вывод для понимания EMERGENCY
    if lat_build and lat_ml:
        print(f"{'─'*80}")
        print("СРАВНЕНИЕ (для понимания EMERGENCY)")
        print(f"{'─'*80}")
        p99_build = pctl(lat_build, 99)
        p99_ml = pctl(lat_ml, 99)

        print(f"  Build p99: {p99_build:.2f} мкс ({p99_build/1000:.2f} мс)")
        print(f"  ML p99:    {p99_ml:.2f} мкс ({p99_ml/1000:.2f} мс)")

        if p99_ml > p99_build:
            diff = p99_ml - p99_build
            print(f"  ⚠️  ML latency выше на {diff:.2f} мкс ({diff/1000:.2f} мс)")
            print("     ML может быть причиной EMERGENCY")
        elif p99_build > p99_ml:
            diff = p99_build - p99_ml
            print(f"  ⚠️  Build latency выше на {diff:.2f} мкс ({diff/1000:.2f} мс)")
            print("     Build может быть причиной EMERGENCY")
        else:
            print("  ✅ Build и ML latency примерно равны")

    print()
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()

