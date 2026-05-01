from __future__ import annotations
"""
Диагностика латентности ML из metrics:ml_confirm stream.

Анализирует последние N минут метрик и показывает:
- Перцентили латентности (p50/p95/p99)
- Распределение по символам/сценариям
- Сравнение латентности с/без ошибок
- Временные паттерны
"""

from utils.time_utils import get_ny_time_millis

import argparse
import os
import time
from collections import Counter, defaultdict
from typing import Any, Dict, List

import redis


def now_ms() -> int:
    return get_ny_time_millis()


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def pctl(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def read_stream_window(
    r: redis.Redis,
    stream: str,
    start_ms: int,
    window_ms: int,
    *,
    max_scan: int = 200000
) -> List[Dict[str, Any]]:
    """Read stream items in [start_ms, start_ms+window_ms] by ts_ms field."""
    end_ms = start_ms + window_ms
    rows: List[Dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            d = dict(fields or {})
            ts = _i(d.get("ts_ms", d.get("ts", d.get("timestamp", 0))), 0)
            if ts <= 0:
                continue
            if ts < start_ms:
                scanned = max_scan
                break
            if ts <= end_ms:
                d["_ts_ms"] = ts
                rows.append(d)
        if len(batch) < 2000:
            break
    rows.sort(key=lambda x: int(x.get("_ts_ms", 0)))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Диагностика латентности ML")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm"))
    ap.add_argument("--window-min", type=int, default=60, help="Окно анализа в минутах")
    ap.add_argument("--max-scan", type=int, default=200000, help="Максимум сообщений для сканирования")
    ap.add_argument("--top-n", type=int, default=20, help="Топ N символов для показа")
    ap.add_argument("--threshold-ms", type=float, default=6.0, help="Порог латентности (мс) для алертов")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    window_ms = args.window_min * 60_000
    start_ms = now_ms() - window_ms
    rows = read_stream_window(r, args.stream, start_ms, window_ms, max_scan=args.max_scan)

    if not rows:
        print(f"❌ Нет данных в stream {args.stream} за последние {args.window_min} минут")
        return

    n_total = len(rows)
    lat_all: List[float] = []
    lat_by_symbol: Dict[str, List[float]] = defaultdict(list)
    lat_by_mode: Dict[str, List[float]] = defaultdict(list)
    lat_with_err: List[float] = []
    lat_without_err: List[float] = []

    for r in rows:
        # latency: prefer latency_ms; fallback latency_us
        lat_ms = 0.0
        if str(r.get("latency_ms", "") or "").strip() != "":
            lat_ms = _f(r.get("latency_ms", 0.0), 0.0)
        else:
            lat_us = _f(r.get("latency_us", 0.0), 0.0)
            lat_ms = lat_us / 1000.0 if lat_us > 0 else 0.0

        if lat_ms > 0:
            lat_all.append(lat_ms)
            sym = str(r.get("symbol", "") or "unknown").upper()
            mode = str(r.get("mode", "") or "unknown").upper()
            lat_by_symbol[sym].append(lat_ms)
            lat_by_mode[mode].append(lat_ms)

            err = str(r.get("error", "") or "").strip()
            if err:
                lat_with_err.append(lat_ms)
            else:
                lat_without_err.append(lat_ms)

    if not lat_all:
        print(f"❌ Нет данных о латентности в stream {args.stream}")
        return

    print(f"\n{'='*80}")
    print(f"ML LATENCY DIAGNOSTICS (последние {args.window_min} минут)")
    print(f"{'='*80}")
    print(f"Всего записей: {n_total}")
    print(f"С латентностью: {len(lat_all)} ({100.0*len(lat_all)/max(1,n_total):.2f}%)")

    # Общая статистика
    lat_all.sort()
    n_lat = len(lat_all)
    p50 = pctl(lat_all, 0.50)
    p90 = pctl(lat_all, 0.90)
    p95 = pctl(lat_all, 0.95)
    p99 = pctl(lat_all, 0.99)
    avg = sum(lat_all) / n_lat
    max_lat = max(lat_all)

    print(f"\n{'─'*80}")
    print(f"ОБЩАЯ СТАТИСТИКА ЛАТЕНТНОСТИ (мс):")
    print(f"{'─'*80}")
    print(f"  Средняя: {avg:.3f} мс")
    print(f"  p50:     {p50:.3f} мс")
    print(f"  p90:     {p90:.3f} мс")
    print(f"  p95:     {p95:.3f} мс")
    print(f"  p99:     {p99:.3f} мс {'⚠️  ПРЕВЫШЕН ПОРОГ!' if p99 > args.threshold_ms else '✅'}")
    print(f"  Макс:    {max_lat:.3f} мс")
    print(f"  Порог:   {args.threshold_ms:.3f} мс")

    # Сравнение с/без ошибок
    if lat_with_err and lat_without_err:
        lat_with_err.sort()
        lat_without_err.sort()
        err_p99 = pctl(lat_with_err, 0.99)
        ok_p99 = pctl(lat_without_err, 0.99)
        err_avg = sum(lat_with_err) / len(lat_with_err)
        ok_avg = sum(lat_without_err) / len(lat_without_err)

        print(f"\n{'─'*80}")
        print(f"СРАВНЕНИЕ: С ОШИБКАМИ vs БЕЗ ОШИБОК:")
        print(f"{'─'*80}")
        print(f"  С ошибками (n={len(lat_with_err)}):")
        print(f"    Средняя: {err_avg:.3f} мс")
        print(f"    p99:     {err_p99:.3f} мс")
        print(f"  Без ошибок (n={len(lat_without_err)}):")
        print(f"    Средняя: {ok_avg:.3f} мс")
        print(f"    p99:     {ok_p99:.3f} мс")
        print(f"  Разница p99: {err_p99 - ok_p99:+.3f} мс")

    # По режимам
    if lat_by_mode:
        print(f"\n{'─'*80}")
        print(f"ЛАТЕНТНОСТЬ ПО РЕЖИМАМ:")
        print(f"{'─'*80}")
        mode_stats = []
        for mode, lats in lat_by_mode.items():
            if lats:
                lats.sort()
                mode_stats.append((mode, len(lats), pctl(lats, 0.50), pctl(lats, 0.99), sum(lats)/len(lats)))
        mode_stats.sort(key=lambda x: x[3], reverse=True)  # sort by p99
        for mode, n, p50_m, p99_m, avg_m in mode_stats:
            print(f"  {mode:12s}: n={n:5d} | avg={avg_m:6.2f} мс | p50={p50_m:6.2f} мс | p99={p99_m:6.2f} мс")

    # По символам (топ медленных)
    if lat_by_symbol:
        print(f"\n{'─'*80}")
        print(f"ЛАТЕНТНОСТЬ ПО СИМВОЛАМ (топ {args.top_n} по p99):")
        print(f"{'─'*80}")
        sym_stats = []
        for sym, lats in lat_by_symbol.items():
            if lats:
                lats.sort()
                sym_stats.append((sym, len(lats), pctl(lats, 0.50), pctl(lats, 0.99), sum(lats)/len(lats)))
        sym_stats.sort(key=lambda x: x[3], reverse=True)  # sort by p99
        for sym, n, p50_s, p99_s, avg_s in sym_stats[:args.top_n]:
            warn = "⚠️" if p99_s > args.threshold_ms else "  "
            print(f"  {warn} {sym:12s}: n={n:5d} | avg={avg_s:6.2f} мс | p50={p50_s:6.2f} мс | p99={p99_s:6.2f} мс")

    # Временное распределение (по 5-минутным бакетам)
    buckets: Dict[int, List[float]] = defaultdict(list)
    for r in rows:
        ts_ms = _i(r.get("ts_ms", 0), 0)
        if ts_ms > 0:
            bucket_min = (ts_ms - start_ms) // (5 * 60_000)  # 5-minute buckets
            lat_ms = 0.0
            if str(r.get("latency_ms", "") or "").strip() != "":
                lat_ms = _f(r.get("latency_ms", 0.0), 0.0)
            else:
                lat_us = _f(r.get("latency_us", 0.0), 0.0)
                lat_ms = lat_us / 1000.0 if lat_us > 0 else 0.0
            if lat_ms > 0:
                buckets[bucket_min].append(lat_ms)

    if buckets:
        print(f"\n{'─'*80}")
        print(f"ВРЕМЕННОЕ РАСПРЕДЕЛЕНИЕ ЛАТЕНТНОСТИ (5-минутные бакеты, p99):")
        print(f"{'─'*80}")
        bucket_stats = []
        for bucket_min, lats in buckets.items():
            if lats:
                lats.sort()
                time_start = start_ms + bucket_min * 5 * 60_000
                ts_start = time.strftime("%H:%M", time.localtime(time_start / 1000))
                bucket_stats.append((bucket_min, ts_start, len(lats), pctl(lats, 0.99)))
        bucket_stats.sort(key=lambda x: x[0])
        for _, ts_start, n, p99_b in bucket_stats:
            warn = "⚠️" if p99_b > args.threshold_ms else "  "
            bar_len = min(50, int(p99_b * 10))
            bar = "█" * bar_len
            print(f"  {warn} {ts_start}: n={n:4d} | p99={p99_b:6.2f} мс {bar}")

    # Алерты
    alerts = []
    if p99 > args.threshold_ms:
        alerts.append(f"⚠️  p99 латентность {p99:.2f} мс превышает порог {args.threshold_ms:.2f} мс")
    if max_lat > args.threshold_ms * 10:
        alerts.append(f"⚠️  Максимальная латентность {max_lat:.2f} мс очень высокая")

    if alerts:
        print(f"\n{'─'*80}")
        print(f"АЛЕРТЫ:")
        print(f"{'─'*80}")
        for alert in alerts:
            print(f"  {alert}")

    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    main()

