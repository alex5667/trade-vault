"""
Диагностика ошибок ML из metrics:ml_confirm stream.

Анализирует последние N минут метрик и показывает:
- Топ ошибок с частотами
- Распределение по символам/сценариям
- Временные паттерны
- Детали последних ошибок
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
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
    ap = argparse.ArgumentParser(description="Диагностика ошибок ML")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm"))
    ap.add_argument("--window-min", type=int, default=60, help="Окно анализа в минутах")
    ap.add_argument("--max-scan", type=int, default=200000, help="Максимум сообщений для сканирования")
    ap.add_argument("--top-n", type=int, default=20, help="Топ N ошибок/символов для показа")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    window_ms = args.window_min * 60_000
    start_ms = now_ms() - window_ms
    rows = read_stream_window(r, args.stream, start_ms, window_ms, max_scan=args.max_scan)

    if not rows:
        print(f"❌ Нет данных в stream {args.stream} за последние {args.window_min} минут")
        return

    n_total = len(rows)
    err_rows = [r for r in rows if (str(r.get("error", "") or "").strip() != "")]
    n_err = len(err_rows)

    print(f"\n{'='*80}")
    print(f"ML ERROR DIAGNOSTICS (последние {args.window_min} минут)")
    print(f"{'='*80}")
    print(f"Всего записей: {n_total}")
    print(f"С ошибками: {n_err} ({100.0*n_err/max(1,n_total):.2f}%)")
    print(f"Без ошибок: {n_total - n_err} ({100.0*(n_total-n_err)/max(1,n_total):.2f}%)")

    if n_err == 0:
        print("\n✅ Ошибок не обнаружено!")
        return

    # Топ ошибок
    err_counts: Counter[str] = Counter()
    for r in err_rows:
        err = str(r.get("error", "") or "").strip()
        if err:
            err_counts[err] += 1

    print(f"\n{'─'*80}")
    print(f"ТОП {args.top_n} ОШИБОК:")
    print(f"{'─'*80}")
    for i, (err, count) in enumerate(err_counts.most_common(args.top_n), 1):
        pct = 100.0 * count / max(1, n_err)
        print(f"{i:2d}. [{count:5d} ({pct:5.2f}%)] {err[:120]}")

    # Ошибки по символам
    err_by_symbol: Counter[str] = Counter()
    for r in err_rows:
        sym = str(r.get("symbol", "") or "unknown").upper()
        err_by_symbol[sym] += 1

    print(f"\n{'─'*80}")
    print(f"ОШИБКИ ПО СИМВОЛАМ (топ {args.top_n}):")
    print(f"{'─'*80}")
    for i, (sym, count) in enumerate(err_by_symbol.most_common(args.top_n), 1):
        pct = 100.0 * count / max(1, n_err)
        print(f"{i:2d}. {sym:12s}: {count:5d} ({pct:5.2f}%)")

    # Ошибки по режимам
    err_by_mode: Counter[str] = Counter()
    for r in err_rows:
        mode = str(r.get("mode", "") or "unknown").upper()
        err_by_mode[mode] += 1

    print(f"\n{'─'*80}")
    print(f"ОШИБКИ ПО РЕЖИМАМ:")
    print(f"{'─'*80}")
    for mode, count in err_by_mode.most_common():
        pct = 100.0 * count / max(1, n_err)
        print(f"  {mode:12s}: {count:5d} ({pct:5.2f}%)")

    # Ошибки по fail_policy
    err_by_policy: Counter[str] = Counter()
    for r in err_rows:
        policy = str(r.get("fail_policy", "") or "unknown").upper()
        err_by_policy[policy] += 1

    print(f"\n{'─'*80}")
    print(f"ОШИБКИ ПО FAIL POLICY:")
    print(f"{'─'*80}")
    for policy, count in err_by_policy.most_common():
        pct = 100.0 * count / max(1, n_err)
        print(f"  {policy:12s}: {count:5d} ({pct:5.2f}%)")

    # Латентность при ошибках
    lat_err = []
    for r in err_rows:
        lat_us = _i(r.get("latency_us", 0), 0)
        if lat_us > 0:
            lat_err.append(lat_us / 1000.0)  # to ms

    if lat_err:
        lat_err.sort()
        n_lat = len(lat_err)
        p50 = lat_err[n_lat // 2] if n_lat > 0 else 0.0
        p95 = lat_err[int(n_lat * 0.95)] if n_lat > 0 else 0.0
        p99 = lat_err[int(n_lat * 0.99)] if n_lat > 0 else 0.0
        avg = sum(lat_err) / n_lat

        print(f"\n{'─'*80}")
        print(f"ЛАТЕНТНОСТЬ ПРИ ОШИБКАХ (мс):")
        print(f"{'─'*80}")
        print(f"  Средняя: {avg:.2f} мс")
        print(f"  p50:     {p50:.2f} мс")
        print(f"  p95:     {p95:.2f} мс")
        print(f"  p99:     {p99:.2f} мс")
        print(f"  Макс:    {max(lat_err):.2f} мс")

    # Последние ошибки (детали)
    print(f"\n{'─'*80}")
    print(f"ПОСЛЕДНИЕ {min(10, len(err_rows))} ОШИБОК (детали):")
    print(f"{'─'*80}")
    for i, r in enumerate(err_rows[-10:], 1):
        ts_ms = _i(r.get("ts_ms", 0), 0)
        ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000)) if ts_ms > 0 else "unknown"
        sym = str(r.get("symbol", "") or "unknown")
        mode = str(r.get("mode", "") or "unknown")
        err = str(r.get("error", "") or "")[:80]
        reason = str(r.get("reason", "") or "")[:60]
        lat_us = _i(r.get("latency_us", 0), 0)
        lat_ms = lat_us / 1000.0 if lat_us > 0 else 0.0

        print(f"\n{i}. [{ts_str}] {sym} | {mode}")
        print(f"   Ошибка: {err}")
        print(f"   Причина: {reason}")
        print(f"   Латентность: {lat_ms:.2f} мс")

    # Временное распределение (по 5-минутным бакетам)
    buckets: Dict[int, int] = defaultdict(int)
    for r in err_rows:
        ts_ms = _i(r.get("ts_ms", 0), 0)
        if ts_ms > 0:
            bucket_min = (ts_ms - start_ms) // (5 * 60_000)  # 5-minute buckets
            buckets[bucket_min] += 1

    if buckets:
        print(f"\n{'─'*80}")
        print(f"ВРЕМЕННОЕ РАСПРЕДЕЛЕНИЕ ОШИБОК (5-минутные бакеты):")
        print(f"{'─'*80}")
        for bucket_min in sorted(buckets.keys()):
            count = buckets[bucket_min]
            time_start = start_ms + bucket_min * 5 * 60_000
            time_end = time_start + 5 * 60_000
            ts_start = time.strftime("%H:%M", time.localtime(time_start / 1000))
            ts_end = time.strftime("%H:%M", time.localtime(time_end / 1000))
            bar = "█" * min(50, int(count * 50 / max(1, max(buckets.values()))))
            print(f"  {ts_start}-{ts_end}: {count:4d} {bar}")

    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    main()

