#!/usr/bin/env python3
"""
EV Gate Statistics Analyzer

Анализирует накопленную статистику P(hit TP1) из Redis и предоставляет
инсайты для тюнинга EV gate параметров.

Usage:
    python tools/analyze_ev_stats.py
    python tools/analyze_ev_stats.py --min-trades 50
    python tools/analyze_ev_stats.py --export stats.csv
"""

import redis
import argparse
import sys
from collections import defaultdict
from typing import Dict, List
import statistics
import csv


def connect_redis(url: str = "redis://localhost:6379/0") -> redis.Redis:
    """Подключение к Redis."""
    return redis.from_url(url, decode_responses=True)


def parse_ev_key(key: str) -> Dict[str, str]:
    """
    Парсит ключ ev:tp1:{kind}:{symbol}:{tf}:{regime}
    
    Returns:
        dict с полями: kind, symbol, tf, regime
    """
    parts = key.split(":")
    if len(parts) < 5:
        return {}
    
    return {
        "kind": parts[2],
        "symbol": parts[3],
        "tf": parts[4],
        "regime": parts[5] if len(parts) > 5 else "na",
    }


def fetch_all_ev_stats(r: redis.Redis) -> List[Dict]:
    """
    Получает все EV статистики из Redis.
    
    Returns:
        List of dicts с ключами: kind, symbol, tf, regime, p_ema, n, last_ts_ms
    """
    keys = list(r.scan_iter(match="ev:tp1:*"))
    stats = []
    
    for key in keys:
        meta = parse_ev_key(key)
        if not meta:
            continue
            
        data = r.hmget(key, "p_ema", "n", "last_ts_ms")
        p_ema, n, last_ts_ms = data
        
        try:
            stats.append({
                **meta,
                "key": key,
                "p_ema": float(p_ema) if p_ema else None,
                "n": int(float(n)) if n else 0,
                "last_ts_ms": int(last_ts_ms) if last_ts_ms else 0,
            })
        except (ValueError, TypeError):
            continue
    
    return stats


def analyze_stats(stats: List[Dict], min_trades: int = 10) -> Dict:
    """
    Анализирует статистику и возвращает инсайты.
    
    Args:
        stats: список статистик
        min_trades: минимальное количество сделок для учета
        
    Returns:
        dict с результатами анализа
    """
    # Фильтруем по min_trades
    filtered = [s for s in stats if s["n"] >= min_trades and s["p_ema"] is not None]
    
    if not filtered:
        return {"error": "No data with sufficient trades"}
    
    # Группировка по различным аспектам
    by_kind = defaultdict(list)
    by_symbol = defaultdict(list)
    by_regime = defaultdict(list)
    
    all_probs = []
    
    for s in filtered:
        p = s["p_ema"]
        by_kind[s["kind"]].append(p)
        by_symbol[s["symbol"]].append(p)
        by_regime[s["regime"]].append(p)
        all_probs.append(p)
    
    # Агрегированная статистика
    return {
        "total_keys": len(stats),
        "valid_keys": len(filtered),
        "overall": {
            "mean": statistics.mean(all_probs),
            "median": statistics.median(all_probs),
            "min": min(all_probs),
            "max": max(all_probs),
            "stdev": statistics.stdev(all_probs) if len(all_probs) > 1 else 0.0,
        },
        "by_kind": {
            k: {
                "mean": statistics.mean(v),
                "median": statistics.median(v),
                "count": len(v),
            }
            for k, v in by_kind.items()
        },
        "by_symbol": {
            k: {
                "mean": statistics.mean(v),
                "median": statistics.median(v),
                "count": len(v),
            }
            for k, v in by_symbol.items()
        },
        "by_regime": {
            k: {
                "mean": statistics.mean(v),
                "median": statistics.median(v),
                "count": len(v),
            }
            for k, v in by_regime.items()
        },
    }


def print_analysis(analysis: Dict, stats: List[Dict], min_trades: int):
    """Печатает анализ в читаемом формате."""
    
    if "error" in analysis:
        print(f"❌ {analysis['error']}")
        return
    
    print("=" * 80)
    print("EV GATE STATISTICS ANALYSIS")
    print("=" * 80)
    print()
    
    print(f"📊 Dataset: {analysis['valid_keys']} valid keys (min_trades >= {min_trades})")
    print(f"   Total keys found: {analysis['total_keys']}")
    print()
    
    # Overall statistics
    ov = analysis["overall"]
    print("🎯 Overall P(hit TP1):")
    print(f"   Mean:   {ov['mean']:.3f}")
    print(f"   Median: {ov['median']:.3f}")
    print(f"   Range:  [{ov['min']:.3f}, {ov['max']:.3f}]")
    print(f"   StDev:  {ov['stdev']:.3f}")
    print()
    
    # By kind
    print("📈 By Signal Kind:")
    for kind, data in sorted(analysis["by_kind"].items(), key=lambda x: -x[1]["mean"]):
        print(f"   {kind:15s} → mean={data['mean']:.3f}, median={data['median']:.3f} (n={data['count']})")
    print()
    
    # By symbol
    print("💱 By Symbol:")
    for sym, data in sorted(analysis["by_symbol"].items(), key=lambda x: -x[1]["mean"]):
        print(f"   {sym:10s} → mean={data['mean']:.3f}, median={data['median']:.3f} (n={data['count']})")
    print()
    
    # By regime
    print("🌊 By Regime:")
    for reg, data in sorted(analysis["by_regime"].items(), key=lambda x: -x[1]["mean"]):
        print(f"   {reg:10s} → mean={data['mean']:.3f}, median={data['median']:.3f} (n={data['count']})")
    print()
    
    # Recommendations
    print("💡 Recommendations:")
    
    # Check if current p_min is too strict
    filtered_stats = [s for s in stats if s["n"] >= min_trades and s["p_ema"] is not None]
    p_values = [s["p_ema"] for s in filtered_stats]
    p25 = statistics.quantiles(p_values, n=4)[0] if len(p_values) > 4 else min(p_values)
    p50 = statistics.median(p_values)
    p75 = statistics.quantiles(p_values, n=4)[2] if len(p_values) > 4 else max(p_values)
    
    print(f"   P25: {p25:.3f}, P50: {p50:.3f}, P75: {p75:.3f}")
    
    current_p_min = 0.55  # Default from ENV
    if p50 < current_p_min:
        print(f"   ⚠️  Median P(TP1) = {p50:.3f} < current p_min = {current_p_min:.3f}")
        print(f"       → Consider lowering EDGE_EV_P_MIN to {p25:.2f} or {(p25+p50)/2:.2f}")
    else:
        print(f"   ✅ Median P(TP1) = {p50:.3f} >= current p_min = {current_p_min:.3f}")
    
    print()
    print("=" * 80)


def export_to_csv(stats: List[Dict], filename: str):
    """Экспортирует статистику в CSV."""
    if not stats:
        print("❌ No data to export")
        return
    
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "kind", "symbol", "tf", "regime", "p_ema", "n", "last_ts_ms", "key"
        ])
        writer.writeheader()
        writer.writerows(stats)
    
    print(f"✅ Exported {len(stats)} rows to {filename}")


def main():
    parser = argparse.ArgumentParser(description="Analyze EV gate statistics from Redis")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis URL")
    parser.add_argument("--min-trades", type=int, default=10, help="Minimum trades to consider")
    parser.add_argument("--export", help="Export to CSV file")
    
    args = parser.parse_args()
    
    try:
        r = connect_redis(args.redis_url)
        r.ping()
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        sys.exit(1)
    
    print("🔍 Fetching EV statistics from Redis...")
    stats = fetch_all_ev_stats(r)
    
    if not stats:
        print("❌ No EV statistics found in Redis")
        print("   Keys pattern: ev:tp1:*")
        print("   Make sure trades are closing and stats_aggregator is running")
        sys.exit(1)
    
    print(f"✅ Found {len(stats)} EV stat keys")
    print()
    
    analysis = analyze_stats(stats, min_trades=args.min_trades)
    print_analysis(analysis, stats, args.min_trades)
    
    if args.export:
        export_to_csv(stats, args.export)


if __name__ == "__main__":
    main()
