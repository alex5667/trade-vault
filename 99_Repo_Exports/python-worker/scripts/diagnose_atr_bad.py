#!/usr/bin/env python3
"""
Диагностический скрипт для анализа проблем с ATR (Average True Range).

Проверяет:
1. Распределение причин плохих ATR (atr_bad_total по reason)
2. Задержки данных (stale)
3. Массовые скачки
4. Топ проблемных символов

Usage:
    python scripts/diagnose_atr_bad.py [--symbol SYMBOL] [--top N] [--reason REASON]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from typing import Dict, List, Tuple

import redis


def _decode(x) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        return x.decode("utf-8", "ignore")
    return str(x)


def _sscan_all(r: redis.Redis, key: str, limit: int = 2000) -> List[str]:
    """Scan Redis set and return all members."""
    out: List[str] = []
    cur = 0
    while True:
        cur, batch = r.sscan(key, cursor=cur, count=10000)
        for b in batch or []:
            s = _decode(b)
            if s:
                out.append(s)
                if len(out) >= limit:
                    return sorted(set(out))
        if int(cur) == 0:
            break
    return sorted(set(out))


def get_atr_bad_symbols(r: redis.Redis) -> List[str]:
    """Get all symbols with bad ATR."""
    return _sscan_all(r, "cfg:atr_bad:symbols", limit=2000)


def get_reason_distribution(r: redis.Redis, symbols: List[str]) -> Dict[str, int]:
    """Get distribution of reasons across all symbols."""
    reason_stats: Dict[str, int] = defaultdict(int)
    
    for symbol in symbols:
        try:
            reason_hash = r.hgetall(f"metrics:atr_bad_total:{symbol}")
            if reason_hash:
                for reason_key, count_val in (reason_hash or {}).items():
                    reason = _decode(reason_key)
                    count = int(_decode(count_val) or "0")
                    if count > 0:
                        reason_stats[reason] += count
        except Exception as e:
            print(f"Warning: Failed to get metrics for {symbol}: {e}", file=sys.stderr)
    
    return dict(reason_stats)


def get_symbol_details(r: redis.Redis, symbol: str) -> Dict[str, any]:
    """Get detailed info about a symbol's ATR bad status."""
    details: Dict[str, any] = {
        "symbol": symbol
        "bad_active": False
        "current_reason": "unknown"
        "reason_counts": {}
        "total_count": 0
        "bad_info": None
    }
    
    # Check if currently bad
    try:
        bad_info_raw = _decode(r.get(f"cfg:atr_bad:{symbol}"))
        if bad_info_raw:
            details["bad_active"] = True
            try:
                if bad_info_raw.startswith("{"):
                    details["bad_info"] = json.loads(bad_info_raw)
                    details["current_reason"] = str(details["bad_info"].get("reason", "unknown"))
                else:
                    details["current_reason"] = "active" if bad_info_raw == "1" else "unknown"
            except Exception:
                details["current_reason"] = "unknown"
    except Exception:
        pass
    
    # Get reason distribution from metrics
    try:
        reason_hash = r.hgetall(f"metrics:atr_bad_total:{symbol}")
        if reason_hash:
            for reason_key, count_val in (reason_hash or {}).items():
                reason = _decode(reason_key)
                count = int(_decode(count_val) or "0")
                if count > 0:
                    details["reason_counts"][reason] = count
                    details["total_count"] += count
    except Exception:
        pass
    
    return details


def analyze_stale_issues(r: redis.Redis, symbols: List[str]) -> Dict[str, int]:
    """Analyze stale data issues (age > max_age_ms)."""
    stale_stats: Dict[str, int] = defaultdict(int)
    
    for symbol in symbols:
        try:
            reason_hash = r.hgetall(f"metrics:atr_bad_total:{symbol}")
            if reason_hash:
                for reason_key, count_val in (reason_hash or {}).items():
                    reason = _decode(reason_key)
                    if "stale" in reason.lower():
                        count = int(_decode(count_val) or "0")
                        stale_stats[symbol] += count
        except Exception:
            pass
    
    return dict(stale_stats)


def analyze_jump_issues(r: redis.Redis, symbols: List[str]) -> Dict[str, int]:
    """Analyze jump issues (relative jumps > threshold)."""
    jump_stats: Dict[str, int] = defaultdict(int)
    
    for symbol in symbols:
        try:
            reason_hash = r.hgetall(f"metrics:atr_bad_total:{symbol}")
            if reason_hash:
                for reason_key, count_val in (reason_hash or {}).items():
                    reason = _decode(reason_key)
                    if "jump" in reason.lower():
                        count = int(_decode(count_val) or "0")
                        jump_stats[symbol] += count
        except Exception:
            pass
    
    return dict(jump_stats)


def extract_tf_from_reason(reason: str) -> str:
    """Extract timeframe from reason string like 'stale>120000:tf=1m'"""
    import re
    match = re.search(r'tf=([a-z0-9]+)', reason.lower())
    return match.group(1) if match else "unknown"


def show_tf_breakdown(r: redis.Redis, symbols: List[str]) -> None:
    """Show breakdown of bad ATR by timeframe."""
    tf_stats: Dict[str, int] = defaultdict(int)
    reason_by_tf: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    
    # Analyze a sample of symbols (or all if count is small) to avoid massive lookups
    sample_symbols = symbols[:500] 
    
    for symbol in sample_symbols:
        try:
            bad_info_raw = _decode(r.get(f"cfg:atr_bad:{symbol}"))
            if bad_info_raw:
                info = json.loads(bad_info_raw) if bad_info_raw.startswith("{") else {}
                reason = str(info.get("reason", ""))
                
                # Try to extract TF from reason
                tf = extract_tf_from_reason(reason)
                tf_stats[tf] += 1
                
                # Extract base reason (stale, jump, etc.)
                base_reason = "other"
                if "stale" in reason.lower():
                    base_reason = "stale"
                elif "jump" in reason.lower():
                    base_reason = "jump"
                elif "atr<=0" in reason.lower():
                    base_reason = "atr_zero"
                elif "bps_oob" in reason.lower():
                    base_reason = "bps_oob"
                    
                reason_by_tf[tf][base_reason] += 1
        except Exception:
            pass
            
    print("\n--- Bad ATR Breakdown by Timeframe (Top 500 sample) ---")
    if not tf_stats:
        print("  No timeframe info found")
        return

    for tf in sorted(tf_stats.keys()):
        count = tf_stats[tf]
        reasons = reason_by_tf[tf]
        reason_str = ", ".join([f"{k}:{v}" for k, v in sorted(reasons.items(), key=lambda x: -x[1])])
        print(f"  {tf:8s}: {count:4d} symbols ({reason_str})")

def print_summary(r: redis.Redis, symbols: List[str], top_n: int = 20):
    """Print summary of ATR bad issues."""
    print("=" * 80)
    print("ATR BAD DIAGNOSTICS SUMMARY")
    print("=" * 80)
    print(f"\nTotal symbols with bad ATR: {len(symbols)}")
    
    # Timeframe breakdown
    show_tf_breakdown(r, symbols)
    
    # Reason distribution
    print("\n--- Reason Distribution (all symbols) ---")
    reason_dist = get_reason_distribution(r, symbols)
    if reason_dist:
        total_reasons = sum(reason_dist.values())
        for reason, count in sorted(reason_dist.items(), key=lambda x: x[1], reverse=True):
            pct = 100.0 * count / total_reasons if total_reasons > 0 else 0.0
            print(f"  {reason:40s} {count:8d} ({pct:5.1f}%)")
    else:
        print("  No reason metrics found")
    
    # Stale analysis
    print("\n--- Stale Data Issues ---")
    stale_issues = analyze_stale_issues(r, symbols)
    if stale_issues:
        stale_sorted = sorted(stale_issues.items(), key=lambda x: x[1], reverse=True)
        print(f"  Symbols with stale issues: {len(stale_issues)}")
        for symbol, count in stale_sorted[:top_n]:
            print(f"    {symbol:20s} {count:6d} stale events")
    else:
        print("  No stale issues detected")
    
    # Jump analysis
    print("\n--- Jump Issues (massive ATR changes) ---")
    jump_issues = analyze_jump_issues(r, symbols)
    if jump_issues:
        jump_sorted = sorted(jump_issues.items(), key=lambda x: x[1], reverse=True)
        print(f"  Symbols with jump issues: {len(jump_issues)}")
        for symbol, count in jump_sorted[:top_n]:
            print(f"    {symbol:20s} {count:6d} jump events")
    else:
        print("  No jump issues detected")
    
    # Top problematic symbols
    print("\n--- Top Problematic Symbols ---")
    symbol_details_list = []
    for symbol in symbols[:top_n * 2]:  # Check more to get top N by count
        details = get_symbol_details(r, symbol)
        if details["total_count"] > 0 or details["bad_active"]:
            symbol_details_list.append(details)
    
    # Sort by total count
    symbol_details_list.sort(key=lambda x: x["total_count"], reverse=True)
    
    for i, details in enumerate(symbol_details_list[:top_n], 1):
        print(f"\n  {i}. {details['symbol']}")
        print(f"     Active: {details['bad_active']}")
        print(f"     Current reason: {details['current_reason']}")
        print(f"     Total events: {details['total_count']}")
        if details["reason_counts"]:
            print(f"     Reason breakdown:")
            for reason, count in sorted(details["reason_counts"].items(), key=lambda x: x[1], reverse=True):
                print(f"       {reason:40s} {count:6d}")
        if details["bad_info"]:
            print(f"     Bad info: {json.dumps(details['bad_info'], indent=8)}")
    
    print("\n" + "=" * 80)


def print_symbol_details(r: redis.Redis, symbol: str):
    """Print detailed info for a specific symbol."""
    print("=" * 80)
    print(f"ATR BAD DETAILS: {symbol}")
    print("=" * 80)
    
    details = get_symbol_details(r, symbol)
    
    print(f"\nSymbol: {details['symbol']}")
    print(f"Currently bad: {details['bad_active']}")
    print(f"Current reason: {details['current_reason']}")
    print(f"Total bad events: {details['total_count']}")
    
    if details["reason_counts"]:
        print("\nReason breakdown:")
        for reason, count in sorted(details["reason_counts"].items(), key=lambda x: x[1], reverse=True):
            print(f"  {reason:40s} {count:8d}")
    
    if details["bad_info"]:
        print(f"\nBad info (from cfg:atr_bad:{symbol}):")
        print(json.dumps(details["bad_info"], indent=2))
    
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Diagnose ATR bad issues")
    parser.add_argument("--symbol", type=str, help="Analyze specific symbol")
    parser.add_argument("--top", type=int, default=20, help="Number of top symbols to show (default: 20)")
    parser.add_argument("--reason", type=str, help="Filter by reason (substring match)")
    parser.add_argument("--redis-url", type=str, help="Redis URL (default: from REDIS_URL env)")
    
    args = parser.parse_args()
    
    redis_url = args.redis_url or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
    r = redis.Redis.from_url(redis_url, decode_responses=False)
    
    if args.symbol:
        # Single symbol analysis
        print_symbol_details(r, args.symbol.upper())
    else:
        # Full analysis
        symbols = get_atr_bad_symbols(r)
        
        if args.reason:
            # Filter symbols by reason
            filtered = []
            for symbol in symbols:
                details = get_symbol_details(r, symbol)
                if any(args.reason.lower() in r.lower() for r in details["reason_counts"].keys()):
                    filtered.append(symbol)
            symbols = filtered
            print(f"Filtered to {len(symbols)} symbols matching reason '{args.reason}'")
        
        if not symbols:
            print("No symbols with bad ATR found.")
            return
        
        print_summary(r, symbols, top_n=args.top)


if __name__ == "__main__":
    main()

