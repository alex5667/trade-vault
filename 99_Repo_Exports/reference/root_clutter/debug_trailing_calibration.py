#!/usr/bin/env python3
"""
Debug script to analyze trailing calibration data issues.
Checks what's actually in the trades:closed stream.
"""

import os
import sys
from pathlib import Path

# Add the python-worker directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "python-worker"))

from core.redis_client import get_redis
from domain.normalizers import canon_source, canon_symbol

def analyze_trades_closed_stream():
    """Analyze the trades:closed stream to understand data structure."""
    redis = get_redis()

    print("🔍 Analyzing trades:closed stream...")

    # Get recent entries
    entries = redis.xrevrange("trades:closed", max="+", count=1000) or []

    if not entries:
        print("❌ No entries found in trades:closed stream")
        return

    print(f"📊 Found {len(entries)} entries in trades:closed stream")

    # Analyze sources and symbols
    sources = {}
    symbols = {}
    source_symbol_pairs = {}

    for entry_id, fields in entries:
        # Normalize field access
        t = {str(k): str(v) for k, v in fields.items()}

        # Extract source and symbol
        raw_source = t.get("source") or t.get("strategy") or ""
        raw_symbol = t.get("symbol") or ""

        if raw_source:
            sources[raw_source] = sources.get(raw_source, 0) + 1
        if raw_symbol:
            symbols[raw_symbol] = symbols.get(raw_symbol, 0) + 1

        # Track pairs
        pair = (raw_source, raw_symbol)
        source_symbol_pairs[pair] = source_symbol_pairs.get(pair, 0) + 1

    print("\n📋 Sources found:")
    for source, count in sorted(sources.items(), key=lambda x: x[1], reverse=True):
        print(f"  {source}: {count}")

    print("\n📋 Top symbols found:")
    for symbol, count in sorted(symbols.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"  {symbol}: {count}")

    print("\n📋 Top source/symbol pairs:")
    for (source, symbol), count in sorted(source_symbol_pairs.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"  {source}/{symbol}: {count}")

    # Check for CryptoOrderFlow specifically
    crypto_orderflow_count = 0
    for entry_id, fields in entries:
        t = {str(k): str(v) for k, v in fields.items()}
        raw_source = t.get("source") or t.get("strategy") or ""
        if "cryptoorderflow" in raw_source.lower() or "orderflow" in raw_source.lower():
            crypto_orderflow_count += 1

    print(f"\n🎯 Entries with orderflow-related sources: {crypto_orderflow_count}")

    # Check what the canon_source function produces
    print("\n🔄 Source normalization check:")
    for raw_source in sources.keys():
        normalized = canon_source(raw_source)
        print(f"  '{raw_source}' -> '{normalized}'")

if __name__ == "__main__":
    analyze_trades_closed_stream()
