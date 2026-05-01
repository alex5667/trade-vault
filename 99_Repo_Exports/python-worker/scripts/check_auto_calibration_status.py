#!/usr/bin/env python3
from __future__ import annotations
"""
Check Auto Calibration Status

Проверить статус автоматической калибровки параметров торговли.
"""


import json
import os
import sys
from datetime import datetime

# Add parent directory to path for common imports
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from services.auto_calibration_service import get_auto_calibration_service


def format_timestamp(ts_ms: int) -> str:
    """Форматировать timestamp в читаемый вид."""
    if not ts_ms:
        return "Never"
    dt = datetime.fromtimestamp(ts_ms / 1000)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def main():
    try:
        service = get_auto_calibration_service()
        status = service.get_calibration_status()

        print("=== Auto Calibration Status ===\n")

        # Конфигурация
        config = status["config"]
        print("📋 Configuration:")
        print(f"  Trades threshold: {config['trades_threshold']}")
        print(f"  Enabled symbols: {', '.join(config['enabled_symbols'])}")
        print(f"  Min trades for calibration: {config['min_trades_for_calibration']}")
        print(f"  Source: {config['source']}")
        print()

        # Счетчики сделок
        counters = status["counters"]
        print("📊 Trade Counters:")
        for symbol, count in counters.items():
            threshold = config['trades_threshold']
            progress = min(100, (count / threshold) * 100)
            bars = "█" * int(progress / 5) + "░" * (20 - int(progress / 5))
            print(f"  {symbol}: {count}/{threshold} [{bars}] {progress:.1f}%")
        print()

        # Последняя калибровка
        last_cal = status.get("last_calibration_ts")
        print("🕒 Last Calibration:")
        print(f"  {format_timestamp(last_cal) if last_cal else 'Never'}")
        print()

        # Текущие параметры в Redis (если есть)
        print("🔧 Current Parameters in Redis:")
        from core.redis_client import get_redis
        redis = get_redis()

        for symbol in config['enabled_symbols']:
            spec_key = f"symbol_specs:{symbol}"
            spec_data = redis.get(spec_key)

            if spec_data:
                try:
                    spec = json.loads(spec_data)
                    trailing = spec.get("trailing", {})
                    stop_atr = trailing.get("stop_atr_mult", "Not set")
                    rr_levels = trailing.get("rr_levels", "Not set")
                    print(f"  {symbol}: stop_atr_mult={stop_atr}, rr_levels={rr_levels}")
                except json.JSONDecodeError:
                    print(f"  {symbol}: Invalid JSON in Redis")
            else:
                print(f"  {symbol}: No spec in Redis")

        print("\n✅ Status check completed successfully")

    except Exception as e:
        print(f"❌ Error checking status: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
