from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

#!/usr/bin/env python3
"""
Проверка формирования сделок для XAUUSDT.

Проверяет всю цепочку:
1. Генерируются ли сигналы для XAUUSDT
2. Открываются ли позиции (orders:open)
3. Закрываются ли позиции (trades:closed)
"""

import json
import os
import sys
from datetime import UTC, datetime
from typing import Any

# Add parent directory to path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import redis

from core.redis_client import get_redis

SYMBOL = "XAUUSDT"
SOURCE = "CryptoOrderFlow"
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

# Streams to check
SIGNAL_STREAMS = [
    RS.CRYPTO_RAW,
    "signals:aggregated:XAUUSDT",
    "signals:cryptoorderflow:XAUUSDT",
]


def check_stream_length(r: redis.Redis, stream_name: str) -> int:
    """Получает длину stream."""
    try:
        info = r.xinfo_stream(stream_name)
        return info.get("length", 0)
    except redis.exceptions.ResponseError:
        return 0
    except Exception:
        return 0


def read_recent_entries(r: redis.Redis, stream_name: str, count: int = 100, filter_symbol: str | None = None) -> list[dict]:
    """Читает последние записи из stream."""
    entries = []
    try:
        data = r.xrevrange(stream_name, count=count)
        for entry_id, fields in data:
            try:
                # Parse payload
                payload_str = fields.get("payload") or fields.get("data") or "{}"
                if isinstance(payload_str, bytes):
                    payload_str = payload_str.decode("utf-8", errors="ignore")

                if payload_str:
                    try:
                        payload = json.loads(payload_str)
                    except json.JSONDecodeError:
                        payload = dict(fields)
                else:
                    payload = dict(fields)

                # Filter by symbol if needed
                if filter_symbol:
                    symbol = (payload.get("symbol") or "").upper()
                    if symbol != filter_symbol:
                        continue

                entries.append({
                    "id": entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id),
                    "payload": payload,
                    "fields": dict(fields),
                })
            except Exception as e:
                print(f"⚠️  Ошибка парсинга записи из {stream_name}: {e}")
                continue
    except Exception as e:
        print(f"⚠️  Ошибка чтения {stream_name}: {e}")

    return entries


def check_signals(r: redis.Redis) -> dict[str, Any]:
    """Проверяет наличие сигналов для XAUUSDT."""
    print("\n1️⃣ Проверка сигналов:")
    print("-" * 80)

    all_signals = []
    for stream_name in SIGNAL_STREAMS:
        length = check_stream_length(r, stream_name)
        if length == 0:
            print(f"  ⚠️  {stream_name}: stream пуст или не существует")
            continue

        signals = read_recent_entries(r, stream_name, count=200, filter_symbol=SYMBOL)
        if signals:
            print(f"  ✅ {stream_name}: найдено {len(signals)} сигналов для {SYMBOL}")
            all_signals.extend(signals)
        else:
            print(f"  ❌ {stream_name}: сигналов для {SYMBOL} не найдено (всего записей: {length})")

    result = {
        "found": len(all_signals),
        "signals": all_signals[:10],  # Keep first 10 for analysis
    }

    if all_signals:
        # Analyze confidence
        confidences = []
        for sig in all_signals:
            payload = sig.get("payload", {})
            conf = float(payload.get("confidence") or payload.get("conf") or 0.0)
            if conf > 0:
                confidences.append(conf)

        if confidences:
            result["avg_confidence"] = sum(confidences) / len(confidences)
            result["min_confidence"] = min(confidences)
            result["max_confidence"] = max(confidences)
            print(f"  📊 Confidence: avg={result['avg_confidence']:.1%}, min={result['min_confidence']:.1%}, max={result['max_confidence']:.1%}")

    return result


def check_open_positions(r: redis.Redis) -> dict[str, Any]:
    """Проверяет открытые позиции для XAUUSDT."""
    print("\n2️⃣ Проверка открытых позиций (orders:open):")
    print("-" * 80)

    try:
        open_ids = r.smembers("orders:open") or set()
        print(f"  📊 Всего открытых позиций: {len(open_ids)}")

        xauusdt_positions = []
        for oid in open_ids:
            try:
                oid_str = oid.decode() if isinstance(oid, bytes) else str(oid)
                order_data = r.hgetall(f"order:{oid_str}")
                if not order_data:
                    continue

                symbol = (order_data.get("symbol") or "").upper()
                source = (order_data.get("source") or "").strip()

                if symbol == SYMBOL and (not SOURCE or source == SOURCE or source.lower() == SOURCE.lower()):
                    xauusdt_positions.append({
                        "id": oid_str,
                        "symbol": symbol,
                        "source": source,
                        "status": order_data.get("status", "unknown"),
                        "direction": order_data.get("direction", "unknown"),
                        "entry_price": order_data.get("entry_price", "unknown"),
                        "entry_ts_ms": order_data.get("entry_ts_ms", "unknown"),
                    })
            except Exception as e:
                print(f"  ⚠️  Ошибка чтения order:{oid}: {e}")
                continue

        result = {
            "total_open": len(open_ids),
            "xauusdt_open": len(xauusdt_positions),
            "positions": xauusdt_positions,
        }

        if xauusdt_positions:
            print(f"  ✅ Найдено {len(xauusdt_positions)} открытых позиций для {SYMBOL} (source={SOURCE})")
            for pos in xauusdt_positions[:5]:  # Show first 5
                print(f"     - {pos['id']}: {pos['direction']} @ {pos['entry_price']} (status={pos['status']})")
        else:
            print(f"  ❌ Нет открытых позиций для {SYMBOL} (source={SOURCE})")

        return result
    except Exception as e:
        print(f"  ❌ Ошибка проверки orders:open: {e}")
        return {"error": str(e)}


def check_closed_trades(r: redis.Redis, count: int = 1000) -> dict[str, Any]:
    """Проверяет закрытые сделки для XAUUSDT."""
    print("\n3️⃣ Проверка закрытых сделок (trades:closed):")
    print("-" * 80)

    try:
        stream_length = check_stream_length(r, "trades:closed")
        print(f"  📊 Всего записей в trades:closed: {stream_length}")

        # Read recent entries
        entries = read_recent_entries(r, "trades:closed", count=count, filter_symbol=SYMBOL)

        # Filter by source
        xauusdt_trades = []
        for entry in entries:
            payload = entry.get("payload", {})
            symbol = (payload.get("symbol") or "").upper()
            source = (payload.get("source") or "").strip()

            if symbol == SYMBOL and (not SOURCE or source == SOURCE or source.lower() == SOURCE.lower()):
                xauusdt_trades.append({
                    "id": entry.get("id"),
                    "order_id": payload.get("order_id") or payload.get("id", "unknown"),
                    "symbol": symbol,
                    "source": source,
                    "direction": payload.get("direction", "unknown"),
                    "pnl_net": payload.get("pnl_net", "unknown"),
                    "exit_ts_ms": payload.get("exit_ts_ms", "unknown"),
                    "close_reason": payload.get("close_reason", "unknown"),
                })

        result = {
            "stream_length": stream_length,
            "found": len(xauusdt_trades),
            "trades": xauusdt_trades[:10],  # Keep first 10
        }

        if xauusdt_trades:
            print(f"  ✅ Найдено {len(xauusdt_trades)} закрытых сделок для {SYMBOL} (source={SOURCE})")
            for trade in xauusdt_trades[:5]:  # Show first 5
                pnl = trade.get("pnl_net", "unknown")
                reason = trade.get("close_reason", "unknown")
                print(f"     - {trade['order_id']}: {trade['direction']}, PnL={pnl}, reason={reason}")
        else:
            print(f"  ❌ Нет закрытых сделок для {SYMBOL} (source={SOURCE})")
            print(f"     Проверено {len(entries)} записей из {stream_length} в stream")

        return result
    except Exception as e:
        print(f"  ❌ Ошибка проверки trades:closed: {e}")
        return {"error": str(e)}


def check_ticks(r: redis.Redis) -> dict[str, Any]:
    """Проверяет наличие тиков для XAUUSDT."""
    print("\n4️⃣ Проверка тиков (stream:tick_XAUUSDT):")
    print("-" * 80)

    stream_name = f"stream:tick_{SYMBOL}"
    try:
        length = check_stream_length(r, stream_name)
        if length > 0:
            print(f"  ✅ {stream_name}: {length} тиков")

            # Check last tick timestamp
            entries = r.xrevrange(stream_name, count=1)
            if entries:
                entry_id, fields = entries[0]
                ts = fields.get("ts_ms") or fields.get("ts") or 0
                if isinstance(ts, bytes):
                    ts = int(ts.decode())
                elif isinstance(ts, str):
                    ts = int(ts)

                age_sec = (get_ny_time_millis() - ts) / 1000
                print(f"     Последний тик: {age_sec:.1f} сек назад")
        else:
            print(f"  ❌ {stream_name}: stream пуст или не существует")

        return {"length": length, "stream": stream_name}
    except Exception as e:
        print(f"  ❌ Ошибка проверки {stream_name}: {e}")
        return {"error": str(e)}


def check_config(r: redis.Redis) -> dict[str, Any]:
    """Проверяет конфигурацию для XAUUSDT."""
    print("\n5️⃣ Проверка конфигурации:")
    print("-" * 80)

    config_key = f"config:orderflow:{SYMBOL}"
    try:
        config = r.hgetall(config_key)
        if config:
            print(f"  ✅ {config_key}: найдено {len(config)} параметров")
            # Show some key params
            key_params = ["of_score_min", "confidence_floor", "delta_abs_min", "orders_queue_enabled"]
            for param in key_params:
                if param in config:
                    print(f"     - {param}: {config[param]}")
        else:
            print(f"  ⚠️  {config_key}: конфигурация не найдена (используются дефолты)")

        return {"found": len(config) if config else 0, "config": dict(config) if config else {}}
    except Exception as e:
        print(f"  ❌ Ошибка проверки {config_key}: {e}")
        return {"error": str(e)}


def main():
    print(f"🔍 Проверка формирования сделок для {SYMBOL} (source={SOURCE})")
    print(f"⏰ Время: {datetime.now(UTC).isoformat()}")
    print("=" * 80)

    r = get_redis() if not REDIS_URL else redis.from_url(REDIS_URL, decode_responses=False)

    # Run all checks
    signals_result = check_signals(r)
    open_result = check_open_positions(r)
    closed_result = check_closed_trades(r)
    ticks_result = check_ticks(r)
    config_result = check_config(r)

    # Summary
    print("\n" + "=" * 80)
    print("📊 ИТОГОВАЯ СТАТИСТИКА:")
    print("-" * 80)
    print(f"  Сигналы: {signals_result.get('found', 0)} найдено")
    print(f"  Открытые позиции: {open_result.get('xauusdt_open', 0)}")
    print(f"  Закрытые сделки: {closed_result.get('found', 0)}")
    print(f"  Тики: {ticks_result.get('length', 0)} в stream")

    # Diagnosis
    print("\n" + "=" * 80)
    print("🔍 ДИАГНОСТИКА:")
    print("-" * 80)

    if signals_result.get("found", 0) == 0:
        print("❌ ПРОБЛЕМА: Сигналы не генерируются для XAUUSDT")
        print("   💡 Проверьте:")
        print("      - CryptoOrderflowService обрабатывает ли XAUUSDT")
        print("      - XAUUSDT в ORDERFLOW_SYMBOLS")
        print("      - Тики приходят в stream:tick_XAUUSDT")
        if ticks_result.get("length", 0) == 0:
            print("      ⚠️  Тики не приходят - это основная проблема!")

    if signals_result.get("found", 0) > 0 and open_result.get("xauusdt_open", 0) == 0:
        print("❌ ПРОБЛЕМА: Сигналы генерируются, но позиции не открываются")
        print("   💡 Проверьте:")
        print("      - TradeMonitorService обрабатывает ли сигналы")
        print("      - Confidence threshold (CRYPTO_SIGNAL_MIN_CONF)")
        if "avg_confidence" in signals_result:
            conf = signals_result["avg_confidence"]
            print(f"      - Средняя confidence сигналов: {conf:.1%}")
            if conf < 0.70:
                print("      ⚠️  Confidence слишком низкая! Нужно >= 70%")
        print("      - ML confirm gate не блокирует ли сигналы")

    if open_result.get("xauusdt_open", 0) > 0 and closed_result.get("found", 0) == 0:
        print("❌ ПРОБЛЕМА: Позиции открываются, но не закрываются")
        print("   💡 Проверьте:")
        print("      - TradeMonitorService закрывает ли позиции")
        print("      - Позиции не застряли ли в orders:open")
        print(f"      - Всего открытых позиций: {open_result.get('total_open', 0)}")

    if closed_result.get("found", 0) == 0:
        print("\n❌ ПРОБЛЕМА: Нет закрытых сделок для XAUUSDT")
        print("   💡 Это означает, что цепочка не работает:")
        print("      Сигналы → Открытие позиций → Закрытие позиций")
        print("\n   Рекомендации:")
        if signals_result.get("found", 0) == 0:
            print("   1. Убедитесь, что тики приходят для XAUUSDT")
            print("   2. Проверьте логи CryptoOrderflowService")
        elif open_result.get("xauusdt_open", 0) == 0:
            print("   1. Проверьте фильтры TradeMonitorService (confidence, ML gate)")
            print("   2. Запустите: python3 python-worker/tools/diagnose_xauusdt_signals.py")
        else:
            print("   1. Проверьте, что позиции закрываются (TP/SL)")
            print("   2. Проверьте логи TradeMonitorService")


if __name__ == "__main__":
    main()

