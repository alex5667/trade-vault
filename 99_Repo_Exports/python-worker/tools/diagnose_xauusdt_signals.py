#!/usr/bin/env python3
"""
Диагностический скрипт для проверки, почему не открываются сделки для XAUUSDT.

Проверяет:
1. Генерируются ли сигналы CryptoOrderFlow для XAUUSDT
2. Какая confidence у сигналов
3. Проходят ли сигналы через фильтры TradeMonitorService
4. Блокируются ли сигналы ML confirm gate
"""

import json
import os
import sys
from datetime import UTC, datetime
from typing import Any
from core.redis_keys import RedisStreams as RS

# Add parent directory to path
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import redis

# from core.redis_client import get_redis # Removed missing import


SYMBOL = "XAUUSDT"
SOURCE = "CryptoOrderFlow"
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

# Streams to check
STREAMS_TO_CHECK = [
    RS.CRYPTO_RAW,  # Raw signals from CryptoOrderflowService
    "signals:aggregated:XAUUSDT",  # Aggregated signals
    "signals:cryptoorderflow:XAUUSDT",  # Audit stream
    RS.SIGNAL_OUTBOX,  # Outbox stream
]

# Confidence threshold from TradeMonitorService
CONF_THRESHOLD = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70")) / 100.0


def check_stream_exists(r: redis.Redis, stream_name: str) -> bool:
    """Проверяет существование stream."""
    try:
        info = r.xinfo_stream(stream_name)
        return True
    except redis.exceptions.ResponseError as e:
        if "no such key" in str(e).lower():
            return False
        raise
    except Exception:
        return False


def get_stream_length(r: redis.Redis, stream_name: str) -> int:
    """Получает длину stream."""
    try:
        info = r.xinfo_stream(stream_name)
        return info.get("length", 0)
    except Exception:
        return 0


def read_recent_signals(r: redis.Redis, stream_name: str, count: int = 100) -> list[dict]:
    """Читает последние сигналы из stream."""
    signals = []
    try:
        # Read from the end
        entries = r.xrevrange(stream_name, count=count)
        for entry_id, fields in entries:
            try:
                # Parse payload
                payload_str = fields.get("payload") or fields.get("data") or "{}"
                if isinstance(payload_str, bytes):
                    payload_str = payload_str.decode("utf-8", errors="ignore")

                if payload_str:
                    try:
                        payload = json.loads(payload_str)
                    except json.JSONDecodeError:
                        # Try parsing fields directly
                        payload = dict(fields)
                else:
                    payload = dict(fields)

                # Check if it's for XAUUSDT
                symbol = (payload.get("symbol") or "").upper()
                source = (payload.get("source") or "").strip()

                if symbol == SYMBOL and (not SOURCE or source == SOURCE or source.lower() == SOURCE.lower()):
                    signals.append({
                        "id": entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id),
                        "payload": payload,
                        "fields": dict(fields),
                    })
            except Exception as e:
                print(f"⚠️  Ошибка парсинга сигнала из {stream_name}: {e}")
                continue
    except Exception as e:
        print(f"⚠️  Ошибка чтения {stream_name}: {e}")

    return signals


def analyze_signal(signal: dict) -> dict[str, Any]:
    """Анализирует сигнал на предмет проблем."""
    payload = signal.get("payload", {})

    analysis = {
        "sid": payload.get("sid") or payload.get("signal_id", "unknown"),
        "symbol": payload.get("symbol", "unknown"),
        "source": payload.get("source", "unknown"),
        "confidence": float(payload.get("confidence") or payload.get("conf") or 0.0),
        "direction": payload.get("side") or payload.get("direction", "unknown"),
        "entry": payload.get("entry") or payload.get("price", 0.0),
        "ts": payload.get("ts") or payload.get("timestamp", 0),
        "issues": [],
    }

    # Check confidence threshold
    if analysis["confidence"] < CONF_THRESHOLD:
        analysis["issues"].append(
            f"❌ Confidence {analysis['confidence']:.1%} < threshold {CONF_THRESHOLD:.1%}"
        )

    # Check required fields
    if not analysis["sid"] or analysis["sid"] == "unknown":
        analysis["issues"].append("❌ Missing sid")

    if analysis["entry"] <= 0:
        analysis["issues"].append("❌ Invalid entry price")

    if analysis["direction"] not in ("LONG", "SHORT"):
        analysis["issues"].append(f"❌ Invalid direction: {analysis['direction']}")

    # Check ML confirm gate fields
    indicators = payload.get("indicators") or {}
    if isinstance(indicators, str):
        try:
            indicators = json.loads(indicators)
        except Exception:
            indicators = {}

    ml_ok = indicators.get("of_confirm_ok", indicators.get("ml_confirm_ok"))
    if ml_ok is not None and int(ml_ok) == 0:
        analysis["issues"].append("❌ ML confirm gate blocked (of_confirm_ok=0)")

    return analysis


def main():
    print(f"🔍 Диагностика сигналов для {SYMBOL} (source={SOURCE})")
    print(f"📊 Confidence threshold: {CONF_THRESHOLD:.1%}")
    print(f"⏰ Время: {datetime.now(UTC).isoformat()}")
    print("=" * 80)

    r = redis.from_url(REDIS_URL, decode_responses=True)

    # 1. Check if streams exist
    print("\n1️⃣ Проверка наличия streams:")
    for stream_name in STREAMS_TO_CHECK:
        exists = check_stream_exists(r, stream_name)
        length = get_stream_length(r, stream_name) if exists else 0
        status = "✅" if exists else "❌"
        print(f"  {status} {stream_name}: exists={exists}, length={length}")

    # 2. Read recent signals
    print(f"\n2️⃣ Поиск сигналов для {SYMBOL} в последних 100 сообщениях:")
    all_signals = []

    for stream_name in STREAMS_TO_CHECK:
        if not check_stream_exists(r, stream_name):
            continue

        signals = read_recent_signals(r, stream_name, count=100)
        if signals:
            print(f"  ✅ {stream_name}: найдено {len(signals)} сигналов")
            all_signals.extend(signals)
        else:
            print(f"  ⚠️  {stream_name}: сигналов не найдено")

    if not all_signals:
        print(f"\n❌ ПРОБЛЕМА: Не найдено сигналов для {SYMBOL} с source={SOURCE}")
        print("\n💡 Возможные причины:")
        print("  1. CryptoOrderflowService не генерирует сигналы для XAUUSDT")
        print("  2. XAUUSDT не в списке обрабатываемых символов")
        print("  3. Сигналы блокируются на этапе генерации (gates, filters)")
        print("  4. Сигналы публикуются в другой stream")
        return

    # 3. Analyze signals
    print(f"\n3️⃣ Анализ {len(all_signals)} найденных сигналов:")
    print("-" * 80)

    signals_by_status = {"ok": [], "blocked": []}

    for signal in all_signals:
        analysis = analyze_signal(signal)

        if analysis["issues"]:
            signals_by_status["blocked"].append(analysis)
            print(f"\n❌ БЛОКИРОВАН: {analysis['sid']}")
            print(f"   Confidence: {analysis['confidence']:.1%}")
            print(f"   Direction: {analysis['direction']}")
            print(f"   Entry: {analysis['entry']}")
            for issue in analysis["issues"]:
                print(f"   {issue}")
        else:
            signals_by_status["ok"].append(analysis)
            print(f"\n✅ OK: {analysis['sid']}")
            print(f"   Confidence: {analysis['confidence']:.1%}")
            print(f"   Direction: {analysis['direction']}")
            print(f"   Entry: {analysis['entry']}")

    # 4. Summary
    print("\n" + "=" * 80)
    print("📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   Всего сигналов: {len(all_signals)}")
    print(f"   ✅ Проходят фильтры: {len(signals_by_status['ok'])}")
    print(f"   ❌ Блокируются: {len(signals_by_status['blocked'])}")

    if signals_by_status["ok"]:
        avg_conf = sum(s["confidence"] for s in signals_by_status["ok"]) / len(signals_by_status["ok"])
        print(f"   Средняя confidence (OK): {avg_conf:.1%}")

    if signals_by_status["blocked"]:
        print("\n❌ ПРОБЛЕМА: Сигналы генерируются, но блокируются фильтрами!")
        print("\n💡 Рекомендации:")

        conf_blocked = [s for s in signals_by_status["blocked"] if "Confidence" in str(s["issues"])]
        if conf_blocked:
            print(f"   - {len(conf_blocked)} сигналов заблокированы из-за низкой confidence")
            print(f"   - Текущий порог: {CONF_THRESHOLD:.1%}")
            print("   - Рассмотрите снижение CRYPTO_SIGNAL_MIN_CONF")

        ml_blocked = [s for s in signals_by_status["blocked"] if "ML confirm" in str(s["issues"])]
        if ml_blocked:
            print(f"   - {len(ml_blocked)} сигналов заблокированы ML confirm gate")
            print("   - Проверьте настройки ML_CONFIRM_MODE и конфигурацию модели")
    elif not all_signals:
        print("\n❌ ПРОБЛЕМА: Сигналы не генерируются вообще!")
        print("\n💡 Рекомендации:")
        print("   1. Проверьте, что CryptoOrderflowService обрабатывает XAUUSDT")
        print("   2. Проверьте логи CryptoOrderflowService на ошибки")
        print("   3. Проверьте, что тики приходят для XAUUSDT (stream:tick_XAUUSDT)")
        print("   4. Проверьте конфигурацию orderflow для XAUUSDT (config:orderflow:XAUUSDT)")


if __name__ == "__main__":
    main()

