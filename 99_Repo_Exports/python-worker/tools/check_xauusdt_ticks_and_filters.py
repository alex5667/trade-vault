#!/usr/bin/env python3
"""
Проверка обработки тиков XAUUSDT и фильтров, которые режут сделки.

Проверяет:
1. Приходят ли тики в stream:tick_XAUUSDT
2. Обрабатываются ли тики сервисом
3. Какие фильтры блокируют сделки (ATR gate, cooldown, confidence и т.д.)
"""

import json
import os
from typing import Any

import redis
from core.redis_keys import RedisStreams as RS

# Добавляем путь к корню проекта
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

SYMBOL = "XAUUSDT"
TICK_STREAM = f"stream:tick_{SYMBOL}"
CONFIG_KEY = f"config:orderflow:{SYMBOL}"
ATR_KEY = f"atr:{SYMBOL}:1m"

def get_redis_client():
    """Подключение к Redis."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.from_url(redis_url, decode_responses=True)

def check_ticks_stream(r: redis.Redis) -> dict[str, Any]:
    """Проверка наличия тиков в stream."""
    print(f"\n1️⃣ Проверка тиков в {TICK_STREAM}:")

    try:
        length = r.xlen(TICK_STREAM)
        print(f"  ✅ Stream существует, записей: {length:,}")

        if length > 0:
            # Получаем последние 5 записей
            entries = r.xrevrange(TICK_STREAM, count=5)
            if entries:
                print("  ✅ Последние тики:")
                for i, (msg_id, fields) in enumerate(entries[:3], 1):
                    data_str = fields.get("data", "{}")
                    try:
                        data = json.loads(data_str)
                        ts = data.get("ts", 0)
                        bid = data.get("bid", 0)
                        ask = data.get("ask", 0)
                        last = data.get("last", 0)
                        print(f"    {i}. msg_id={msg_id[:20]}... ts={ts} bid={bid:.2f} ask={ask:.2f} last={last:.2f}")
                    except Exception:
                        print(f"    {i}. msg_id={msg_id[:20]}... (не удалось распарсить)")
        else:
            print("  ⚠️ Stream пуст!")

        return {"exists": True, "length": length}
    except Exception as e:
        print(f"  ❌ Ошибка: {e}")
        return {"exists": False, "error": str(e)}

def check_atr_value(r: redis.Redis) -> dict[str, Any]:
    """Проверка значения ATR."""
    print(f"\n2️⃣ Проверка ATR ({ATR_KEY}):")

    try:
        atr_str = r.get(ATR_KEY)
        if atr_str:
            atr = float(atr_str)
            print(f"  ✅ ATR (1m): {atr:.4f}")
            return {"exists": True, "value": atr}
        else:
            print("  ⚠️ ATR не найден в Redis")
            return {"exists": False}
    except Exception as e:
        print(f"  ❌ Ошибка: {e}")
        return {"exists": False, "error": str(e)}

def check_config(r: redis.Redis) -> dict[str, Any]:
    """Проверка конфигурации orderflow для XAUUSDT."""
    print(f"\n3️⃣ Проверка конфигурации ({CONFIG_KEY}):")

    try:
        config = r.hgetall(CONFIG_KEY)
        if config:
            print(f"  ✅ Конфигурация найдена ({len(config)} параметров):")

            # Ключевые параметры для фильтров
            key_params = [
                "atr_gate_audit_only",
                "atr_bps_min_static",
                "signal_cooldown_sec",
                "min_signal_confidence",
                "disable_confidence_filter",
                "exec_risk_ref_bps",
                "of_score_min",
            ]

            for param in key_params:
                value = config.get(param)
                if value is not None:
                    print(f"    - {param}: {value}")

            # Показываем все параметры, если их немного
            if len(config) <= 20:
                print("  📋 Все параметры:")
                for k, v in sorted(config.items()):
                    print(f"    - {k}: {v}")

            return {"exists": True, "config": config}
        else:
            print("  ⚠️ Конфигурация не найдена (используются дефолты)")
            return {"exists": False}
    except Exception as e:
        print(f"  ❌ Ошибка: {e}")
        return {"exists": False, "error": str(e)}

def check_recent_signals(r: redis.Redis) -> dict[str, Any]:
    """Проверка последних сигналов для XAUUSDT."""
    print("\n4️⃣ Проверка последних сигналов:")

    streams_to_check = [
        RS.CRYPTO_RAW,
        f"signals:cryptoorderflow:{SYMBOL}",
        f"signals:aggregated:{SYMBOL}",
    ]

    results = {}
    for stream_name in streams_to_check:
        try:
            length = r.xlen(stream_name)
            if length > 0:
                # Проверяем последние записи на наличие XAUUSDT
                entries = r.xrevrange(stream_name, count=100)
                xauusdt_count = 0
                for msg_id, fields in entries:
                    symbol = fields.get("symbol", "")
                    if symbol == SYMBOL:
                        xauusdt_count += 1

                status = "✅" if xauusdt_count > 0 else "⚠️"
                print(f"  {status} {stream_name}: всего={length}, XAUUSDT={xauusdt_count}")
                results[stream_name] = {"total": length, "xauusdt": xauusdt_count}
            else:
                print(f"  ⚠️ {stream_name}: пуст")
                results[stream_name] = {"total": 0, "xauusdt": 0}
        except Exception as e:
            print(f"  ❌ {stream_name}: ошибка - {e}")
            results[stream_name] = {"error": str(e)}

    return results

def analyze_filters(config: dict[str, Any], atr_value: float | None) -> None:
    """Анализ фильтров, которые могут блокировать сделки."""
    print("\n5️⃣ Анализ фильтров, блокирующих сделки:")

    # 1. ATR Gate
    print("\n  🚦 ATR Gate:")
    atr_gate_audit_only = config.get("atr_gate_audit_only", "false")
    atr_bps_min_static = float(config.get("atr_bps_min_static", "0.0") or "0.0")

    if atr_gate_audit_only.lower() in ("true", "1", "yes"):
        print("    ✅ ATR gate в режиме AUDIT (не блокирует, только логирует)")
    else:
        print("    ⚠️ ATR gate в режиме ENFORCE (блокирует сигналы)")

    if atr_bps_min_static > 0:
        print(f"    📊 Статический минимум ATR: {atr_bps_min_static:.2f} bps")

    if atr_value:
        # Примерный расчет: если entry_price ~ 5000, то atr_bps = (atr / 5000) * 10000
        # Для XAUUSDT цена берется из реальных тиков
        # Золото на 2025 год торгуется около ~5000 USDT за унцию (примерная оценка)
        # Но реальная цена может быть любой - берется из тиков
        example_price = 5000.0  # Примерная цена для расчета (реальная берется из тиков)
        atr_bps_approx = (atr_value / example_price) * 10000.0
        print(f"    📊 Текущий ATR: {atr_value:.4f}")
        print(f"    📊 Примерный atr_bps (при цене {example_price:.2f}): {atr_bps_approx:.2f} bps")

        # Unified threshold обычно = max(atr_floor_th, fees_th)
        # Для XAUUSDT fees_th обычно ~ 12-15 bps, atr_floor_th зависит от regime
        fees_th_typical = 12.0  # Примерное значение
        print(f"    📊 Типичный unified threshold: ~{max(atr_bps_min_static, fees_th_typical):.2f} bps")

        if atr_bps_approx < max(atr_bps_min_static, fees_th_typical):
            print(f"    ❌ ПРОБЛЕМА: atr_bps ({atr_bps_approx:.2f}) < threshold (~{max(atr_bps_min_static, fees_th_typical):.2f})")
            print("       → ATR gate блокирует сигналы!")

    # 2. Cooldown
    print("\n  ⏱️ Cooldown:")
    cooldown_sec = int(config.get("signal_cooldown_sec", "3") or "3")
    print(f"    📊 Cooldown: {cooldown_sec} секунд")
    if cooldown_sec > 10:
        print("    ⚠️ Cooldown довольно большой, может буферизовать сигналы")

    # 3. Confidence filter
    print("\n  🎯 Confidence Filter:")
    min_confidence = float(config.get("min_signal_confidence", "70.0") or "70.0")
    disable_confidence = config.get("disable_confidence_filter", "false").lower() in ("true", "1", "yes")

    if disable_confidence:
        print("    ✅ Confidence filter отключен")
    else:
        print(f"    📊 Минимальный confidence: {min_confidence:.1f}%")

    # 4. OF Score
    print("\n  📈 OF Score:")
    of_score_min = float(config.get("of_score_min", "0.60") or "0.60")
    print(f"    📊 Минимальный OF score: {of_score_min:.2f}")

    # 5. Exec Risk
    print("\n  ⚠️ Exec Risk:")
    exec_risk_ref_bps = float(config.get("exec_risk_ref_bps", "12.0") or "12.0")
    print(f"    📊 Exec risk ref bps: {exec_risk_ref_bps:.2f}")

def main():
    """Основная функция."""
    print("=" * 80)
    print(f"Проверка обработки тиков и фильтров для {SYMBOL}")
    print("=" * 80)

    r = get_redis_client()

    # 1. Проверка тиков
    ticks_result = check_ticks_stream(r)

    # 2. Проверка ATR
    atr_result = check_atr_value(r)

    # 3. Проверка конфигурации
    config_result = check_config(r)
    config = config_result.get("config", {}) if config_result.get("exists") else {}

    # 4. Проверка сигналов
    signals_result = check_recent_signals(r)

    # 5. Анализ фильтров
    atr_value = atr_result.get("value") if atr_result.get("exists") else None
    analyze_filters(config, atr_value)

    # Итоговый вывод
    print("\n" + "=" * 80)
    print("📋 ИТОГОВАЯ СВОДКА:")
    print("=" * 80)

    if ticks_result.get("length", 0) > 0:
        print(f"✅ Тики приходят: {ticks_result['length']:,} записей в stream")
    else:
        print("❌ Тики НЕ приходят или stream пуст")

    if atr_result.get("exists"):
        print(f"✅ ATR доступен: {atr_result['value']:.4f}")
    else:
        print("⚠️ ATR не найден")

    if config_result.get("exists"):
        print(f"✅ Конфигурация найдена: {len(config)} параметров")
    else:
        print("⚠️ Конфигурация не найдена (используются дефолты)")

    # Рекомендации
    print("\n💡 РЕКОМЕНДАЦИИ:")

    if ticks_result.get("length", 0) == 0:
        print(f"  1. Проверьте, что тики записываются в {TICK_STREAM}")
        print("  2. Проверьте логи go-worker или tick-ingest сервиса")

    atr_gate_audit = config.get("atr_gate_audit_only", "false").lower() in ("true", "1", "yes")
    if not atr_gate_audit and atr_value:
        example_price = 2650.0
        atr_bps_approx = (atr_value / example_price) * 10000.0
        if atr_bps_approx < 15.0:  # Типичный порог
            print(f"  3. ATR gate может блокировать сигналы (atr_bps ≈ {atr_bps_approx:.2f} bps)")
            print("     Решение: установить atr_gate_audit_only=1 для XAUUSDT")
            print(f"     Команда: redis-cli HSET {CONFIG_KEY} atr_gate_audit_only 1")

    if not config_result.get("exists"):
        print("  4. Рекомендуется настроить конфигурацию для XAUUSDT")
        print(f"     Команда: redis-cli HSET {CONFIG_KEY} atr_gate_audit_only 1 signal_cooldown_sec 10")

if __name__ == "__main__":
    main()

