#!/usr/bin/env python3
"""
Скрипт для исправления проблем с сигналами CryptoOrderFlow.
- Проверяет настройки
- Временно снижает пороги для тестирования
- Проверяет цепочку публикации
"""

import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import redis
except ImportError:
    print("❌ redis-py не установлен")
    sys.exit(1)

def main():
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)
    
    print("=" * 80)
    print("🔧 ИСПРАВЛЕНИЕ ПРОБЛЕМ С СИГНАЛАМИ")
    print("=" * 80)
    
    # 1. Проверяем последние сигналы
    print("\n1️⃣ Проверка последних сигналов...")
    raw_entries = r.xrevrange("signals:crypto:raw", max="+", min="-", count=3)
    if raw_entries:
        latest = raw_entries[0]
        msg_id, fields = latest
        payload_str = fields.get("payload", "{}")
        try:
            payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
            signal_ts = payload.get("generated_at") or payload.get("tick_ts")
            symbol = payload.get("symbol", "N/A")
            direction = payload.get("direction", "N/A")
            confidence = payload.get("confidence", 0.0)
            print(f"   Последний сигнал: {symbol} {direction} | conf={confidence:.2%} | ts={signal_ts}")
        except Exception as e:
            print(f"   Ошибка парсинга: {e}")
    else:
        print("   ❌ Сигналы не найдены")
    
    # 2. Проверяем Telegram stream
    print("\n2️⃣ Проверка Telegram stream...")
    telegram_entries = r.xrevrange("notify:telegram", max="+", min="-", count=10)
    crypto_signals_in_telegram = 0
    for msg_id, fields in telegram_entries:
        text = fields.get("text", "")
        source = fields.get("source", "")
        if "CryptoOrderFlow" in text or source == "CryptoOrderFlow" or "direction" in fields:
            crypto_signals_in_telegram += 1
    
    print(f"   Найдено сигналов CryptoOrderFlow в Telegram: {crypto_signals_in_telegram}")
    
    if crypto_signals_in_telegram == 0:
        print("   ⚠️ Сигналы не попадают в Telegram!")
        print("   Возможные причины:")
        print("     1. Гейтинг every_n=3 пропускает сигналы")
        print("     2. Ошибки публикации в Redis")
        print("     3. Сигналы публикуются в другой stream")
    
    # 3. Проверяем счетчик гейтинга
    print("\n3️⃣ Проверка гейтинга...")
    counter = r.get("notify:telegram:signal_counter")
    every_n = 3  # из docker-compose
    if counter:
        counter_int = int(counter)
        remainder = counter_int % every_n
        print(f"   Счетчик: {counter_int}")
        print(f"   Every N: {every_n}")
        print(f"   Остаток: {remainder}")
        if remainder != 0:
            print(f"   ⚠️ Следующий сигнал будет пропущен (остаток {remainder} != 0)")
        else:
            print(f"   ✅ Следующий сигнал пройдет в Telegram")
    
    # 4. Рекомендации
    print("\n" + "=" * 80)
    print("📋 РЕКОМЕНДАЦИИ")
    print("=" * 80)
    
    if crypto_signals_in_telegram == 0:
        print("\n🔧 Для тестирования:")
        print("   1. Временно установите CRYPTO_NOTIFY_SIGNAL_EVERY_N=1")
        print("   2. Снизьте пороги для более частых сигналов:")
        print("      - BTC_DELTA_Z_THRESHOLD=2.0")
        print("      - CRYPTO_SIGNAL_MIN_CONF=80")
        print("   3. Перезапустите сервис")
    
    print("\n✅ Проверка завершена")

if __name__ == "__main__":
    main()

