#!/usr/bin/env python3
"""
Скрипт для отправки тестового торгового сигнала в Telegram бот через Redis.
"""

import sys
import os
import json
import time
from datetime import datetime

# Импорт парсера
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'telegram-worker'))
from app.parse_utils import parse_signal

# Попытка подключения к Redis
try:
    import redis
except ImportError:
    print("❌ Модуль redis не установлен. Установите: pip install redis")
    sys.exit(1)

# Исходное сообщение
TEST_MESSAGE = """INJ USDT Long 📈

🧠 Плечо: до 25
Соблюдаем риск менеджмент

💲 Точка входа: 8,553 - 7,815
✅ Тейк: 9,714
❌ Стоп: 7,495 (смотря откуда вы заходили)

❗ Не финансовый совет.

👀 Накидайте реакций, если хотите больше сетапов.

@trademansi0n | @tmanalytics"""


def connect_redis(host='localhost', port=6379):
    """Подключается к Redis."""
    try:
        r = redis.Redis(host=host, port=port, decode_responses=True)
        r.ping()
        print(f"✅ Подключение к Redis: {host}:{port} успешно")
        return r
    except Exception as e:
        print(f"❌ Не удалось подключиться к Redis {host}:{port}: {e}")
        return None


def format_bot_message(parsed: dict) -> str:
    """Форматирует сообщение для отправки в Telegram бот."""
    lines = []
    lines.append("🚨 НОВЫЙ ТОРГОВЫЙ СИГНАЛ")
    lines.append("")

    symbol = parsed.get('symbol', 'N/A')
    direction = parsed.get('direction', 'N/A')
    direction_emoji = "📈" if direction == "LONG" else "📉" if direction == "SHORT" else "📊"

    lines.append(f"{direction_emoji} **{symbol} {direction}**")
    lines.append("")

    leverage = parsed.get('leverage')
    if leverage:
        lines.append(f"⚡ **Плечо**: {leverage}x")

    entry = parsed.get('entry')
    if entry:
        lines.append(f"💰 **Вход**: {entry}")

    tp = parsed.get('tp', [])
    if tp:
        lines.append("")
        lines.append("✅ **Take Profit**:")
        for i, price in enumerate(tp, 1):
            lines.append(f"   TP{i}: {price}")

    stop = parsed.get('stop')
    if stop:
        lines.append("")
        lines.append(f"❌ **Stop Loss**: {stop}")

    # Дополнительная информация
    if parsed.get('orderType'):
        lines.append("")
        lines.append(f"📝 Тип ордера: {parsed.get('orderType')}")

    if parsed.get('profitPct'):
        lines.append(f"💹 Потенциальная прибыль: {parsed.get('profitPct')}%")

    # Метаданные
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    channel = parsed.get('channel') or parsed.get('username') or parsed.get('source') or 'Unknown Channel'
    lines.append(f"📡 Канал: {channel}")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"⏰ {timestamp}")

    confidence = parsed.get('confidence', 0.0)
    confidence_pct = int(confidence * 100)
    lines.append(f"🎯 Уверенность: {confidence_pct}%")

    return "\n".join(lines)


def send_to_redis_raw(r: redis.Redis, message_text: str, stream='telegram:raw'):
    """Отправляет сырое сообщение в Redis stream."""
    try:
        timestamp = int(time.time() * 1000)

        data = {
            "chat_id": "test_channel",
            "chat_title": "Test Channel",
            "username": "@trademansi0n",
            "msg_id": f"test_{timestamp}",
            "timestamp": str(timestamp),
            "text": message_text,
            "thread_group": "test_group"
        }

        msg_id = r.xadd(stream, data)
        print(f"✅ Сырое сообщение отправлено в stream '{stream}'")
        print(f"   Message ID: {msg_id}")
        return msg_id
    except Exception as e:
        print(f"❌ Ошибка при отправке в Redis stream: {e}")
        return None


def send_to_redis_parsed(r: redis.Redis, parsed: dict, stream='telegram:parsed'):
    """Отправляет распарсенный сигнал в Redis stream."""
    try:
        timestamp = int(time.time() * 1000)

        # Добавляем метаданные
        parsed.update({
            "chat_id": "test_channel",
            "chat_title": "Test Channel",
            "username": "@trademansi0n",
            "channel": "@trademansi0n",
            "msg_id": f"test_{timestamp}",
            "timestamp": str(timestamp),
            "thread_group": "test_group"
        })

        # Конвертируем все в строки для Redis
        data = {}
        for key, value in parsed.items():
            if isinstance(value, (list, dict)):
                data[key] = json.dumps(value, ensure_ascii=False)
            elif value is None:
                data[key] = ""
            else:
                data[key] = str(value)

        msg_id = r.xadd(stream, data)
        print(f"✅ Распарсенный сигнал отправлен в stream '{stream}'")
        print(f"   Message ID: {msg_id}")
        return msg_id
    except Exception as e:
        print(f"❌ Ошибка при отправке распарсенного сигнала: {e}")
        import traceback
        traceback.print_exc()
        return None


def send_to_telegram_bot(parsed: dict):
    """Отправляет сообщение напрямую в Telegram бот (если настроен)."""
    try:
        # Здесь можно добавить логику отправки через Telegram Bot API
        # Пока просто выводим форматированное сообщение
        print("\n" + "="*80)
        print("📤 СООБЩЕНИЕ ДЛЯ TELEGRAM БОТА:")
        print("="*80)
        bot_message = format_bot_message(parsed)
        print(bot_message)
        print("="*80)

        return True
    except Exception as e:
        print(f"❌ Ошибка при подготовке сообщения для бота: {e}")
        return False


def main():
    print("\n" + "="*80)
    print("📨 ОТПРАВКА ТЕСТОВОГО СИГНАЛА В БОТ")
    print("="*80)

    # 1. Парсим сообщение
    print("\n1️⃣ Парсинг сообщения...")
    parsed = parse_signal(TEST_MESSAGE)

    print("\n✅ Парсинг завершен:")
    print(f"   Символ: {parsed.get('symbol')}")
    print(f"   Направление: {parsed.get('direction')}")
    print(f"   Плечо: {parsed.get('leverage')}x")
    print(f"   Вход: {parsed.get('entry')}")
    print(f"   TP: {parsed.get('tp')}")
    print(f"   Stop: {parsed.get('stop')}")
    print(f"   Уверенность: {int(parsed.get('confidence', 0) * 100)}%")

    # 2. Подключаемся к Redis
    print("\n2️⃣ Подключение к Redis...")
    redis_hosts = [
        ('localhost', 6379),
        ('localhost', 6380),
        ('localhost', 6381),
    ]

    r = None
    for host, port in redis_hosts:
        r = connect_redis(host, port)
        if r:
            break

    if not r:
        print("\n⚠️ Redis недоступен. Пропускаем отправку в Redis.")
        print("   Убедитесь, что Redis запущен: docker-compose up -d redis")
    else:
        # 3. Отправляем сырое сообщение
        print("\n3️⃣ Отправка сырого сообщения в Redis...")
        raw_id = send_to_redis_raw(r, TEST_MESSAGE)

        # 4. Отправляем распарсенный сигнал
        print("\n4️⃣ Отправка распарсенного сигнала в Redis...")
        parsed_id = send_to_redis_parsed(r, parsed)

        # 5. Проверяем отправку
        if raw_id and parsed_id:
            print("\n✅ Сообщение успешно отправлено в оба потока!")

            # Проверяем наличие сообщений в потоках
            print("\n📊 Проверка Redis streams:")
            try:
                raw_count = r.xlen('telegram:raw')
                parsed_count = r.xlen('telegram:parsed')
                print(f"   telegram:raw: {raw_count} сообщений")
                print(f"   telegram:parsed: {parsed_count} сообщений")
            except Exception as e:
                print(f"   ⚠️ Не удалось получить статистику: {e}")

    # 6. Форматируем сообщение для Telegram бота
    print("\n5️⃣ Форматирование сообщения для Telegram бота...")
    send_to_telegram_bot(parsed)

    # 7. Сохраняем результат
    output_file = "/home/alex/front/trade/scanner_infra/test_signal_sent.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)

    print(f"\n💾 Результат сохранен в: {output_file}")

    print("\n" + "="*80)
    print("✅ ТЕСТ ЗАВЕРШЕН")
    print("="*80)
    print("\n💡 Если Redis работает, сообщение должно быть обработано telegram-worker")
    print("   и отправлено в Telegram бот через notify-worker.")
    print("\n🔍 Проверьте логи:")
    print("   docker-compose logs -f telegram-worker")
    print("   docker-compose logs -f notify-worker")
    print("\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())

