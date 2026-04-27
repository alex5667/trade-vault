#!/usr/bin/env python3
"""
Демонстрация полного цикла: парсинг → Redis → отправка в бот
"""

import sys
import os
import time
import redis
from datetime import datetime

# Добавляем путь к модулям приложения
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.parse_utils import parse_signal

def clean_for_redis(data):
    """Очищает данные для отправки в Redis"""
    cleaned = {}
    for key, value in data.items():
        if value is not None:
            if isinstance(value, list):
                cleaned[key] = ','.join(map(str, value))
            else:
                cleaned[key] = str(value)
    return cleaned

def format_price(value):
    """Форматирует цену, избегая научной нотации."""
    if value in ["-", None, "", "N/A"]:
        return "N/A"

    try:
        # Конвертируем в float
        price = float(value)

        # Определяем количество знаков после запятой
        if price >= 1000:
            # Для больших чисел: 1234.56
            return f"{price:.2f}"
        elif price >= 1:
            # Для обычных чисел: 12.345
            return f"{price:.3f}"
        elif price >= 0.01:
            # Для малых чисел: 0.12345
            return f"{price:.5f}"
        elif price >= 0.0001:
            # Для очень малых чисел: 0.001234
            return f"{price:.6f}"
        else:
            # Для экстремально малых чисел: 0.00005879
            return f"{price:.8f}".rstrip('0').rstrip('.')
    except (ValueError, TypeError):
        return str(value)

def format_telegram_message(fields):
    """Форматирует сообщение для отправки в Telegram бот"""

    symbol = fields.get('symbol', 'N/A')
    direction = fields.get('direction', 'N/A')
    entry = fields.get('entry', 'N/A')
    stop = fields.get('stop', 'N/A')
    tp = fields.get('tp', 'N/A')
    leverage = fields.get('leverage', 'N/A')
    order_type = fields.get('orderType', 'N/A')
    profit_pct = fields.get('profitPct', 'N/A')
    exchange = fields.get('exchange', 'N/A')
    channel = fields.get('channel') or fields.get('username') or fields.get('chat_title') or 'Unknown Channel'

    # Эмодзи для направления
    direction_emoji = "🟢" if direction == "LONG" else "🔴"

    # Форматируем цели (избегаем научной нотации)
    if tp and tp != 'N/A':
        tp_list = tp.split(',') if isinstance(tp, str) else tp
        tp_formatted = " | ".join([f"{format_price(t.strip() if isinstance(t, str) else t)}$" for t in tp_list])
    else:
        tp_formatted = "N/A"

    # Форматируем stop и entry (избегаем научной нотации)
    stop_formatted = format_price(stop)
    entry_formatted = format_price(entry)

    message = f"""🚨 ТОРГОВЫЙ СИГНАЛ

{direction_emoji} {direction} {symbol}
💰 Вход: {entry_formatted}$ ({leverage}x)
🎯 Цели: {tp_formatted}
🛑 Стоп: {stop_formatted}$
📈 Потенциал: {profit_pct}%
🏢 {exchange} | {order_type}

📺 Канал: {channel}
⏰ {datetime.now().strftime('%H:%M:%S')}"""

    return message

def demo_full_cycle():
    """Демонстрирует полный цикл обработки сигнала"""

    print("🎬 ДЕМОНСТРАЦИЯ ПОЛНОГО ЦИКЛА ОБРАБОТКИ СИГНАЛОВ")
    print("=" * 60)

    # Подключаемся к Redis
    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_client.ping()
        print("✅ Redis подключен")
    except Exception as e:
        print(f"❌ Ошибка подключения к Redis: {e}")
        return

    # Ваш сигнал
    signal_text = """#ICP/USDT | SHORT ⬇️
[Фьючерсы 19x плечо]

⚪️Точка входа: 4.7$
⚪️Тип ордера: Рыночный ордер
⚪️Отгрызаем профит на: 4.627$ 4.55$ 4.506$
⚪️Cтоп: 5.046$

Потенциальная прибыль когда догрызем последний тейк будет = +78% ✅

Торгуем (https://partner.bybit.com/b/aff_7_91692?affiliate_id=91692&group_id=887144&group_type=1) строго по правилам и не завышаем плечи, цели и стоп ставим строго те которые указаны в сигнале 🤝
➖➖➖➖➖➖➖➖➖➖
Торгуй с Бобровскими на Bybit 🦫: https://partner.bybit.com/b/aff_7_91692?affiliate_id=91692&group_id=887144&group_type=1

Зарабатывай еще больше в
приватке
📈: https://tinyurl.com/BoberPrivate"""

    print("📨 ШАГ 1: Получение сообщения из Telegram канала")
    print("-" * 50)
    print("Канал: Trading Signals (@trading_signals)")
    print(f"Время: {datetime.now().strftime('%H:%M:%S')}")
    print(f"Сообщение: {signal_text[:100]}...")
    print()

    print("🔍 ШАГ 2: Парсинг сигнала")
    print("-" * 50)

    # Парсим сигнал
    parsed = parse_signal(signal_text)

    # Добавляем метаданные
    chat_id = "-1001234567890"
    chat_title = "Trading Signals"
    username = "@trading_signals"
    msg_id = "12345"
    timestamp = int(time.time() * 1000)

    parsed.update({
        "chat_id": str(chat_id),
        "chat_title": chat_title,
        "username": username or "",
        "channel": username or chat_title or "Unknown Channel",
        "msg_id": str(msg_id),
        "timestamp": str(timestamp),
    })

    print("✅ Результат парсинга:")
    print(f"   Символ: {parsed.get('symbol')}")
    print(f"   Направление: {parsed.get('direction')}")
    print(f"   Вход: {parsed.get('entry')}")
    print(f"   Стоп: {parsed.get('stop')}")
    print(f"   Цели: {parsed.get('tp')}")
    print(f"   Плечо: {parsed.get('leverage')}")
    print(f"   Уверенность: {parsed.get('confidence')}")
    print()

    # Проверяем обязательные поля
    has_direction = bool(parsed.get("direction"))
    has_entry = (parsed.get("entry") is not None)
    has_stop = parsed.get("stop") is not None
    has_tp = isinstance(parsed.get("tp"), list) and len(parsed.get("tp", [])) > 0

    if not (has_direction and has_entry and has_stop and has_tp):
        print("❌ Сигнал неполный, пропускаем")
        return

    print("📤 ШАГ 3: Отправка в Redis Streams")
    print("-" * 50)

    # Сырое сообщение
    raw_message = {
        "chat_id": str(chat_id),
        "chat_title": chat_title,
        "username": username or "",
        "msg_id": str(msg_id),
        "timestamp": str(timestamp),
        "text": signal_text,
    }

    # Отправляем в raw stream
    raw_stream_id = redis_client.xadd("signal:telegram:raw", raw_message)
    print(f"✅ Сырое сообщение → signal:telegram:raw ({raw_stream_id})")

    # Очищаем данные для parsed stream
    flat = clean_for_redis(parsed)

    # Отправляем в parsed stream
    parsed_stream_id = redis_client.xadd("signal:telegram:parsed", flat)
    print(f"✅ Распарсенный сигнал → signal:telegram:parsed ({parsed_stream_id})")
    print()

    print("🤖 ШАГ 4: Notify Worker обрабатывает сигнал")
    print("-" * 50)

    # Читаем из parsed stream
    messages = redis_client.xread({"signal:telegram:parsed": "$"}, count=1, block=1000)

    if messages:
        stream_name, stream_messages = messages[0]
        for msg_id, fields in stream_messages:
            print(f"📨 Получен сигнал из {stream_name}: {msg_id}")

            # Форматируем сообщение для бота
            bot_message = format_telegram_message(fields)

            print("📱 ШАГ 5: Отправка в Telegram бот")
            print("-" * 50)
            print("🤖 TELEGRAM BOT API CALL:")
            print("=" * 50)
            print("URL: https://api.telegram.org/bot<TOKEN>/sendMessage")
            print("Chat ID: <CHAT_ID>")
            print("Message:")
            print("-" * 30)
            print(bot_message)
            print("-" * 30)
            print("✅ Сообщение отправлено в бот!")
            print("=" * 50)

    print()
    print("🎉 ПОЛНЫЙ ЦИКЛ ЗАВЕРШЕН УСПЕШНО!")
    print("=" * 60)
    print("📊 Статистика:")
    print(f"   Raw messages: {redis_client.xlen('signal:telegram:raw')}")
    print(f"   Parsed messages: {redis_client.xlen('signal:telegram:parsed')}")
    print("=" * 60)

if __name__ == "__main__":
    demo_full_cycle()
