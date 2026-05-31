#!/usr/bin/env python3
"""
Unified Signal Reader - объединяет XAUUSD сигналы и сигналы из телеграм-каналов
в единый поток для телеграм-бота.

ИСТОЧНИКИ СИГНАЛОВ:
1. notify:telegram - XAUUSD сигналы от xau_orderflow_handler.py
2. signal:telegram:parsed - Парсированные сигналы из телеграм-каналов (крипто, золото и т.д.)

НАЗНАЧЕНИЕ:
- Читает сигналы из обеих источников
- Форматирует их в единый стандарт
- Отправляет в телеграм-бот
"""

import os
import json
import time
import redis
import requests
import asyncio
from datetime import datetime, timezone

# Конфигурация
REDIS_URL = os.getenv("REDIS_URL", "redis://scanner-redis-worker-1:6379/0")
XAUUSD_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")  # XAUUSD сигналы
CHANNELS_STREAM = os.getenv("PARSED_STREAM", "signal:telegram:parsed")  # Парсированные сигналы каналов
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_NOTIFY_CHAT_IDS")

# Consumer groups (разные для каждого источника)
XAUUSD_GROUP = "unified-xauusd-group"
CHANNELS_GROUP = "unified-channels-group"
CONSUMER_NAME = "unified-signal-reader"

# Глобальные счетчики для логирования
message_count = 0
LOG_INTERVAL = 10  # Логируем каждое 10-е сообщение  # noqa: RUF003

print("🚀 Unified Signal Reader запуск...")
print(f"   Redis: {REDIS_URL}")
print(f"   XAUUSD Stream: {XAUUSD_STREAM}")
print(f"   Channels Stream: {CHANNELS_STREAM}")
print(f"   Bot Token: {'✅' if TELEGRAM_BOT_TOKEN else '❌'}")
print(f"   Chat ID: {TELEGRAM_CHAT_ID}")

# Redis клиент
r = redis.from_url(REDIS_URL, decode_responses=True)

def send_telegram_message(text: str) -> bool:
    """Отправляет сообщение в Telegram"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ Нет токена или chat_id")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }

    try:
        response = requests.post(url, data=data, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Telegram exception: {e}")
        return False

def format_xauusd_signal(fields: dict[str, str]) -> str | None:
    """
    Форматирует XAUUSD сигнал от xau_orderflow_handler.py

    Args:
        fields: Поля сообщения из Redis stream

    Returns:
        Отформатированное сообщение или None если невалидно
    """
    try:
        # Проверяем что это действительно XAUUSD сигнал
        text = fields.get('text', '')
        if 'XAUUSD' not in text:
            return None

        # Извлекаем данные
        side = fields.get('side', '')
        price = fields.get('price', '')
        lot = fields.get('lot', '')
        note = fields.get('note', '')

        # Проверяем обязательные поля
        if not all([side, price, note]):
            return None

        # Парсим risk данные если есть
        risk_str = fields.get('risk', '{}')
        try:
            risk = json.loads(risk_str) if isinstance(risk_str, str) else risk_str
        except (json.JSONDecodeError, ValueError, TypeError):
            risk = {}

        sl = risk.get('sl', '-')
        tp_levels = risk.get('tp_levels', [])

        # Форматируем TP
        if tp_levels:
            tp_str = " | ".join([f"TP{i+1}: {tp:.2f}" for i, tp in enumerate(tp_levels[:3])])
        else:
            tp_str = "-"

        # Определяем эмодзи для направления
        direction_emoji = "🟢" if side.upper() == "LONG" else "🔴"
        signal_emoji = "🎯" if "Breakout" in note else "🛡️"

        # Формируем сообщение
        message = f"""{signal_emoji} XAUUSD СИГНАЛ

{direction_emoji} {side} @ {price}$
📊 Лот: {lot}
🎯 {tp_str}
🛑 SL: {sl}$

💡 {note}
🤖 Система: Order Flow Analysis
⏰ {datetime.now(timezone.utc).strftime("%H:%M:%S %d.%m.%Y UTC")}"""

        return message

    except Exception as e:
        print(f"❌ Ошибка форматирования XAUUSD сигнала: {e}")
        return None

def format_channel_signal(fields: dict[str, str]) -> str | None:
    """
    Форматирует сигнал из телеграм-канала

    Args:
        fields: Поля сообщения из Redis stream

    Returns:
        Отформатированное сообщение или None если невалидно
    """
    try:
        # Извлекаем основные данные
        symbol = fields.get('symbol', '')
        direction = fields.get('direction', '')
        entry = fields.get('entry', '')
        stop = fields.get('stop', '')

        # Проверяем обязательные поля
        if not all([symbol, direction, entry]):
            return None

        # Извлекаем дополнительные данные
        tp_str = fields.get('tp', '')
        leverage = fields.get('leverage', '-')
        source = fields.get('source', fields.get('username', fields.get('chat_title', 'Unknown')))
        confidence = fields.get('confidence', '-')
        exchange = fields.get('exchange', '-')

        # Парсим TP
        try:
            if tp_str.startswith('[') and tp_str.endswith(']'):
                tp_list = json.loads(tp_str)
            else:
                tp_list = [float(x.strip()) for x in tp_str.split(",") if x.strip()]
        except (json.JSONDecodeError, ValueError, AttributeError):
            tp_list = []

        # Форматируем TP
        if tp_list:
            tp_formatted = " | ".join([f"TP{i+1}: {tp:.4f}" for i, tp in enumerate(tp_list[:3])])
        else:
            tp_formatted = "-"

        # Определяем эмодзи
        direction_emoji = "🟢" if direction.upper() in ["LONG", "BUY"] else "🔴"

        # Определяем тип инструмента по символу
        if any(crypto in symbol.upper() for crypto in ['BTC', 'ETH', 'XRP', 'ADA', 'DOT', 'USDT', 'USDC']):
            instrument_emoji = "₿"
        elif any(gold in symbol.upper() for gold in ['XAU', 'GOLD']):
            instrument_emoji = "🥇"
        elif any(forex in symbol.upper() for forex in ['USD', 'EUR', 'GBP', 'JPY']):
            instrument_emoji = "💱"
        else:
            instrument_emoji = "📈"

        # Формируем сообщение
        message = f"""{instrument_emoji} ТОРГОВЫЙ СИГНАЛ

{direction_emoji} {direction} {symbol}
💰 Вход: {entry}$ ({leverage}x)
🎯 {tp_formatted}
🛑 Стоп: {stop}$
🏢 {exchange}

📺 Канал: {source}
⭐ Confidence: {confidence}
⏰ {datetime.now(timezone.utc).strftime("%H:%M:%S %d.%m.%Y UTC")}"""

        return message

    except Exception as e:
        print(f"❌ Ошибка форматирования канального сигнала: {e}")
        return None

def create_consumer_groups():
    """Создает consumer groups для обоих стримов"""
    for stream, group in [(XAUUSD_STREAM, XAUUSD_GROUP), (CHANNELS_STREAM, CHANNELS_GROUP)]:
        try:
            r.xgroup_create(stream, group, id='0', mkstream=True)
            print(f"✅ Consumer group создана: {group} для {stream}")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                print(f"✅ Consumer group уже существует: {group}")
            else:
                print(f"❌ Ошибка создания group {group}: {e}")

def process_message(stream: str, msg_id: str, fields: dict[str, str]) -> bool:
    """
    Обрабатывает одно сообщение из любого стрима

    Returns:
        True если сообщение успешно обработано
    """
    global message_count
    message_count += 1

    # Логируем только каждое N-е сообщение  # noqa: RUF003
    if message_count % LOG_INTERVAL == 0:
        print(f"📨 Обработка сообщения #{message_count} из {stream}")

    # Определяем тип сигнала и форматируем
    if stream == XAUUSD_STREAM:
        # XAUUSD сигнал от order flow handler
        message = format_xauusd_signal(fields)
        signal_type = "XAUUSD"
    elif stream == CHANNELS_STREAM:
        # Сигнал из телеграм-канала
        message = format_channel_signal(fields)
        signal_type = "CHANNEL"
    else:
        print(f"❌ Неизвестный stream: {stream}")
        return False

    if not message:
        if message_count % LOG_INTERVAL == 0:
            print(f"⚠️ Невалидный {signal_type} сигнал: {msg_id}")
        return True  # Считаем успешно обработанным чтобы не застревало

    # Отправляем в Telegram
    success = send_telegram_message(message)

    if success:
        if message_count % LOG_INTERVAL == 0:
            print(f"✅ {signal_type} сигнал отправлен: {msg_id}")
        return True
    else:
        print(f"❌ Не удалось отправить {signal_type} сигнал: {msg_id}")  # noqa: RUF001
        return False

async def read_stream_messages(stream: str, group: str) -> int:
    """
    Читает сообщения из одного стрима

    Returns:
        Количество обработанных сообщений
    """
    try:
        messages = r.xreadgroup(
            group,
            CONSUMER_NAME,
            {stream: '>'},
            count=5,  # Читаем по 5 сообщений за раз
            block=1000  # 1 секунда таймаут
        )

        if not messages:
            return 0

        processed = 0
        for stream_name, items in messages:
            for msg_id, fields in items:
                try:
                    process_message(stream_name, msg_id, fields)

                    # Всегда подтверждаем обработку
                    r.xack(stream_name, group, msg_id)
                    processed += 1

                except Exception as e:
                    print(f"❌ Ошибка обработки сообщения {msg_id}: {e}")
                    # Подтверждаем даже при ошибке чтобы не зациклиться
                    r.xack(stream_name, group, msg_id)

        return processed

    except Exception as e:
        print(f"❌ Ошибка чтения из {stream}: {e}")
        return 0

async def main():
    """Главный цикл unified signal reader"""
    print("🔄 Создание consumer groups...")
    create_consumer_groups()

    print("🔄 Начинаю чтение сигналов из обеих источников...")
    print(f"   📊 Логирование каждого {LOG_INTERVAL}-го сообщения")  # noqa: RUF001

    # Статистика
    last_stats_time = time.time()
    total_processed = 0
    xauusd_processed = 0
    channels_processed = 0

    while True:
        try:
            # Читаем из обеих стримов параллельно
            xauusd_count = await read_stream_messages(XAUUSD_STREAM, XAUUSD_GROUP)
            channels_count = await read_stream_messages(CHANNELS_STREAM, CHANNELS_GROUP)

            # Обновляем статистику
            total_processed += xauusd_count + channels_count
            xauusd_processed += xauusd_count
            channels_processed += channels_count

            # Выводим статистику каждые 60 секунд
            current_time = time.time()
            if current_time - last_stats_time >= 60:
                print("\n📊 СТАТИСТИКА (за минуту):")
                print(f"   🎯 XAUUSD сигналов: {xauusd_processed}")
                print(f"   📺 Канальных сигналов: {channels_processed}")
                print(f"   📈 Всего: {total_processed}")  # noqa: RUF001
                print(f"   ⏰ Время: {datetime.now().strftime('%H:%M:%S')}\n")

                # Сброс счетчиков  # noqa: RUF003
                last_stats_time = current_time
                xauusd_processed = 0
                channels_processed = 0

            # Небольшая пауза если нет сообщений
            if xauusd_count == 0 and channels_count == 0:
                await asyncio.sleep(1)

        except KeyboardInterrupt:
            print("🛑 Получен сигнал остановки")
            break
        except Exception as e:
            print(f"❌ Ошибка в главном цикле: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("⛔ Unified Signal Reader остановлен")
