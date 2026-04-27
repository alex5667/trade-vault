#!/usr/bin/env python3
"""
Filtered Telegram Stream Reader - читает ТОЛЬКО XAUUSD сигналы из notify:telegram
"""
import os
import time
import redis
import requests

# Конфигурация
REDIS_URL = os.getenv("REDIS_URL", "redis://scanner-redis-worker-1:6379/0")
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_NOTIFY_CHAT_IDS")
CONSUMER_GROUP = "xauusd-signals-group"  # Отдельная группа для XAUUSD
CONSUMER_NAME = "xauusd-signals-sender"

print("🎯 Filtered Telegram Stream Reader запуск...")
print(f"   Redis: {REDIS_URL}")
print(f"   Stream: {NOTIFY_STREAM}")
print(f"   Группа: {CONSUMER_GROUP} (только XAUUSD сигналы)")
print(f"   Bot Token: {'✅' if TELEGRAM_BOT_TOKEN else '❌'}")
print(f"   Chat ID: {TELEGRAM_CHAT_ID}")

# Redis клиент
r = redis.from_url(REDIS_URL, decode_responses=True)

def is_xauusd_signal(fields):
    """Проверяет является ли сообщение XAUUSD сигналом от нашей системы"""
    try:
        # Проверяем по символу
        symbol = fields.get('symbol', '')
        if symbol and symbol == 'XAUUSD':
            return True

        # Проверяем по sid (наш формат: timestamp:SIDE:price)
        sid = fields.get('sid', '')
        if sid and ('LONG:' in sid or 'SHORT:' in sid):
            return True

        # Проверяем по тексту (содержит наши ключевые слова)
        text = fields.get('text', '')
        if 'XAUUSD' in text and ('Breakout' in text or 'Absorption' in text):
            return True

        # Проверяем по note (наши типы сигналов)
        note = fields.get('note', '')
        if note in ['Breakout (delta spike)', 'Absorption (weak progress + delta spike)']:
            return True

        return False

    except Exception as e:
        print(f"⚠️ Ошибка проверки фильтра: {e}")
        return False

def send_telegram_message(text):
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
        if response.status_code == 200:
            print("✅ Telegram: XAUUSD сигнал отправлен")
            return True
        else:
            print(f"❌ Telegram error: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Telegram exception: {e}")
        return False

# Создаём consumer group
try:
    r.xgroup_create(NOTIFY_STREAM, CONSUMER_GROUP, id='0', mkstream=True)
    print(f"✅ Consumer group создана: {CONSUMER_GROUP}")
except redis.exceptions.ResponseError as e:
    if "BUSYGROUP" in str(e):
        print(f"✅ Consumer group уже существует: {CONSUMER_GROUP}")
    else:
        print(f"❌ Ошибка создания group: {e}")

print(f"🔄 Начинаю чтение ТОЛЬКО XAUUSD сигналов из {NOTIFY_STREAM}...")

# Главный цикл
while True:
    try:
        # Читаем сообщения из stream
        messages = r.xreadgroup(
            CONSUMER_GROUP,
            CONSUMER_NAME,
            {NOTIFY_STREAM: '>'},
            count=1,
            block=1000  # 1 секунда
        )

        if not messages:
            continue

        for stream, items in messages:  # noqa: B007, RUF100
            for msg_id, fields in items:
                try:
                    # ФИЛЬТРАЦИЯ: Проверяем является ли это XAUUSD сигналом
                    if not is_xauusd_signal(fields):
                        print(f"⏭️ Пропускаем не-XAUUSD сигнал: {msg_id}")
                        # Подтверждаем чтобы не читать снова
                        r.xack(NOTIFY_STREAM, CONSUMER_GROUP, msg_id)
                        continue

                    print(f"🎯 XAUUSD сигнал найден: {msg_id}")

                    # Извлекаем текст сообщения
                    text = fields.get('text', '')
                    if not text:
                        print("⚠️ Пустой текст в XAUUSD сигнале")
                        r.xack(NOTIFY_STREAM, CONSUMER_GROUP, msg_id)
                        continue

                    # Отправляем в Telegram
                    success = send_telegram_message(text)

                    if success:
                        # Подтверждаем обработку
                        r.xack(NOTIFY_STREAM, CONSUMER_GROUP, msg_id)
                        print(f"✅ XAUUSD сигнал обработан: {msg_id}")
                    else:
                        print(f"❌ Не удалось отправить XAUUSD сигнал: {msg_id}")  # noqa: RUF001

                except Exception as e:
                    print(f"❌ Ошибка обработки сообщения {msg_id}: {e}")
                    # В случае ошибки подтверждаем чтобы не зациклиться  # noqa: RUF003
                    r.xack(NOTIFY_STREAM, CONSUMER_GROUP, msg_id)

    except Exception as e:
        print(f"❌ Ошибка в главном цикле: {e}")
        time.sleep(5)  # Пауза перед повтором

