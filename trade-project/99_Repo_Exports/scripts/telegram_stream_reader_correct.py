#!/usr/bin/env python3
"""Telegram stream reader — forwards XAUUSD + channel signals to Telegram."""
import os
import time

import redis
import requests

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://scanner-redis-worker-1:6379/0")
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
CONSUMER_GROUP = "trading-signals-group"
CONSUMER_NAME = "trading-signals-sender"

print("🎯 Trading Signals Reader запуск...")
print("   ✅ XAUUSD сигналы (наши)")
print("   ✅ Торговые сигналы из каналов")
print("   ❌ Реклама и VIP предложения")

# Redis клиент
r = redis.from_url(REDIS_URL, decode_responses=True)

def should_send_signal(fields):
    """Проверяет нужно ли отправлять сигнал в Telegram"""

    # 1. ВСЕГДА пропускаем наши XAUUSD сигналы
    text = fields.get('text', '')
    if 'XAUUSD' in text and ('Breakout' in text or 'Absorption' in text):
        print("   ✅ Наш XAUUSD сигнал")
        return True

    # 2. Пропускаем спарщенные торговые сигналы из каналов
    signal_type = fields.get('type', '')
    if signal_type == 'trading_signal':
        # Проверяем что это реальный торговый сигнал, а не реклама  # noqa: RUF003
        symbol = fields.get('symbol', '')
        direction = fields.get('direction', '')

        # Блокируем рекламу и VIP предложения
        if symbol in ['VIP', 'None', '', 'GALAUSDTUSDT', 'DOGSUSDTUSDT']:
            print(f"   ❌ Блокируем рекламу: {symbol}")
            return False

        # Блокируем общую аналитику без конкретного символа
        if 'Market Overview' in text or 'VIP Service' in text:
            print("   ❌ Блокируем аналитику/рекламу")
            return False

        # Пропускаем нормальные торговые сигналы
        if symbol and direction:
            print(f"   ✅ Торговый сигнал: {symbol} {direction}")
            return True

    # 3. Блокируем всё остальное
    print("   ❌ Блокируем прочее")
    return False

def send_telegram(text):
    """Отправляет в Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        response = requests.post(url, data=data, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"   ❌ Telegram error: {e}")
        return False

# Create consumer group
try:
    r.xgroup_create(NOTIFY_STREAM, CONSUMER_GROUP, id='0', mkstream=True)
    print(f"✅ Group created: {CONSUMER_GROUP}")
except redis.exceptions.ResponseError as _be:
    print(f"✅ Group already exists: {CONSUMER_GROUP}")

print("🔄 Читаю торговые сигналы...")

# Главный цикл
while True:
    try:
        messages = r.xreadgroup(CONSUMER_GROUP, CONSUMER_NAME, {NOTIFY_STREAM: '>'}, count=1, block=1000)

        if not messages:
            continue

        for stream, items in messages:  # noqa: B007, RUF100
            for msg_id, fields in items:
                print(f"📨 Сообщение: {msg_id}")

                if should_send_signal(fields):
                    text = fields.get('text', '')
                    if send_telegram(text):
                        print("✅ Отправлен в Telegram")
                    else:
                        print("❌ Ошибка отправки")

                # Подтверждаем в любом случае
                r.xack(NOTIFY_STREAM, CONSUMER_GROUP, msg_id)

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        time.sleep(5)
