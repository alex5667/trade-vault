#!/usr/bin/env python3
"""
Simple notify worker for testing XAUUSD signal delivery to Telegram.
"""
import os
import asyncio
import json

import httpx

# Settings — read from environment (no hard-coded secrets)
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# Redis connection к worker-1 через docker exec
def get_redis_data():
    """Получает данные из Redis через docker exec"""
    import subprocess

    try:
        # Получаем последние сообщения из notify:telegram stream
        result = subprocess.run([
            'docker', 'exec', 'scanner-redis-worker-1',
            'redis-cli', 'XREVRANGE', 'notify:telegram', '+', '-', 'COUNT', '5'
        ], capture_output=True, text=True, check=True)

        lines = result.stdout.strip().split('\n')
        messages = []

        # Парсим результат Redis XREVRANGE
        i = 0
        while i < len(lines):
            if lines[i] and '-' in lines[i]:  # Message ID
                message_id = lines[i]
                i += 1

                # Читаем поля сообщения
                fields = {}
                while i < len(lines) and lines[i] and '-' not in lines[i]:
                    if i + 1 < len(lines):
                        key = lines[i]
                        value = lines[i + 1]
                        fields[key] = value
                        i += 2
                    else:
                        break

                if fields:
                    messages.append({
                        'id': message_id,
                        'fields': fields
                    })
            else:
                i += 1

        return messages

    except Exception as e:
        print(f"❌ Ошибка получения данных из Redis: {e}")
        return []

async def send_telegram_message(text: str) -> bool:
    """Отправляет сообщение в Telegram"""
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(API_URL, json=payload)

            if response.status_code == 200:
                print("✅ Сообщение отправлено в Telegram")
                return True
            else:
                print(f"❌ Ошибка Telegram API: {response.status_code} - {response.text}")
                return False

    except Exception as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")
        return False

def format_signal_message(fields: dict[str, str]) -> str:
    """Format a signal dict into an HTML Telegram message."""
    fields.get('text', '')
    symbol = fields.get('sid', '').split(':')[0] if ':' in fields.get('sid', '') else 'N/A'
    side = fields.get('side', 'N/A')
    price = fields.get('price', 'N/A')
    lot = fields.get('lot', 'N/A')
    note = fields.get('note', '')

    # Parse risk data
    risk_str = fields.get('risk', '{}')
    try:
        risk = json.loads(risk_str)
        sl = risk.get('sl', 'N/A')
        tp_levels = risk.get('tp_levels', [])
        rr = risk.get('rr', [])
    except (json.JSONDecodeError, ValueError, TypeError):
        sl = 'N/A'
        tp_levels = []
        rr = []

    # Формируем красивое сообщение
    message = f"🚀 <b>{symbol} {side}</b> @ {price}\n"
    message += f"💰 Volume: {lot} lot\n\n"
    message += "📊 <b>Risk Management:</b>\n"
    message += f"🛑 Stop Loss: {sl}\n"

    for i, (tp, r) in enumerate(zip(tp_levels, rr), 1):
        message += f"🎯 TP{i}: {tp} (RR {r})\n"

    if note:
        message += f"\n⚡ Reason: {note}"

    message += "\n\n🤖 <i>Scanner Infrastructure XAUUSD Signals</i>"

    return message

async def main():
    """Основная функция"""
    print("🚀 Simple Notify Worker - запуск...")
    print("📡 Checking notify:telegram stream in Redis...")

    # Получаем данные из Redis
    messages = get_redis_data()

    if not messages:
        print("📭 Нет новых сообщений в notify:telegram stream")
        return

    print(f"📬 Найдено {len(messages)} сообщений:")

    # Обрабатываем каждое сообщение
    for msg in messages:
        print(f"\n🔍 Обработка сообщения {msg['id']}:")

        # Проверяем что это XAUUSD сигнал
        fields = msg['fields']
        if 'XAUUSD' in fields.get('text', '') or 'XAUUSD' in fields.get('sid', ''):
            print("   ✅ XAUUSD сигнал обнаружен")

            # Форматируем и отправляем
            formatted_message = format_signal_message(fields)
            print("   📤 Отправляем в Telegram...")

            success = await send_telegram_message(formatted_message)
            if success:
                print("   ✅ Сигнал отправлен успешно!")
            else:
                print("   ❌ Ошибка отправки сигнала")
        else:
            print("   ⏭️ Пропуск - не XAUUSD сигнал")

if __name__ == "__main__":
    asyncio.run(main())
