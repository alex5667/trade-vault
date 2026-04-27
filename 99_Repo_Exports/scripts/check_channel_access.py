#!/usr/bin/env python3
"""
Проверяем доступ к каналам CoinCodeCap
"""
import asyncio
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError
import os

# Из переменных окружения
API_ID = int(os.getenv("TG_API_ID", "26448331"))
API_HASH = os.getenv("TG_API_HASH", "0ecdf86d800a01e3fc73efcf0d8e22c0")
SESSION_NAME = 'telegram_worker'

channels_to_check = [
    "@coincodecap",
    "@CoinCodeCap_Classic_Signals",
    "@Classic_Coincodecap",
    "@Coin_CodeCapClassic"
]

async def check_channels():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()

    print("="*80)
    print("ПРОВЕРКА ДОСТУПА К КАНАЛАМ CoinCodeCap")
    print("="*80)

    for channel in channels_to_check:
        print(f"\nПроверка {channel}:")
        try:
            entity = await client.get_entity(channel)
            print("  ✅ Доступ есть")
            print(f"     ID: {entity.id}")
            print(f"     Title: {entity.title}")

            # Получаем последние сообщения
            messages = await client.get_messages(entity, limit=5)
            print(f"     Последние {len(messages)} сообщений:")
            for i, msg in enumerate(messages, 1):
                if msg.text:
                    preview = msg.text[:60].replace('\n', ' ')
                    print(f"       {i}. [{msg.date}] {preview}...")

        except ChannelPrivateError:
            print("  ❌ Канал приватный или нет доступа")
        except Exception as e:
            print(f"  ❌ Ошибка: {e}")

    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(check_channels())

