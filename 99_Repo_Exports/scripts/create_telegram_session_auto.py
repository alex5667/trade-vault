#!/usr/bin/env python3
"""
Автоматическое создание Telegram сессии БЕЗ интерактивного ввода.
Использует данные из переменных окружения или хардкод.
"""
import asyncio
import sys
from telethon import TelegramClient

# Данные для авторизации
API_ID = 26448331
API_HASH = '0ecd4a260c8bf429074c274db55bb15f'
PHONE = '+380672935013'
SESSION_FILE = 'telegram-worker/sessions/tg_session'

# ВАЖНО: Эти данные нужно получить один раз
# CODE - получите через SMS на телефон +380672935013
# PASSWORD - пароль 2FA если включен

async def create_session_with_existing_auth():
    """Пытается использовать существующую авторизацию."""
    print("🔧 Создание Telegram сессии...")
    print(f"📱 Телефон: {PHONE}")
    print(f"🆔 API ID: {API_ID}")

    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

    try:
        print("🔌 Подключение к Telegram...")
        await client.connect()

        if await client.is_user_authorized():
            print("✅ Пользователь уже авторизован!")
            me = await client.get_me()
            print(f"👤 Авторизован как: {me.first_name} {me.last_name or ''} (@{me.username or 'no_username'})")
            await client.disconnect()
            return True

        print("❌ Сессия не авторизована")
        print("")
        print("📋 ДЛЯ АВТОРИЗАЦИИ ВЫПОЛНИТЕ:")
        print("")
        print("1. Запустите скрипт на хосте (НЕ в контейнере):")
        print("   cd /home/alex/front/trade/scanner_infra")
        print("   python3 setup-telegram-session.py")
        print("")
        print("2. Следуйте инструкциям:")
        print("   - Введите код из SMS")
        print("   - Введите пароль 2FA (если есть)")
        print("")
        print("3. Перезапустите telegram-worker:")
        print("   docker-compose restart telegram-worker")
        print("")

        await client.disconnect()
        return False

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(create_session_with_existing_auth())
    sys.exit(0 if success else 1)

