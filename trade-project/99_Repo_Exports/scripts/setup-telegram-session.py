#!/usr/bin/env python3
import sys
import asyncio
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

def load_env():
    env_vars = {}
    try:
        with open('telegram-worker/.env', 'r') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    env_vars[key] = value
    except FileNotFoundError:
        print("❌ Файл .env не найден")
        return None
    return env_vars

async def setup_telegram_session():
    print("🔧 Настройка Telegram сессии...")

    env = load_env()
    if not env:
        return False

    api_id = env.get('TG_API_ID')
    api_hash = env.get('TG_API_HASH')
    phone = env.get('TG_PHONE')
    session_name = 'telegram-worker/sessions/tg_session'

    print(f"📱 Телефон: {phone}")
    print(f"🆔 API ID: {api_id}")

    try:
        client = TelegramClient(session_name, int(api_id), api_hash)

        print("🔌 Подключение к Telegram...")
        await client.connect()

        if not await client.is_user_authorized():
            print("📞 Отправляем код на телефон...")
            await client.send_code_request(phone)

            print("💡 Введите код из SMS:")
            code = input("Код: ").strip()

            try:
                await client.sign_in(phone, code)
                print("✅ Авторизация успешна!")
            except SessionPasswordNeededError:
                print("🔐 Требуется пароль 2FA")
                password = input("Пароль 2FA: ").strip()
                await client.sign_in(password=password)
                print("✅ Авторизация с паролем успешна!")
        else:
            print("✅ Пользователь уже авторизован")

        me = await client.get_me()
        print(f"👤 Авторизован как: {me.first_name} {me.last_name or ''}")

        await client.disconnect()
        print("✅ Сессия сохранена в telegram-worker/sessions/tg_session.session")
        return True

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

async def main():
    success = await setup_telegram_session()
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
