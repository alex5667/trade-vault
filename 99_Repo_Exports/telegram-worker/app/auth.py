"""
Авторизация в Telegram (Telethon) и работа с белым списком источников.

Функции:
- load_whitelist: загрузка белого списка из настроек
- ensure_authorized: безинтерактивная авторизация по коду/2FA
"""

import asyncio
from telethon import TelegramClient
from telethon.errors import RPCError, SessionPasswordNeededError, PhoneCodeInvalidError, PhoneNumberInvalidError
from .config import Settings


def load_whitelist(settings: Settings) -> list[str]:
    """
    Возвращает белый список источников из строки настроек.

    Поддерживаются форматы: @username или numeric chat_id в виде строки.
    Пустой список = разрешены все источники.
    """
    return [w.strip() for w in settings.whitelist.split(',') if w.strip()]


async def ensure_authorized(client: TelegramClient, settings: Settings) -> None:
    """
    Гарантирует, что клиент авторизован:
    - Если сессия действительна — ничего не делает
    - Иначе отправляет код на указанный телефон и выполняет sign_in
    - При включенной 2FA запрашивает пароль (из настроек)

    Исключительные ситуации обрабатываются сообщениями и завершением процесса
    с кодом, понятным оркестратору контейнеров.
    """
    await client.connect()
    if await client.is_user_authorized():
        print("✅ Telegram: session already authorized")
        return

    if not settings.phone:
        print("❌ TG_PHONE is not set. Set TG_PHONE in environment for non-interactive login.")
        raise SystemExit(1)

    try:
        sent = await client.send_code_request(settings.phone)
    except (RPCError, PhoneNumberInvalidError) as e:
        print(f"❌ Failed to send code: {e}")
        raise SystemExit(1)

    if not settings.code:
        print("📨 Code sent to your Telegram. Set TG_CODE in env and restart the container once.")
        raise SystemExit(2)

    try:
        await client.sign_in(phone=settings.phone, code=settings.code, phone_code_hash=sent.phone_code_hash)
    except SessionPasswordNeededError:
        if not settings.password:
            print("🔐 2FA enabled. Set TG_PASSWORD in env and restart.")
            raise SystemExit(3)
        await client.sign_in(password=settings.password)
    except PhoneCodeInvalidError:
        print("❌ Invalid TG_CODE. Update TG_CODE and restart.")
        raise SystemExit(4)

    print("✅ Telegram: authorization complete") 