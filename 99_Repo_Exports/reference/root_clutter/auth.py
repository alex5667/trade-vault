#!/usr/bin/env python3
"""
Интерактивная авторизация Telegram (Telethon).
Запускать на ЛОКАЛЬНОМ компьютере (не в Docker!).

После успешного запуска появится файл tg_session.session.
Скопируйте его в telegram-worker/sessions/tg_session.session
"""
from telethon import TelegramClient

api_id = 26448331
api_hash = '0ecddda8790f53864284dc141acbee57'
phone = '+380672935013'

client = TelegramClient('tg_session', api_id, api_hash)
client.start(phone=phone)
print("✅ Успешно! Появился файл tg_session.session")
print("📋 Скопируйте его командой:")
print("   cp tg_session.session telegram-worker/sessions/tg_session.session")
