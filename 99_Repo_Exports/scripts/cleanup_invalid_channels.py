#!/usr/bin/env python3
"""
Скрипт для очистки невалидных каналов из Redis
Senior Developer approach: validate before process
"""

import redis
import re

# Паттерн для валидных Telegram каналов
CHANNEL_PATTERN = re.compile(r'^@?[a-zA-Z][a-zA-Z0-9_]{4,31}$')

# Известные невалидные каналы
INVALID_CHANNELS = {
    '@my_trd_56_bot',  # Это бот, не канал
    '@wallstreetqueenofficalTG1',  # Несуществующий
    '@Wallstreetqueenoffical_Live',  # Несуществующий (опечатка в official)
}

def validate_channel_name(channel: str) -> tuple[bool, str]:
    """
    Валидация имени канала.

    Returns:
        (is_valid, reason)
    """
    if not channel:
        return False, "Empty channel name"

    # Убираем @ для проверки
    clean_name = channel.lstrip('@')

    # Проверка на бота
    if clean_name.endswith('_bot') or clean_name.endswith('bot'):
        return False, "Bot detected (not a channel)"

    # Проверка формата
    if not CHANNEL_PATTERN.match(channel):
        return False, "Invalid format"

    # Проверка на известные невалидные каналы
    if channel in INVALID_CHANNELS:
        return False, "Known invalid channel"

    return True, "OK"

def main():
    print("=" * 80)
    print("ОЧИСТКА НЕВАЛИДНЫХ КАНАЛОВ")
    print("=" * 80)
    print()

    # Подключение к Redis
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
    except Exception as e:
        print(f"❌ Ошибка подключения к Redis: {e}")
        return 1

    # Получаем все каналы
    try:
        all_channels = r.smembers('telegram:channels:usernames')
        print(f"📊 Всего каналов: {len(all_channels)}")
        print()
    except Exception as e:
        print(f"❌ Ошибка получения каналов: {e}")
        return 1

    # Валидация каждого канала
    valid_channels = []
    invalid_channels = []

    for channel in sorted(all_channels):
        is_valid, reason = validate_channel_name(channel)
        if is_valid:
            valid_channels.append(channel)
        else:
            invalid_channels.append((channel, reason))
            print(f"❌ {channel}: {reason}")

    print()
    print(f"✅ Валидных каналов: {len(valid_channels)}")
    print(f"❌ Невалидных каналов: {len(invalid_channels)}")
    print()

    if not invalid_channels:
        print("🎉 Все каналы валидны!")
        return 0

    # Спрашиваем подтверждение на удаление
    print("Удалить невалидные каналы? (y/N): ", end='')
    response = input().strip().lower()

    if response == 'y':
        for channel, reason in invalid_channels:  # noqa: B007
            try:
                # Удаляем из множества каналов
                r.srem('telegram:channels:usernames', channel)

                # Удаляем статус канала
                r.delete(f'telegram:channel:{channel}:status')

                print(f"🗑️  Удален: {channel}")
            except Exception as e:
                print(f"⚠️  Ошибка удаления {channel}: {e}")

        print()
        print("✅ Очистка завершена!")
        print(f"📊 Осталось каналов: {r.scard('telegram:channels:usernames')}")
    else:
        print("❌ Отменено")

    return 0

if __name__ == "__main__":
    exit(main())

