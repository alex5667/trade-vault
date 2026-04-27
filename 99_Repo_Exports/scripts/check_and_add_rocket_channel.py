#!/usr/bin/env python3
"""
Скрипт для проверки и добавления канала @RocketwalletsignalsTG
"""

import redis
import os

def main():
    # Подключение к Redis
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    print("=" * 80)
    print("ПРОВЕРКА КАНАЛОВ TELEGRAM")
    print("=" * 80)

    # Получаем все каналы
    channels = r.smembers("telegram:channels:usernames")
    print(f"\n📊 Всего каналов: {len(channels)}")

    # Ищем Rocket канал
    rocket_channels = [ch for ch in channels if 'rocket' in ch.lower()]

    if rocket_channels:
        print("\n✅ Найдены Rocket каналы:")
        for ch in rocket_channels:
            status_key = f"telegram:channel:{ch}:status"
            status = r.get(status_key)
            print(f"  - {ch} (статус: {status or 'не установлен'})")
    else:
        print("\n❌ Rocket каналы НЕ найдены!")
        print("\n📋 Все каналы:")
        for ch in sorted(channels):
            print(f"  - {ch}")

    # Проверяем конкретный канал
    target_channel = "@RocketwalletsignalsTG"
    target_channel_clean = "RocketwalletsignalsTG"

    print("\n" + "=" * 80)
    print(f"ПРОВЕРКА КАНАЛА {target_channel}")
    print("=" * 80)

    if target_channel in channels or target_channel_clean in channels:
        print("✅ Канал найден в списке!")
        # Проверяем статус
        for variant in [target_channel, target_channel_clean]:
            status_key = f"telegram:channel:{variant}:status"
            status = r.get(status_key)
            if status:
                print(f"  Статус ({variant}): {status}")
    else:
        print("❌ Канал НЕ найден в списке!")
        print("\n🔧 Добавляем канал...")

        # Добавляем канал
        r.sadd("telegram:channels:usernames", target_channel)

        # Устанавливаем статус ACTIVE
        status_key = f"telegram:channel:{target_channel}:status"
        r.set(status_key, "ACTIVE")

        print(f"✅ Канал {target_channel} добавлен со статусом ACTIVE")
        print("\n⚠️  ТРЕБУЕТСЯ ПЕРЕЗАПУСК telegram-worker!")
        print("   Выполните: docker-compose restart telegram-worker")

    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()

