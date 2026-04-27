#!/usr/bin/env python3
"""
Скрипт для синхронизации данных между Redis на портах 6380 и 6381
"""

import redis

def sync_redis_data():
    """Синхронизирует данные из redis-worker-1 в redis-worker-2"""

    # Подключаемся к обоим Redis
    source = redis.Redis(host='localhost', port=6380, db=0, decode_responses=True)
    target = redis.Redis(host='localhost', port=6381, db=0, decode_responses=True)

    print("🔄 Синхронизация данных между Redis 6380 → 6381")
    print("=" * 60)

    # Список стримов для синхронизации
    streams = [
        'stream:top-losers',
        'stream:top-gainers',
        'stream:volume-signals',
        'stream:funding-signals',
        'stream:volatilityRange',
        'stream:volatility'
    ]

    total_synced = 0

    for stream_name in streams:
        try:
            # Получаем длину исходного стрима
            source_length = source.xlen(stream_name)

            if source_length == 0:
                print(f"⚠️  {stream_name}: пуст, пропускаем")
                continue

            # Получаем все сообщения из исходного стрима
            messages = source.xrange(stream_name, '-', '+')

            synced_count = 0
            for msg_id, fields in messages:
                try:
                    # Копируем сообщение в целевой стрим
                    target.xadd(stream_name, fields, id=msg_id, maxlen=1000, approximate=True)
                    synced_count += 1
                except Exception:
                    # Сообщение уже существует или другая ошибка
                    pass

            # Проверяем результат
            target_length = target.xlen(stream_name)
            total_synced += synced_count

            print(f"✅ {stream_name:30} : {synced_count:3} новых ({target_length:3} всего)")

        except Exception as e:
            print(f"❌ {stream_name:30} : Ошибка - {e}")

    print("=" * 60)
    print(f"📊 ВСЕГО СИНХРОНИЗИРОВАНО: {total_synced} сообщений")

if __name__ == "__main__":
    try:
        sync_redis_data()
    except KeyboardInterrupt:
        print("\n⛔ Синхронизация прервана")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
