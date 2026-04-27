#!/usr/bin/env python3
"""
Скрипт для экспорта данных из основного Redis (порт 6379) в Redis на порту 6380.
"""

import redis

def export_data_to_6380():
    """Экспортирует данные из основного Redis в Redis на порту 6380."""

    # Подключаемся к обоим Redis
    source_redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    target_redis = redis.Redis(host='localhost', port=6380, db=0, decode_responses=True)

    print("🚀 ЭКСПОРТ ДАННЫХ ИЗ REDIS 6379 В REDIS 6380")
    print("=" * 50)

    # Маппинг стримов
    stream_mapping = {
        'stream:top-losers': 'stream:top-losers',
        'stream:top-gainers': 'stream:top-gainers',
        'stream:funding-rates': 'stream:funding-signals',
        'stream:volatility': 'stream:volatility',
        'stream:volatilityRange': 'stream:volatilityRange'
    }

    results = {}

    for source_stream, target_stream in stream_mapping.items():
        try:
            print(f"\n📊 Обработка стрима: {source_stream} -> {target_stream}")

            # Проверяем, существует ли исходный стрим
            if not source_redis.exists(source_stream):
                print(f"   ⚠️ Исходный стрим {source_stream} не найден")
                results[source_stream] = 0
                continue

            # Получаем длину исходного стрима
            source_length = source_redis.xlen(source_stream)
            print(f"   📈 Исходный стрим содержит {source_length} сообщений")

            if source_length == 0:
                print("   ⚠️ Исходный стрим пуст")
                results[source_stream] = 0
                continue

            # Получаем все сообщения из исходного стрима
            messages = source_redis.xrange(source_stream, '-', '+')
            exported_count = 0

            for msg_id, fields in messages:
                try:
                    # Копируем сообщение в целевой стрим
                    target_redis.xadd(target_stream, fields, maxlen=1000, approximate=True)
                    exported_count += 1

                    # Показываем прогресс для больших стримов
                    if exported_count % 100 == 0:
                        print(f"   📤 Экспортировано {exported_count}/{source_length} сообщений...")

                except Exception as e:
                    print(f"   ❌ Ошибка экспорта сообщения {msg_id}: {e}")

            # Проверяем результат
            target_length = target_redis.xlen(target_stream)
            results[source_stream] = exported_count

            print(f"   ✅ Экспортировано {exported_count} сообщений")
            print(f"   📊 Целевой стрим содержит {target_length} сообщений")

        except Exception as e:
            print(f"   ❌ Ошибка обработки стрима {source_stream}: {e}")
            results[source_stream] = -1

    # Итоговая статистика
    print("\n" + "=" * 50)
    print("📊 ИТОГОВАЯ СТАТИСТИКА ЭКСПОРТА")
    print("=" * 50)

    total_exported = 0
    successful_streams = 0
    failed_streams = 0

    for source_stream, count in results.items():
        if count == -1:
            print(f"❌ {source_stream:25} : Ошибка")
            failed_streams += 1
        elif count == 0:
            print(f"⚠️  {source_stream:25} : Пуст или не найден")
        else:
            print(f"✅ {source_stream:25} : {count} сообщений")
            total_exported += count
            successful_streams += 1

    print("-" * 50)
    print(f"Всего экспортировано: {total_exported} сообщений")
    print(f"Успешных стримов: {successful_streams}")
    print(f"Ошибок: {failed_streams}")

    # Проверяем финальное состояние Redis на порту 6380
    print("\n🔍 ПРОВЕРКА ФИНАЛЬНОГО СОСТОЯНИЯ REDIS НА ПОРТУ 6380")
    print("-" * 50)

    for source_stream, target_stream in stream_mapping.items():  # noqa: B007
        try:
            length = target_redis.xlen(target_stream)
            print(f"{target_stream:25} : {length} сообщений")
        except Exception as e:
            print(f"{target_stream:25} : Ошибка - {e}")

if __name__ == "__main__":
    export_data_to_6380()
