#!/usr/bin/env python3
"""
Проверка выгрузки сигналов python-worker в Redis на порт 6380
"""

import redis
import json

def check_python_worker_export():
    """Проверяет, выгружает ли python-worker данные в Redis на порт 6380."""

    print("=" * 70)
    print("🔍 ПРОВЕРКА ВЫГРУЗКИ СИГНАЛОВ PYTHON-WORKER В REDIS НА ПОРТ 6380")
    print("=" * 70)

    # Подключаемся к Redis на порту 6380
    try:
        r = redis.Redis(host='localhost', port=6380, db=0, decode_responses=True)
        r.ping()
        print("✅ Подключение к Redis на порту 6380 успешно")
    except Exception as e:
        print(f"❌ Ошибка подключения к Redis на порту 6380: {e}")
        return

    # Список стримов для проверки
    streams = {
        'stream:top-losers': 'Losers (падающие активы)',
        'stream:top-gainers': 'Gainers (растущие активы)',
        'stream:volume-signals': 'Volume (объемы торгов)',
        'stream:funding-signals': 'Funding (ставки финансирования)',
        'stream:volatilityRange': 'Volatility by Range',
        'stream:volatility': 'Volatility Spike'
    }

    print("\n📊 ПРОВЕРКА СТРИМОВ:")
    print("-" * 70)

    total_messages = 0
    streams_with_data = 0

    for stream_name, description in streams.items():
        try:
            length = r.xlen(stream_name)
            total_messages += length

            if length > 0:
                streams_with_data += 1
                # Получаем последнее сообщение
                messages = r.xrevrange(stream_name, count=1)
                if messages:
                    msg_id, fields = messages[0]
                    if 'data' in fields:
                        data = json.loads(fields['data'])
                        symbol = data.get('symbol', data.get('data', {}).get('symbol', 'unknown'))
                        print(f"✅ {description:30} : {length:3} сообщений (последний: {symbol})")
                    else:
                        print(f"✅ {description:30} : {length:3} сообщений")
            else:
                print(f"⚠️  {description:30} : Пуст")

        except Exception as e:
            print(f"❌ {description:30} : Ошибка - {e}")

    print("-" * 70)
    print(f"📈 ВСЕГО СООБЩЕНИЙ: {total_messages}")
    print(f"📊 СТРИМОВ С ДАННЫМИ: {streams_with_data}/{len(streams)}")

    # Проверяем, выгружает ли python-worker данные
    print("\n🎯 ВЫВОД:")
    if total_messages > 0:
        print("✅ Python-worker ВЫГРУЖАЕТ данные в Redis на порт 6380")
        print(f"   Найдено {total_messages} сообщений в {streams_with_data} стримах")
    else:
        print("⚠️  В данный момент нет данных в стримах на порту 6380")
        print("   Возможные причины:")
        print("   1. Скринер метрик запускается раз в час")
        print("   2. Нет данных в основном Redis для анализа")
        print("   3. Python-worker только что перезапустился")

    # Проверяем тестовую запись
    test_key = 'test:signal:check'
    if r.exists(test_key):
        value = r.get(test_key)
        print(f"\n✅ Тестовая запись найдена: {test_key} = {value}")
        print("   Это подтверждает, что python-worker может записывать в Redis на порт 6380")

if __name__ == "__main__":
    check_python_worker_export()
