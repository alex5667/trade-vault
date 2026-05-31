#!/usr/bin/env python3
"""
Скрипт для проверки всех типов сигналов в Redis на порту 6380.
"""

import redis
import json

def check_all_signals():
    """Проверяет все типы сигналов в Redis на порту 6380."""

    # Подключаемся к Redis на порту 6380
    r = redis.Redis(host='localhost', port=6380, db=0, decode_responses=True)

    print("🔍 ПРОВЕРКА ВСЕХ ТИПОВ СИГНАЛОВ В REDIS НА ПОРТУ 6380")
    print("=" * 60)

    # Список всех стримов для проверки
    streams = {
        'stream:top-losers': 'Losers (падающие активы)',
        'stream:top-gainers': 'Gainers (растущие активы)',
        'stream:volume-signals': 'Volume (объемы торгов)',
        'stream:funding-signals': 'Funding (ставки финансирования)',
        'stream:volatilityRange': 'Volatility by Range (волатильность по диапазону)',
        'stream:volatility': 'Volatility Spike (всплеск волатильности)'
    }

    results = {}

    for stream_name, description in streams.items():
        try:
            # Проверяем длину стрима
            length = r.xlen(stream_name)
            results[stream_name] = length

            print(f"\n📊 {description}")
            print(f"   Стрим: {stream_name}")
            print(f"   Количество сообщений: {length}")

            if length > 0:
                # Получаем последние сообщения
                messages = r.xrevrange(stream_name, count=2)
                print("   Последние сообщения:")

                for msg_id, fields in messages:
                    # Парсим JSON данные
                    if 'data' in fields:
                        try:
                            data = json.loads(fields['data'])
                            symbol = data.get('symbol', 'unknown')
                            timestamp = fields.get('timestamp', 'unknown')
                            print(f"     ID: {msg_id}")
                            print(f"     Symbol: {symbol}")
                            print(f"     Timestamp: {timestamp}")

                            # Показываем ключевые поля в зависимости от типа
                            if 'price_change_percent' in data:
                                print(f"     Price Change: {data['price_change_percent']}%")
                            if 'volume_24h' in data:
                                print(f"     Volume 24h: {data['volume_24h']}")
                            if 'funding_rate' in data:
                                print(f"     Funding Rate: {data['funding_rate']}")
                            if 'volatility' in data:
                                print(f"     Volatility: {data['volatility']}%")
                            if 'range' in data:
                                print(f"     Range: {data['range']}")

                        except json.JSONDecodeError:
                            print(f"     Данные: {fields['data']}")
                    else:
                        print(f"     Данные: {fields}")
            else:
                print("   ⚠️ Стрим пуст")

        except Exception as e:
            print(f"   ❌ Ошибка проверки стрима {stream_name}: {e}")
            results[stream_name] = -1

    # Итоговая статистика
    print("\n" + "=" * 60)
    print("📊 ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 60)

    total_messages = 0
    empty_streams = 0
    error_streams = 0

    for stream_name, count in results.items():
        if count == -1:
            print(f"❌ {stream_name:25} : Ошибка")
            error_streams += 1
        elif count == 0:
            print(f"⚠️  {stream_name:25} : Пуст")
            empty_streams += 1
        else:
            print(f"✅ {stream_name:25} : {count} сообщений")
            total_messages += count

    print("-" * 60)
    print(f"Всего сообщений: {total_messages}")
    print(f"Пустых стримов: {empty_streams}")
    print(f"Ошибок: {error_streams}")

    # Проверяем наличие всех типов сигналов
    required_streams = ['losers', 'gainers', 'volume', 'funding', 'volatilitybyrange', 'volatilityspike']
    found_streams = []

    for stream_name, count in results.items():
        if count > 0:
            if 'losers' in stream_name:
                found_streams.append('losers')
            elif 'gainers' in stream_name:
                found_streams.append('gainers')
            elif 'volume' in stream_name:
                found_streams.append('volume')
            elif 'funding' in stream_name:
                found_streams.append('funding')
            elif 'volatilityRange' in stream_name:
                found_streams.append('volatilitybyrange')
            elif 'volatility' in stream_name and 'Range' not in stream_name:
                found_streams.append('volatilityspike')

    print(f"\n🎯 НАЙДЕННЫЕ ТИПЫ СИГНАЛОВ: {', '.join(found_streams)}")

    missing = set(required_streams) - set(found_streams)
    if missing:
        print(f"❌ ОТСУТСТВУЮЩИЕ ТИПЫ: {', '.join(missing)}")
    else:
        print("✅ ВСЕ ТИПЫ СИГНАЛОВ НАЙДЕНЫ!")

if __name__ == "__main__":
    check_all_signals()
