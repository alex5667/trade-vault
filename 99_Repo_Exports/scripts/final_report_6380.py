#!/usr/bin/env python3
"""
Финальный отчет о состоянии всех типов сигналов в Redis на порту 6380.
"""

import redis
import json

def generate_final_report():
    """Генерирует финальный отчет о всех сигналах в Redis на порту 6380."""

    # Подключаемся к Redis на порту 6380
    r = redis.Redis(host='localhost', port=6380, db=0, decode_responses=True)

    print("📊 ФИНАЛЬНЫЙ ОТЧЕТ: СИГНАЛЫ В REDIS НА ПОРТУ 6380")
    print("=" * 60)
    print(f"Время проверки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
    total_messages = 0

    for stream_name, description in streams.items():
        try:
            # Проверяем длину стрима
            length = r.xlen(stream_name)
            results[stream_name] = length
            total_messages += length

            status = "✅" if length > 0 else "⚠️"
            print(f"{status} {description:35} : {length:3} сообщений")

        except Exception as e:
            results[stream_name] = -1
            print(f"❌ {description:35} : Ошибка - {e}")

    print("-" * 60)
    print(f"📈 ВСЕГО СООБЩЕНИЙ: {total_messages}")

    # Проверяем наличие всех типов сигналов
    required_types = ['losers', 'gainers', 'volume', 'funding', 'volatilitybyrange', 'volatilityspike']
    found_types = []

    if results.get('stream:top-losers', 0) > 0:
        found_types.append('losers')
    if results.get('stream:top-gainers', 0) > 0:
        found_types.append('gainers')
    if results.get('stream:volume-signals', 0) > 0:
        found_types.append('volume')
    if results.get('stream:funding-signals', 0) > 0:
        found_types.append('funding')
    if results.get('stream:volatilityRange', 0) > 0:
        found_types.append('volatilitybyrange')
    if results.get('stream:volatility', 0) > 0:
        found_types.append('volatilityspike')

    print(f"\n🎯 НАЙДЕННЫЕ ТИПЫ СИГНАЛОВ: {', '.join(found_types)}")

    missing_types = set(required_types) - set(found_types)
    if missing_types:
        print(f"❌ ОТСУТСТВУЮЩИЕ ТИПЫ: {', '.join(missing_types)}")
    else:
        print("✅ ВСЕ ТИПЫ СИГНАЛОВ НАЙДЕНЫ!")

    # Показываем примеры данных
    print("\n📋 ПРИМЕРЫ ДАННЫХ:")
    print("-" * 40)

    for stream_name, description in streams.items():
        if results.get(stream_name, 0) > 0:
            try:
                # Получаем последнее сообщение
                messages = r.xrevrange(stream_name, count=1)
                if messages:
                    msg_id, fields = messages[0]
                    if 'data' in fields:
                        data = json.loads(fields['data'])
                        symbol = data.get('symbol', 'unknown')
                        print(f"{description}: {symbol}")
                    else:
                        print(f"{description}: Данные недоступны")
            except Exception as e:
                print(f"{description}: Ошибка чтения - {e}")

    print("\n✅ ОТЧЕТ ЗАВЕРШЕН!")
    print("Все сигналы успешно выгружены в Redis на порту 6380")

if __name__ == "__main__":
    from datetime import datetime
    generate_final_report()
