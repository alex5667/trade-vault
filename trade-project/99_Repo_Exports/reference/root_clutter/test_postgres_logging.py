#!/usr/bin/env python3
"""
Тестовый скрипт для проверки логирования PostgreSQL.
Выполняет большое количество запросов к таблице symbol_meta,
чтобы убедиться, что логируется только каждый 10000-й запрос.
"""

import psycopg2
import time

# Настройки подключения
DB_CONFIG = {
    'host': 'localhost',
    'port': 5434,
    'user': 'trading',
    'password': 'trading_password',
    'database': 'trade'
}

def test_logging():
    """Выполняем тестовые запросы для проверки логирования."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("🚀 Начинаем тест логирования PostgreSQL...")
        print("Будет выполнено 50000 запросов к таблице symbol_meta")
        print("Ожидаем логирование только ~5 сообщений STATEMENT (каждый 10000-й запрос)")
        print()

        start_time = time.time()

        # Выполняем много запросов
        for i in range(1, 50001):
            cursor.execute("""
                SELECT exchange, symbol, tickSize
                FROM symbol_meta
                WHERE exchange = 'binance' AND symbol = 'BTCUSDT'
                LIMIT 1
            """)

            if i % 10000 == 0:
                print(f"✅ Выполнен запрос №{i}")
                time.sleep(0.01)  # Небольшая пауза

        end_time = time.time()

        cursor.close()
        conn.close()

        print()
        print(".2f"        print("Проверьте логи PostgreSQL командой:")
        print("docker-compose logs postgres | grep STATEMENT | tail -10")

    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    test_logging()
