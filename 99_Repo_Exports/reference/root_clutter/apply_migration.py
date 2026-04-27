#!/usr/bin/env python3
"""
Скрипт для применения миграции создания таблицы regime_quantiles
"""

import psycopg2
import os

# Параметры подключения (из docker-compose.yml)
DB_CONFIG = {
    'host': 'localhost',
    'port': 5434,
    'user': 'postgres',
    'password': '12345',
    'database': 'trade'
}

MIGRATION_SQL = """
-- Migration: Create regime_quantiles table
-- Description: Creates table for storing ADX and ATR% quantiles by symbol/timeframe
-- Date: 2025-12-17

-- Connect to scanner_analytics database
\\c scanner_analytics;

-- Create regime_quantiles table
CREATE TABLE IF NOT EXISTS regime_quantiles (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol       text NOT NULL,
    timeframe    text NOT NULL,

    -- ADX percentiles
    adx_p40      double precision NOT NULL,
    adx_p60      double precision NOT NULL,
    adx_p75      double precision NOT NULL,

    -- ATR% percentiles (note: column name uses camelCase in some contexts)
    atrp_p25     double precision NOT NULL,
    atrp_p50     double precision NOT NULL,
    atrp_p75     double precision NOT NULL,

    -- Metadata
    "sampleSize" integer NOT NULL,                    -- number of samples used
    "updatedAt"  timestamptz NOT NULL DEFAULT now(), -- last update timestamp

    -- Constraints
    UNIQUE(symbol, timeframe)
);

-- Create index for fast lookups
CREATE INDEX IF NOT EXISTS idx_regime_quantiles_lookup
ON regime_quantiles (symbol, timeframe);

-- Create index for recent updates
CREATE INDEX IF NOT EXISTS idx_regime_quantiles_updated
ON regime_quantiles ("updatedAt" DESC);

-- Log completion
SELECT 'Regime quantiles table created successfully' as status;
"""

def apply_migration():
    try:
        # Сначала создадим базу scanner_analytics, если её нет
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='postgres'  # Подключаемся к postgres для создания БД
        )
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'scanner_analytics';")
            if not cur.fetchone():
                cur.execute("CREATE DATABASE scanner_analytics;")
            print("✓ База данных scanner_analytics создана или уже существует")

        conn.close()

        # Теперь подключаемся к scanner_analytics и применяем миграцию
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='scanner_analytics'
        )

        with conn.cursor() as cur:
            # Убираем команду \c scanner_analytics; и выполняем весь SQL блоком
            sql_to_execute = '\n'.join([line for line in MIGRATION_SQL.split('\n') if not line.strip().startswith('\\c')])

            try:
                cur.execute(sql_to_execute)
                print("✓ Миграция выполнена успешно")
            except Exception as e:
                print(f"⚠ Ошибка при выполнении миграции: {e}")
                print("SQL:", sql_to_execute[:200] + "..." if len(sql_to_execute) > 200 else sql_to_execute)

        conn.commit()
        print("✓ Миграция применена успешно")

        # Проверим, что таблица создана
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'regime_quantiles';")
            if cur.fetchone():
                print("✓ Таблица regime_quantiles создана")

                # Проверим колонки
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'regime_quantiles' AND table_schema = 'public';")
                columns = [row[0] for row in cur.fetchall()]
                required_columns = ['adx_p40', 'adx_p60', 'adx_p75', 'atrp_p25', 'atrp_p50', 'atrp_p75']
                missing = [col for col in required_columns if col not in columns]
                if missing:
                    print(f"❌ Отсутствуют колонки: {missing}")
                else:
                    print("✓ Все необходимые колонки присутствуют")
            else:
                print("❌ Таблица regime_quantiles не найдена")

        conn.close()

    except Exception as e:
        print(f"❌ Ошибка применения миграции: {e}")

if __name__ == "__main__":
    apply_migration()



