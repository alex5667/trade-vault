#!/usr/bin/env python3
"""
Create scanner_analytics database and apply migrations
"""
import psycopg2
import os

def create_database():
    # Connect to default postgres database
    try:
        conn = psycopg2.connect('postgresql://postgres:12345@localhost:5432/postgres')
        conn.autocommit = True
        cursor = conn.cursor()

        # Create scanner_analytics database if it doesn't exist
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'scanner_analytics'")
        exists = cursor.fetchone()

        if not exists:
            print("Создание базы данных scanner_analytics...")
            cursor.execute("CREATE DATABASE scanner_analytics")
            print("✅ База данных scanner_analytics создана")
        else:
            print("✅ База данных scanner_analytics уже существует")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Ошибка при создании базы данных: {e}")
        return False

    return True

def apply_migrations():
    try:
        # Connect to scanner_analytics database
        conn = psycopg2.connect('postgresql://postgres:12345@localhost:5432/scanner_analytics')
        cursor = conn.cursor()

        # Read and execute migration file
        migration_file = 'python-worker/migrations/006_create_scanner_analytics_tables.sql'

        print(f"Применение миграции: {migration_file}")

        with open(migration_file, 'r') as f:
            sql = f.read()

        # Split by \c command (database switch) - we skip this since we're already in the right DB
        sql = sql.replace('\\c scanner_analytics;', '')

        # Execute the SQL
        cursor.execute(sql)
        conn.commit()

        print("✅ Миграция успешно применена")

        # Check if trades_closed table exists
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'trades_closed'")
        table_exists = cursor.fetchone()

        if table_exists:
            print("✅ Таблица trades_closed создана")
        else:
            print("❌ Таблица trades_closed не найдена")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Ошибка при применении миграции: {e}")
        return False

    return True

if __name__ == "__main__":
    print("🚀 Создание базы данных scanner_analytics и применение миграций...")

    if create_database():
        if apply_migrations():
            print("🎉 Все операции завершены успешно!")
        else:
            print("❌ Ошибка при применении миграций")
    else:
        print("❌ Ошибка при создании базы данных")
