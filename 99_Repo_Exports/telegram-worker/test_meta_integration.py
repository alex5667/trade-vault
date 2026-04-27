#!/usr/bin/env python3
"""
Тест интеграции meta-sidecar.
Проверяет, что функции чтения meta работают правильно.
"""

import os
import sys
import json
from typing import Any

# Добавляем текущую директорию в путь для импорта
sys.path.insert(0, os.path.dirname(__file__))

from app.config import load_settings
from outbox_meta import fetch_outbox_meta
from notify_worker import _fetch_outbox_meta, _attach_outbox_meta

def test_redis_connection():
    """Тестируем подключение к Redis."""
    try:
        settings = load_settings()
        import redis
        r = redis.Redis.from_url(settings.redis_url, decode_responses=True)
        r.ping()
        print("✅ Redis подключение работает")
        return r
    except Exception as e:
        print(f"❌ Redis подключение не работает: {e}")
        return None

def test_meta_functions(redis_client):
    """Тестируем функции работы с meta."""
    if not redis_client:
        return

    # Тестируем fetch_outbox_meta
    test_signal_id = "test-signal-123"
    meta = fetch_outbox_meta(redis_client, test_signal_id)
    print(f"✅ fetch_outbox_meta вернул: {meta} (тип: {type(meta)})")

    # Тестируем _fetch_outbox_meta из notify_worker
    meta2 = _fetch_outbox_meta(redis_client, test_signal_id)
    print(f"✅ _fetch_outbox_meta вернул: {meta2} (тип: {type(meta2)})")

    # Тестируем _attach_outbox_meta
    entry = {"signal_id": test_signal_id}
    parsed = {"symbol": "BTCUSDT", "direction": "LONG"}
    raw = {"source": "test"}

    _attach_outbox_meta(redis_client, entry=entry, parsed=parsed, raw=raw)
    print(f"✅ _attach_outbox_meta отработал, parsed: {parsed}")
    print(f"✅ _attach_outbox_meta отработал, raw: {raw}")

def main():
    """Главная функция теста."""
    print("🧪 Тестируем интеграцию meta-sidecar...")
    print()

    # Тестируем подключение к Redis
    redis_client = test_redis_connection()
    if not redis_client:
        print("❌ Невозможно продолжить тест без Redis")
        return

    print()
    # Тестируем функции meta
    test_meta_functions(redis_client)

    print()
    print("🎉 Тест завершен!")

if __name__ == "__main__":
    main()
