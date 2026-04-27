#!/usr/bin/env python3
import subprocess

def execute_redis_command(container, port, command):
    """Выполняет команду Redis в контейнере"""
    cmd = f"docker exec {container} redis-cli -p {port} {command}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip()

def transfer_candles_data():
    """Переносит данные candles:data с порта 6379 на 6380"""

    print("🔄 ПЕРЕНОС CANDLES:DATA С ПОРТА 6379 НА 6380")
    print("=" * 50)

    # Очищаем данные на порту 6380
    print("🧹 Очистка данных на порту 6380...")
    execute_redis_command("scanner-redis-worker-1", 6379, "DEL candles:data")

    # Получаем все записи с порта 6379
    print("📤 Получение данных с порта 6379...")
    entries = execute_redis_command("scanner-redis", 6379, "XRANGE candles:data - +")

    if not entries:
        print("❌ Нет данных для переноса")
        return

    # Парсим и переносим данные
    print("📥 Перенос данных на порт 6380...")

    lines = entries.split('\n')
    entry_id = None
    fields = []
    transferred = 0

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Если это ID записи (формат: timestamp-counter)
        if '-' in line and not line.startswith('"') and not line.startswith('{'):
            # Сохраняем предыдущую запись если есть
            if entry_id and fields:
                transfer_entry(entry_id, fields)
                transferred += 1
                if transferred % 100 == 0:
                    print(f"Перенесено записей: {transferred}")

            # Начинаем новую запись
            entry_id = line
            fields = []
        else:
            # Это поле записи
            fields.append(line)

    # Переносим последнюю запись
    if entry_id and fields:
        transfer_entry(entry_id, fields)
        transferred += 1

    print(f"✅ Перенос завершен! Перенесено записей: {transferred}")

    # Проверяем результат
    count_6379 = execute_redis_command("scanner-redis", 6379, "XLEN candles:data")
    count_6380 = execute_redis_command("scanner-redis-worker-1", 6379, "XLEN candles:data")

    print("📊 Результат:")
    print(f"  Порт 6379: {count_6379} записей")
    print(f"  Порт 6380: {count_6380} записей")

def transfer_entry(entry_id, fields):
    """Переносит одну запись на порт 6380"""
    # Строим команду XADD
    cmd_parts = ["XADD", "candles:data", entry_id]
    cmd_parts.extend(fields)

    # Выполняем команду
    command = " ".join(f'"{part}"' if ' ' in part else part for part in cmd_parts)
    execute_redis_command("scanner-redis-worker-1", 6379, command)

if __name__ == "__main__":
    transfer_candles_data()
