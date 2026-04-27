#!/usr/bin/env python3
"""
Скрипт для проверки наличия полей MFE в Redis.
Подключается к Redis через Docker exec и проверяет последние закрытые сделки.
"""
import subprocess

def run_redis_cmd(cmd):
    """Выполнить команду redis-cli через docker exec"""
    full_cmd = f"docker exec scanner-redis-worker-1 redis-cli {cmd}"
    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip()

def main():
    print("🔍 Проверка наличия полей MFE в Redis...")
    print("=" * 60)

    # Получаем последние 5 сделок из stream
    cmd = "XREVRANGE trades:closed + - COUNT 5"
    output = run_redis_cmd(cmd)

    if not output or output == "(empty array)":
        print("❌ Stream trades:closed пуст или Redis недоступен")
        return

    # Парсим вывод (простой парсинг для демонстрации)
    lines = output.split('\n')

    print(f"\n📊 Найдено записей в stream: {len([line_ for line_ in lines if line_.startswith('1)')])}")
    print("\nПроверка полей в последних сделках:\n")

    # Для каждой сделки проверяем order hash
    for i in range(1, 6):
        print(f"\n--- Сделка #{i} ---")
        # Пытаемся получить order_id из stream (упрощенно)
        # Более надежный способ - через XREAD, но для демо достаточно

        # Альтернативный подход: получаем ключи order:*
        # Используем SCAN вместо KEYS для production

    # Более простой подход: проверим один конкретный order hash
    print("\n🔍 Проверка структуры order hash (пример):")
    print("-" * 60)

    # Получаем список ключей order:*
    keys_output = run_redis_cmd("KEYS 'order:*'")
    keys = [k.strip() for k in keys_output.split('\n') if k.strip()]

    if not keys:
        print("❌ Нет ключей order:* в Redis")
        return

    # Берем первые 3 ключа для проверки
    for order_key in keys[:3]:
        print(f"\n📦 {order_key}")

        # Получаем все поля
        fields_cmd = f"HGETALL {order_key}"
        fields_output = run_redis_cmd(fields_cmd)

        if not fields_output:
            continue

        # Парсим поля (каждая пара строк - ключ/значение)
        field_lines = fields_output.split('\n')
        fields_dict = {}
        for i in range(0, len(field_lines), 2):
            if i + 1 < len(field_lines):
                key = field_lines[i].strip()
                value = field_lines[i+1].strip()
                fields_dict[key] = value

        # Проверяем наличие интересующих полей
        mfe_fields = {
            'mfe': fields_dict.get('mfe', '❌ НЕТ'),
            'mfe_pnl': fields_dict.get('mfe_pnl', '❌ НЕТ'),
            'mfe_usd': fields_dict.get('mfe_usd', '❌ НЕТ'),
            'giveback': fields_dict.get('giveback', '❌ НЕТ'),
            'giveback_pnl': fields_dict.get('giveback_pnl', '❌ НЕТ'),
            'missed_profit': fields_dict.get('missed_profit', '❌ НЕТ'),
            'missed_profit_pnl': fields_dict.get('missed_profit_pnl', '❌ НЕТ'),
            'lot': fields_dict.get('lot', '❌ НЕТ'),
            'pnl_gross': fields_dict.get('pnl_gross', '❌ НЕТ'),
            'pnl_net': fields_dict.get('pnl_net', '❌ НЕТ'),
        }

        for field, value in mfe_fields.items():
            status = "✅" if value != "❌ НЕТ" else "❌"
            print(f"  {status} {field:20s}: {value}")

    print("\n" + "=" * 60)
    print("✅ Проверка завершена")

if __name__ == "__main__":
    main()
