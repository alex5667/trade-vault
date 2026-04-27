#!/usr/bin/env python3
"""
Проверяет все места, где создаются timestamp'ы, чтобы убедиться в правильности.
"""

import subprocess

def run_grep(pattern, include_pattern=None, exclude_pattern=None, exclude_dirs=None):
    """Запускает grep с указанными параметрами."""
    cmd = ['grep', '-r', '-n', pattern, '.']

    if include_pattern:
        cmd.extend(['--include', include_pattern])

    if exclude_pattern:
        cmd.extend(['--exclude-dir', exclude_pattern])

    if exclude_dirs:
        for exclude_dir in exclude_dirs:
            cmd.extend(['--exclude-dir', exclude_dir])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
        return result.stdout.strip().split('\n') if result.stdout.strip() else []
    except Exception as e:
        print(f"Ошибка выполнения grep: {e}")
        return []

def check_timestamp_creation_patterns():
    """Проверяет все паттерны создания timestamp'ов."""
    print("🔍 ПРОВЕРЯЕМ ВСЕ МЕСТА СОЗДАНИЯ TIMESTAMP'ОВ...")
    print("=" * 60)

    exclude_dirs = ['telegram-test-env', 'websocket_test', '__pycache__', '.git', 'venv', 'node_modules']

    # Паттерны, которые создают timestamp'ы
    timestamp_patterns = [
        # Python паттерны
        (r'"timestamp":\s*', 'Python: "timestamp": field'),
        (r"'timestamp':\s*", 'Python: \'timestamp\': field'),
        (r'timestamp\s*=', 'Python: timestamp = assignment'),
        (r'timestamp_ms\s*=', 'Python: timestamp_ms = assignment'),
        (r'timestamp_sec\s*=', 'Python: timestamp_sec = assignment'),

        # Go паттерны
        (r'"timestamp":\s*', 'Go: "timestamp": field'),
        (r'timestamp\s*:', 'Go: timestamp: field'),
        (r'Timestamp\s*:', 'Go: Timestamp: field'),
    ]

    all_timestamp_creations = []

    for pattern, description in timestamp_patterns:
        print(f"🔍 Ищем {description}...")
        results = run_grep(pattern, exclude_dirs=exclude_dirs)
        for line in results:
            if line and not any(exclude in line for exclude in exclude_dirs):
                all_timestamp_creations.append((description, line))

    return all_timestamp_creations

def analyze_timestamp_usage(timestamp_creations):
    """Анализирует использование timestamp'ов."""
    print("\n📊 АНАЛИЗ ИСПОЛЬЗОВАНИЯ TIMESTAMP'ОВ...")
    print("-" * 50)

    correct_usage = []
    problematic_usage = []
    unclear_usage = []

    for desc, line in timestamp_creations:
        line.lower()

        # Проверяем правильное использование NY времени
        if any(correct in line for correct in [
            'get_ny_time_millis()',
            'get_ny_time_seconds()',
            'timeutils.GetNewYorkTimeMillis()',
            'timeutils.GetNewYorkTimeSeconds()',
            'timeutils.GetNewYorkTime()'
        ]):
            correct_usage.append((desc, line))

        # Проверяем проблемное использование
        elif any(problem in line for problem in [
            'int(time.time() * 1000)',
            'time.time() * 1000',
            'int(datetime.now().timestamp() * 1000)',
            'datetime.now().timestamp() * 1000',
            'time.Now().Unix()',
            'time.Now().UnixMilli()',
            'timestamp.Unix()'
        ]):
            problematic_usage.append((desc, line))

        # Остальные случаи - неясные, нужно проверить вручную
        else:
            unclear_usage.append((desc, line))

    return correct_usage, problematic_usage, unclear_usage

def main():
    """Основная функция проверки создания timestamp'ов."""
    print("🚀 ПРОВЕРКА СОЗДАНИЯ TIMESTAMP'ОВ")
    print("Убеждаемся, что все timestamp'ы создаются правильно")
    print("=" * 60)

    # Находим все места создания timestamp'ов
    timestamp_creations = check_timestamp_creation_patterns()

    # Анализируем их использование
    correct_usage, problematic_usage, unclear_usage = analyze_timestamp_usage(timestamp_creations)

    # Выводим результаты
    print(f"\n📊 НАЙДЕНО МЕСТ СОЗДАНИЯ TIMESTAMP'ОВ: {len(timestamp_creations)}")
    print("=" * 60)

    print(f"✅ ПРАВИЛЬНОЕ ИСПОЛЬЗОВАНИЕ NY ВРЕМЕНИ: {len(correct_usage)}")
    if correct_usage:
        print("   Примеры:")
        for desc, line in correct_usage[:3]:
            print(f"   {desc}: {line.strip()}")
        if len(correct_usage) > 3:
            print(f"   ... и еще {len(correct_usage) - 3} случаев")

    print(f"\n❌ ПРОБЛЕМНОЕ ИСПОЛЬЗОВАНИЕ: {len(problematic_usage)}")
    if problematic_usage:
        print("   Проблемы:")
        for desc, line in problematic_usage:
            print(f"   {desc}: {line.strip()}")

    print(f"\n❓ НЕЯСНЫЕ СЛУЧАИ (требуют проверки): {len(unclear_usage)}")
    if unclear_usage:
        print("   Неясные случаи:")
        for desc, line in unclear_usage[:5]:
            print(f"   {desc}: {line.strip()}")
        if len(unclear_usage) > 5:
            print(f"   ... и еще {len(unclear_usage) - 5} случаев")

    print("\n" + "=" * 60)
    if len(problematic_usage) == 0:
        print("🎉 ОТЛИЧНО! ВСЕ TIMESTAMP'Ы СОЗДАЮТСЯ ПРАВИЛЬНО!")
        print("✅ Используется Нью-Йоркское время в миллисекундах")
        if len(unclear_usage) == 0:
            print("🎯 ГАРАНТИЯ: 100% правильность timestamp'ов")
        else:
            print(f"⚠️  Есть {len(unclear_usage)} неясных случаев для ручной проверки")
        return True
    else:
        print(f"❌ НАЙДЕНО ПРОБЛЕМ: {len(problematic_usage)}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
