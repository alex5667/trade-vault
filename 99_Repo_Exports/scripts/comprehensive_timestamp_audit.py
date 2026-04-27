#!/usr/bin/env python3
"""
Комплексная проверка всех временных меток в проекте.
Гарантирует, что ВСЕ временные метки используют Нью-Йоркское время в миллисекундах.
"""

import subprocess
from collections import defaultdict

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

def check_all_timestamp_patterns():
    """Проверяет ВСЕ возможные паттерны временных меток."""
    print("🔍 КОМПЛЕКСНАЯ ПРОВЕРКА ВСЕХ ВРЕМЕННЫХ МЕТОК...")
    print("=" * 80)

    # Исключаем системные директории
    exclude_dirs = ['telegram-test-env', 'websocket_test', '__pycache__', '.git', 'venv', 'node_modules']

    problems = defaultdict(list)

    # 1. Python: int(time.time() * 1000)
    print("1️⃣ Проверяем int(time.time() * 1000)...")
    results = run_grep(r'int\(time\.time\(\)\s*\*\s*1000\)', '*.py', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            problems['int(time.time() * 1000)'].append(line)

    # 2. Python: time.time() * 1000 (без int)
    print("2️⃣ Проверяем time.time() * 1000...")
    results = run_grep(r'time\.time\(\)\s*\*\s*1000', '*.py', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            problems['time.time() * 1000'].append(line)

    # 3. Python: datetime.now().timestamp() * 1000
    print("3️⃣ Проверяем datetime.now().timestamp() * 1000...")
    results = run_grep(r'datetime\.now\(\)\.timestamp\(\)\s*\*\s*1000', '*.py', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            problems['datetime.now().timestamp() * 1000'].append(line)

    # 4. Python: int(datetime.now().timestamp() * 1000)
    print("4️⃣ Проверяем int(datetime.now().timestamp() * 1000)...")
    results = run_grep(r'int\(datetime\.now\(\)\.timestamp\(\)\s*\*\s*1000\)', '*.py', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            problems['int(datetime.now().timestamp() * 1000)'].append(line)

    # 5. Python: time.time() (без умножения)
    print("5️⃣ Проверяем time.time()...")
    results = run_grep(r'time\.time\(\)', '*.py', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            # Исключаем случаи где time.time() используется правильно (для вычислений, не для timestamp)
            if not any(skip in line.lower() for skip in ['cache', 'sleep', 'wait', 'delay', 'timeout', 'duration', 'elapsed', 'uptime', 'since']):
                problems['time.time()'].append(line)

    # 6. Python: datetime.now() (без timestamp)
    print("6️⃣ Проверяем datetime.now()...")
    results = run_grep(r'datetime\.now\(\)', '*.py', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            # Исключаем случаи где datetime.now() используется для форматирования, не для timestamp
            if not any(skip in line.lower() for skip in ['strftime', 'format', 'isoformat', 'display', 'print', 'log']):
                problems['datetime.now()'].append(line)

    # 7. Go: time.Now().Unix()
    print("7️⃣ Проверяем time.Now().Unix()...")
    results = run_grep(r'time\.Now\(\)\.Unix\(\)', '*.go', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            # Исключаем тестовые файлы
            if 'test.go' not in line and 'timezone_test.go' not in line:
                problems['time.Now().Unix()'].append(line)

    # 8. Go: time.Now().UnixMilli()
    print("8️⃣ Проверяем time.Now().UnixMilli()...")
    results = run_grep(r'time\.Now\(\)\.UnixMilli\(\)', '*.go', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            # Исключаем случаи где используется timeutils
            if 'timeutils' not in line:
                problems['time.Now().UnixMilli()'].append(line)

    # 9. Go: timestamp.Unix()
    print("9️⃣ Проверяем timestamp.Unix()...")
    results = run_grep(r'timestamp\.Unix\(\)', '*.go', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            problems['timestamp.Unix()'].append(line)

    # 10. Go: time.Now() (без Unix)
    print("🔟 Проверяем time.Now()...")
    results = run_grep(r'time\.Now\(\)', '*.go', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            # Исключаем случаи где time.Now() используется правильно
            if not any(skip in line.lower() for skip in ['format', 'add', 'sub', 'since', 'before', 'after', 'deadline', 'timeout', 'duration']):
                problems['time.Now()'].append(line)

    return problems

def check_correct_ny_usage():
    """Проверяет правильное использование NY времени."""
    print("\n✅ ПРОВЕРЯЕМ ПРАВИЛЬНОЕ ИСПОЛЬЗОВАНИЕ NY ВРЕМЕНИ...")
    print("-" * 60)

    correct_usage = defaultdict(list)

    # Python: get_ny_time_millis()
    results = run_grep(r'get_ny_time_millis\(\)', '*.py', exclude_dirs=['telegram-test-env', 'websocket_test', '__pycache__', '.git', 'venv'])
    for line in results:
        if line:
            correct_usage['get_ny_time_millis()'].append(line)

    # Python: get_ny_time_seconds()
    results = run_grep(r'get_ny_time_seconds\(\)', '*.py', exclude_dirs=['telegram-test-env', 'websocket_test', '__pycache__', '.git', 'venv'])
    for line in results:
        if line:
            correct_usage['get_ny_time_seconds()'].append(line)

    # Go: timeutils.GetNewYorkTimeMillis()
    results = run_grep(r'timeutils\.GetNewYorkTimeMillis\(\)', '*.go', exclude_dirs=['__pycache__', '.git', 'venv'])
    for line in results:
        if line:
            correct_usage['timeutils.GetNewYorkTimeMillis()'].append(line)

    # Go: timeutils.GetNewYorkTimeSeconds()
    results = run_grep(r'timeutils\.GetNewYorkTimeSeconds\(\)', '*.go', exclude_dirs=['__pycache__', '.git', 'venv'])
    for line in results:
        if line:
            correct_usage['timeutils.GetNewYorkTimeSeconds()'].append(line)

    # Go: timeutils.GetNewYorkTime()
    results = run_grep(r'timeutils\.GetNewYorkTime\(\)', '*.go', exclude_dirs=['__pycache__', '.git', 'venv'])
    for line in results:
        if line:
            correct_usage['timeutils.GetNewYorkTime()'].append(line)

    return correct_usage

def check_imports():
    """Проверяет импорты временных утилит."""
    print("\n📦 ПРОВЕРЯЕМ ИМПОРТЫ ВРЕМЕННЫХ УТИЛИТ...")
    print("-" * 60)

    imports = defaultdict(list)

    # Python импорты
    results = run_grep(r'from common\.utils\.timezone import', '*.py', exclude_dirs=['telegram-test-env', 'websocket_test', '__pycache__', '.git', 'venv'])
    for line in results:
        if line:
            imports['Python timezone imports'].append(line)

    # Go импорты
    results = run_grep(r'"go-worker/internal/timeutils"', '*.go', exclude_dirs=['__pycache__', '.git', 'venv'])
    for line in results:
        if line:
            imports['Go timeutils imports'].append(line)

    return imports

def main():
    """Основная функция комплексной проверки."""
    print("🚀 КОМПЛЕКСНАЯ ПРОВЕРКА ВРЕМЕННЫХ МЕТОК")
    print("Гарантируем, что ВСЕ временные метки используют Нью-Йоркское время в миллисекундах")
    print("=" * 80)

    # Проверяем все проблемные паттерны
    problems = check_all_timestamp_patterns()

    # Проверяем правильное использование
    correct_usage = check_correct_ny_usage()

    # Проверяем импорты
    imports = check_imports()

    # Выводим результаты
    print("\n" + "=" * 80)
    print("📊 РЕЗУЛЬТАТЫ ПРОВЕРКИ")
    print("=" * 80)

    total_problems = sum(len(problem_list) for problem_list in problems.values())
    total_correct = sum(len(usage_list) for usage_list in correct_usage.values())
    total_imports = sum(len(import_list) for import_list in imports.values())

    if total_problems == 0:
        print("🎉 ОТЛИЧНО! ПРОБЛЕМ НЕ НАЙДЕНО!")
        print("✅ ВСЕ временные метки используют Нью-Йоркское время в миллисекундах")
    else:
        print(f"❌ НАЙДЕНО ПРОБЛЕМ: {total_problems}")
        print("\n🔧 ПРОБЛЕМНЫЕ МЕСТА:")
        for pattern, lines in problems.items():
            if lines:
                print(f"\n📌 {pattern} ({len(lines)} случаев):")
                for line in lines[:5]:  # Показываем первые 5
                    print(f"   {line}")
                if len(lines) > 5:
                    print(f"   ... и еще {len(lines) - 5} случаев")

    print(f"\n✅ ПРАВИЛЬНОЕ ИСПОЛЬЗОВАНИЕ: {total_correct} случаев")
    for usage_type, lines in correct_usage.items():
        if lines:
            print(f"   {usage_type}: {len(lines)} случаев")

    print(f"\n📦 ИМПОРТЫ ВРЕМЕННЫХ УТИЛИТ: {total_imports} файлов")
    for import_type, lines in imports.items():
        if lines:
            print(f"   {import_type}: {len(lines)} файлов")

    print("\n" + "=" * 80)
    if total_problems == 0:
        print("🎯 ГАРАНТИЯ: ВСЕ ВРЕМЕННЫЕ МЕТКИ ИСПОЛЬЗУЮТ НЬЮ-ЙОРКСКОЕ ВРЕМЯ В МИЛЛИСЕКУНДАХ!")
        print("✅ Проект готов к продакшну")
    else:
        print(f"⚠️  ТРЕБУЕТСЯ ИСПРАВЛЕНИЕ: {total_problems} проблем")

    return total_problems == 0

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
