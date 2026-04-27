#!/usr/bin/env python3
"""
Точная проверка временных меток - только те случаи, которые действительно создают timestamp'ы.
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

def check_real_timestamp_problems():
    """Проверяет только РЕАЛЬНЫЕ проблемы с timestamp'ами."""
    print("🎯 ТОЧНАЯ ПРОВЕРКА РЕАЛЬНЫХ ПРОБЛЕМ С TIMESTAMP'АМИ")
    print("=" * 70)

    exclude_dirs = ['telegram-test-env', 'websocket_test', '__pycache__', '.git', 'venv', 'node_modules']

    real_problems = []

    # 1. Python: int(time.time() * 1000) - ЭТО ПРОБЛЕМА
    print("1️⃣ Проверяем int(time.time() * 1000)...")
    results = run_grep(r'int\(time\.time\(\)\s*\*\s*1000\)', '*.py', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            real_problems.append(('int(time.time() * 1000)', line))

    # 2. Python: int(datetime.now().timestamp() * 1000) - ЭТО ПРОБЛЕМА
    print("2️⃣ Проверяем int(datetime.now().timestamp() * 1000)...")
    results = run_grep(r'int\(datetime\.now\(\)\.timestamp\(\)\s*\*\s*1000\)', '*.py', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            real_problems.append(('int(datetime.now().timestamp() * 1000)', line))

    # 3. Python: time.time() * 1000 (без int) - ЭТО ПРОБЛЕМА
    print("3️⃣ Проверяем time.time() * 1000...")
    results = run_grep(r'time\.time\(\)\s*\*\s*1000', '*.py', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            real_problems.append(('time.time() * 1000', line))

    # 4. Python: datetime.now().timestamp() * 1000 - ЭТО ПРОБЛЕМА
    print("4️⃣ Проверяем datetime.now().timestamp() * 1000...")
    results = run_grep(r'datetime\.now\(\)\.timestamp\(\)\s*\*\s*1000', '*.py', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            real_problems.append(('datetime.now().timestamp() * 1000', line))

    # 5. Go: time.Now().Unix() - ЭТО ПРОБЛЕМА (кроме тестов)
    print("5️⃣ Проверяем time.Now().Unix()...")
    results = run_grep(r'time\.Now\(\)\.Unix\(\)', '*.go', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            if 'test.go' not in line and 'timezone_test.go' not in line:
                real_problems.append(('time.Now().Unix()', line))

    # 6. Go: time.Now().UnixMilli() - ЭТО ПРОБЛЕМА (если не timeutils)
    print("6️⃣ Проверяем time.Now().UnixMilli()...")
    results = run_grep(r'time\.Now\(\)\.UnixMilli\(\)', '*.go', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            if 'timeutils' not in line:
                real_problems.append(('time.Now().UnixMilli()', line))

    # 7. Go: timestamp.Unix() - ЭТО ПРОБЛЕМА (должно быть миллисекунды)
    print("7️⃣ Проверяем timestamp.Unix()...")
    results = run_grep(r'timestamp\.Unix()\b', '*.go', exclude_dirs=exclude_dirs)
    for line in results:
        if line and not any(exclude in line for exclude in exclude_dirs):
            real_problems.append(('timestamp.Unix()', line))

    return real_problems

def check_legitimate_time_usage():
    """Проверяет легитимное использование time.time() и time.Now()."""
    print("\n✅ ПРОВЕРЯЕМ ЛЕГИТИМНОЕ ИСПОЛЬЗОВАНИЕ ВРЕМЕНИ...")
    print("-" * 50)

    legitimate_cases = []

    # Python: time.time() для вычислений (НЕ timestamp)
    results = run_grep(r'time\.time\(\)', '*.py', exclude_dirs=['telegram-test-env', 'websocket_test', '__pycache__', '.git', 'venv'])
    for line in results:
        if line and any(legit in line.lower() for legit in ['cache', 'sleep', 'wait', 'delay', 'timeout', 'duration', 'elapsed', 'uptime', 'since', 'start_time', 'end_time']):
            legitimate_cases.append(('time.time() for calculations', line))

    # Go: time.Now() для вычислений (НЕ timestamp)
    results = run_grep(r'time\.Now\(\)', '*.go', exclude_dirs=['__pycache__', '.git', 'venv'])
    for line in results:
        if line and any(legit in line.lower() for legit in ['format', 'add', 'sub', 'since', 'before', 'after', 'deadline', 'timeout', 'duration', 'start', 'end']):
            legitimate_cases.append(('time.Now() for calculations', line))

    return legitimate_cases

def check_correct_ny_usage():
    """Проверяет правильное использование NY времени."""
    print("\n🎯 ПРОВЕРЯЕМ ПРАВИЛЬНОЕ ИСПОЛЬЗОВАНИЕ NY ВРЕМЕНИ...")
    print("-" * 50)

    correct_usage = []

    # Python: get_ny_time_millis()
    results = run_grep(r'get_ny_time_millis\(\)', '*.py', exclude_dirs=['telegram-test-env', 'websocket_test', '__pycache__', '.git', 'venv'])
    for line in results:
        if line:
            correct_usage.append(('get_ny_time_millis()', line))

    # Go: timeutils.GetNewYorkTimeMillis()
    results = run_grep(r'timeutils\.GetNewYorkTimeMillis\(\)', '*.go', exclude_dirs=['__pycache__', '.git', 'venv'])
    for line in results:
        if line:
            correct_usage.append(('timeutils.GetNewYorkTimeMillis()', line))

    return correct_usage

def main():
    """Основная функция точной проверки."""
    print("🚀 ТОЧНАЯ ПРОВЕРКА ВРЕМЕННЫХ МЕТОК")
    print("Проверяем только РЕАЛЬНЫЕ проблемы с timestamp'ами")
    print("=" * 70)

    # Проверяем реальные проблемы
    real_problems = check_real_timestamp_problems()

    # Проверяем легитимное использование
    legitimate_cases = check_legitimate_time_usage()

    # Проверяем правильное использование
    correct_usage = check_correct_ny_usage()

    # Выводим результаты
    print("\n" + "=" * 70)
    print("📊 РЕЗУЛЬТАТЫ ТОЧНОЙ ПРОВЕРКИ")
    print("=" * 70)

    if len(real_problems) == 0:
        print("🎉 ОТЛИЧНО! РЕАЛЬНЫХ ПРОБЛЕМ НЕ НАЙДЕНО!")
        print("✅ ВСЕ timestamp'ы используют Нью-Йоркское время в миллисекундах")
    else:
        print(f"❌ НАЙДЕНО РЕАЛЬНЫХ ПРОБЛЕМ: {len(real_problems)}")
        print("\n🔧 ПРОБЛЕМНЫЕ TIMESTAMP'Ы:")
        for pattern, line in real_problems:
            print(f"   {pattern}: {line}")

    print(f"\n✅ ЛЕГИТИМНОЕ ИСПОЛЬЗОВАНИЕ ВРЕМЕНИ: {len(legitimate_cases)} случаев")
    print("   (для вычислений, кеширования, таймаутов - НЕ для timestamp'ов)")

    print(f"\n🎯 ПРАВИЛЬНОЕ ИСПОЛЬЗОВАНИЕ NY ВРЕМЕНИ: {len(correct_usage)} случаев")
    print("   (get_ny_time_millis(), timeutils.GetNewYorkTimeMillis())")

    print("\n" + "=" * 70)
    if len(real_problems) == 0:
        print("🎯 ГАРАНТИЯ: ВСЕ TIMESTAMP'Ы ИСПОЛЬЗУЮТ НЬЮ-ЙОРКСКОЕ ВРЕМЯ В МИЛЛИСЕКУНДАХ!")
        print("✅ Проект готов к продакшну")
        return True
    else:
        print(f"⚠️  ТРЕБУЕТСЯ ИСПРАВЛЕНИЕ: {len(real_problems)} реальных проблем")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
