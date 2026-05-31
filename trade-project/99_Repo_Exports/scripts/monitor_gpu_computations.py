#!/usr/bin/env python3
"""
Скрипт для мониторинга GPU вычислений в реальном времени.

Проверяет:
1. Использование GPU методов в коде
2. Текущее состояние GPU
3. Логи контейнеров на вызовы GPU методов
4. Статистику использования batch методов
"""

import subprocess
from typing import Any
from collections import defaultdict

def run_cmd(cmd: str) -> tuple[str, int]:
    """Выполнить команду и вернуть вывод + код возврата."""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=10
        )
        return result.stdout.strip(), result.returncode
    except subprocess.TimeoutExpired:
        return "", 1
    except Exception as e:
        return f"Error: {e}", 1

def check_gpu_status() -> dict[str, Any]:
    """Проверить текущий статус GPU."""
    output, code = run_cmd("nvidia-smi --query-gpu=name,utilization.gpu,utilization.memory,memory.used,memory.total,power.draw --format=csv,noheader,nounits")

    if code != 0:
        return {"available": False, "error": "nvidia-smi not available"}

    lines = output.strip().split('\n')
    if not lines or not lines[0]:
        return {"available": False, "error": "No GPU data"}

    parts = lines[0].split(', ')
    if len(parts) < 6:
        return {"available": False, "error": "Invalid GPU data"}

    try:
        return {
            "available": True,
            "name": parts[0],
            "utilization_gpu": int(parts[1]),
            "utilization_memory": int(parts[2]),
            "memory_used_mb": int(parts[3]),
            "memory_total_mb": int(parts[4]),
            "power_draw": float(parts[5]) if len(parts) > 5 else 0.0,
            "memory_used_percent": round(int(parts[3]) / int(parts[4]) * 100, 2) if int(parts[4]) > 0 else 0.0
        }
    except (ValueError, IndexError):
        return {"available": False, "error": "Failed to parse GPU data"}

def check_gpu_methods_usage() -> dict[str, Any]:
    """Проверить использование GPU методов в коде."""
    gpu_methods = [
        "compute_l2_metrics_batch",
        "feed_batch",
        "compute_depth_sum_batch",
        "_depth_sum_batch",
        "compute_obi_batch",
        "obi_from_book_batch",
        "compute_rolling_stats_batch",
        "compute_order_stats_batch",
        "extract_features_batch",
    ]

    usage = defaultdict(list)

    # Ищем использование методов в коде
    for method in gpu_methods:
        output, code = run_cmd(f"grep -r '{method}' python-worker/ --include='*.py' | grep -v '__pycache__' | head -20")
        if code == 0 and output:
            files = set()
            for line in output.split('\n'):
                if ':' in line:
                    file_path = line.split(':')[0]
                    files.add(file_path)
            if files:
                usage[method] = list(files)

    return dict(usage)

def check_container_gpu_usage(container_name: str) -> dict[str, Any]:
    """Проверить использование GPU в контейнере."""
    result = {
        "container": container_name,
        "gpu_available": False,
        "gpu_enabled": False,
        "methods_called": [],
        "error": None
    }

    # Проверяем переменные окружения
    env_output, _ = run_cmd(f"docker exec {container_name} env | grep -E 'GPU_ENABLED' 2>/dev/null")
    result["gpu_enabled"] = 'GPU_ENABLED=true' in env_output

    # Проверяем доступность GPU в контейнере
    python_check = """
import sys
try:
    from services.gpu_compute_service import get_gpu_service
    gpu = get_gpu_service()
    print(f"GPU_AVAILABLE={gpu.is_gpu_available()}")
    print(f"GPU_ENABLED={gpu.gpu_enabled if hasattr(gpu, 'gpu_enabled') else 'unknown'}")

    # Проверяем доступные batch методы
    batch_methods = [m for m in dir(gpu) if 'batch' in m.lower() and not m.startswith('_')]
    print(f"BATCH_METHODS={','.join(sorted(batch_methods))}")
except Exception as e:
    print(f"ERROR={e}")
    sys.exit(1)
"""

    python_check_escaped = python_check.replace('"', '\\"').replace('\n', ' ')
    output, code = run_cmd(f'docker exec {container_name} python3 -c "{python_check_escaped}" 2>/dev/null')

    if code == 0:
        for line in output.split('\n'):
            if line.startswith('GPU_AVAILABLE='):
                result["gpu_available"] = line.split('=')[1] == 'True'
            elif line.startswith('BATCH_METHODS='):
                methods = line.split('=', 1)[1]
                result["methods_called"] = methods.split(',') if methods else []
            elif line.startswith('ERROR='):
                result["error"] = line.split('=', 1)[1]
    else:
        result["error"] = f"Failed to check GPU: {output}"

    return result

def check_logs_for_gpu_usage(container_name: str, lines: int = 100) -> dict[str, Any]:
    """Проверить логи контейнера на использование GPU."""
    output, code = run_cmd(f"docker logs {container_name} --tail {lines} 2>&1")

    gpu_keywords = [
        "gpu", "GPU", "batch", "Batch", "l2", "L2", "obi", "OBI",
        "compute_l2_metrics_batch", "feed_batch", "compute_obi_batch"
    ]

    matches = defaultdict(int)
    relevant_lines = []

    if code == 0 and output:
        for line in output.split('\n'):
            line_lower = line.lower()
            for keyword in gpu_keywords:
                if keyword.lower() in line_lower:
                    matches[keyword] += 1
                    if len(relevant_lines) < 20:
                        relevant_lines.append(line.strip())

    return {
        "matches": dict(matches),
        "sample_lines": relevant_lines[:10]
    }

def main():
    """Главная функция."""
    print("=" * 80)
    print("🔍 Мониторинг GPU вычислений в scanner_infra")
    print("=" * 80)
    print()

    # 1. Проверка GPU на хосте
    print("📊 1. GPU на хосте (nvidia-smi):")
    print("-" * 80)
    gpu_status = check_gpu_status()
    if gpu_status.get("available"):
        print(f"   ✅ GPU: {gpu_status['name']}")
        print(f"   📈 Utilization: {gpu_status['utilization_gpu']}% (GPU), {gpu_status['utilization_memory']}% (Memory)")
        print(f"   💾 Memory: {gpu_status['memory_used_mb']} MB / {gpu_status['memory_total_mb']} MB ({gpu_status['memory_used_percent']}%)")
        print(f"   ⚡ Power: {gpu_status['power_draw']} W")

        # Анализ использования
        if gpu_status['utilization_gpu'] > 20:
            print(f"   ✅ GPU активно используется ({gpu_status['utilization_gpu']}%)")
        elif gpu_status['utilization_gpu'] > 5:
            print(f"   ⚠️  GPU используется умеренно ({gpu_status['utilization_gpu']}%)")
        else:
            print(f"   ⚠️  GPU используется слабо ({gpu_status['utilization_gpu']}%)")
    else:
        print(f"   ⚠️  GPU недоступен: {gpu_status.get('error', 'Unknown error')}")
    print()

    # 2. Проверка использования методов в коде
    print("💻 2. Использование GPU методов в коде:")
    print("-" * 80)
    usage = check_gpu_methods_usage()
    if usage:
        for method, files in sorted(usage.items()):
            print(f"   ✅ {method}:")
            for file in files[:3]:  # Показываем первые 3 файла
                print(f"      - {file}")
            if len(files) > 3:
                print(f"      ... и еще {len(files) - 3} файлов")
    else:
        print("   ⚠️  GPU методы не найдены в коде (возможно не используются)")
    print()

    # 3. Проверка контейнеров
    print("🐳 3. GPU в Docker контейнерах:")
    print("-" * 80)
    containers = ["scanner_infra-multi-symbol-orderflow-1", "scanner-crypto-orderflow"]
    for container in containers:
        output, code = run_cmd(f"docker ps --format '{{{{.Names}}}}' | grep -E '^{container}$'")
        if code == 0 and output.strip() == container:
            print(f"   📦 {container}:")
            gpu_info = check_container_gpu_usage(container)
            print(f"      GPU enabled (env): {gpu_info['gpu_enabled']}")
            print(f"      GPU available: {gpu_info['gpu_available']}")
            if gpu_info['methods_called']:
                print(f"      Batch methods: {len(gpu_info['methods_called'])} доступно")
                for method in sorted(gpu_info['methods_called'])[:5]:
                    print(f"         - {method}")
            if gpu_info['error']:
                print(f"      ⚠️  Ошибка: {gpu_info['error']}")

            # Проверяем логи
            logs = check_logs_for_gpu_usage(container, lines=200)
            if logs['matches']:
                print(f"      📝 Упоминаний GPU в логах: {sum(logs['matches'].values())}")
                for keyword, count in sorted(logs['matches'].items(), key=lambda x: x[1], reverse=True)[:3]:
                    print(f"         - '{keyword}': {count} раз")
        else:
            print(f"   ⚠️  {container}: контейнер не запущен")
    print()

    # 4. Итоговая сводка
    print("=" * 80)
    print("📋 Итоговая сводка:")
    print("=" * 80)

    if gpu_status.get("available"):
        util = gpu_status['utilization_gpu']
        if util > 30:
            print(f"   ✅ GPU активно используется ({util}%)")
        elif util > 10:
            print(f"   ⚠️  GPU используется умеренно ({util}%)")
        else:
            print(f"   ⚠️  GPU используется слабо ({util}%) - возможно методы не вызываются")
            print("   💡 Рекомендация: проверьте, используются ли batch методы в коде")
    else:
        print("   ⚠️  GPU недоступен на хосте")

    if usage:
        print(f"   ✅ GPU методы найдены в коде ({len(usage)} методов)")
    else:
        print("   ⚠️  GPU методы не найдены в коде - возможно не используются")

    print()
    print("💡 Для увеличения использования GPU:")
    print("   1. Убедитесь что batch методы вызываются для батчей из 5+ элементов")
    print("   2. Проверьте что GPU_ENABLED=true в контейнерах")
    print("   3. Убедитесь что CuPy установлен в контейнерах")
    print("   4. Используйте feed_batch() вместо feed() для множественных книг")
    print()

if __name__ == '__main__':
    main()

