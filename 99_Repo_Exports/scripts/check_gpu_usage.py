#!/usr/bin/env python3
"""
Скрипт для проверки использования GPU ресурсов в scanner_infra.

Проверяет:
1. Доступность GPU на хосте
2. Использование GPU в контейнерах
3. GPU сервис в Python worker
4. Метрики использования (utilization, memory, temperature)
"""

import os
import subprocess
from typing import Any

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

def check_nvidia_smi() -> dict[str, Any] | None:
    """Проверить GPU через nvidia-smi."""
    output, code = run_cmd("nvidia-smi --query-gpu=name,utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu,power.draw --format=csv,noheader,nounits")

    if code != 0:
        return None

    lines = output.strip().split('\n')
    if not lines or not lines[0]:
        return None

    # Парсим первую строку (первый GPU)
    parts = lines[0].split(', ')
    if len(parts) < 7:
        return None

    try:
        return {
            'name': parts[0],
            'utilization_gpu': int(parts[1]),
            'utilization_memory': int(parts[2]),
            'memory_used_mb': int(parts[3]),
            'memory_total_mb': int(parts[4]),
            'temperature': int(parts[5]),
            'power_draw': float(parts[6]) if len(parts) > 6 else 0.0,
            'memory_used_percent': round(int(parts[3]) / int(parts[4]) * 100, 2) if int(parts[4]) > 0 else 0.0
        }
    except (ValueError, IndexError):
        return None

def check_docker_gpu_containers() -> list[dict[str, Any]]:
    """Проверить контейнеры с GPU доступом."""
    containers = []

    # Найти контейнеры с GPU
    output, code = run_cmd("docker ps --format '{{.Names}}\t{{.Status}}' | grep -E 'orderflow|candle'")
    if code != 0:
        return containers

    for line in output.strip().split('\n'):
        if not line:
            continue
        parts = line.split('\t')
        if len(parts) >= 2:
            containers.append({
                'name': parts[0],
                'status': parts[1]
            })

    return containers

def check_gpu_in_container(container_name: str) -> dict[str, Any] | None:
    """Проверить GPU в контейнере."""
    # Проверить переменные окружения
    env_output, _ = run_cmd(f"docker exec {container_name} env | grep -E 'GPU_ENABLED|NVIDIA'")

    gpu_enabled = 'GPU_ENABLED=true' in env_output

    # Попробовать выполнить Python проверку GPU
    python_check = """
import sys
try:
    import cupy as cp
    if cp.cuda.is_available():
        device_count = cp.cuda.runtime.getDeviceCount()
        if device_count > 0:
            props = cp.cuda.runtime.getDeviceProperties(0)
            gpu_name = props['name'].decode() if isinstance(props['name'], bytes) else props['name']
            memory_total = props['totalGlobalMem'] / (1024**3)
            print("GPU_AVAILABLE=True")
            print(f"GPU_NAME={gpu_name}")
            print(f"GPU_MEMORY_GB={memory_total:.2f}")
        else:
            print("GPU_AVAILABLE=False")
    else:
        print("GPU_AVAILABLE=False")
except Exception as e:
    print(f"GPU_ERROR={e}")
    sys.exit(1)
"""

    # Экранируем кавычки для правильной передачи в docker exec
    python_check_escaped = python_check.replace('"', '\\"').replace('\n', ' ')
    output, code = run_cmd(f'docker exec {container_name} python3 -c "{python_check_escaped}"')

    result = {
        'container': container_name,
        'gpu_enabled_env': gpu_enabled,
        'gpu_available': False,
        'gpu_name': None,
        'gpu_memory_gb': None,
        'error': None
    }

    if code == 0:
        for line in output.split('\n'):
            if line.startswith('GPU_AVAILABLE='):
                result['gpu_available'] = line.split('=')[1] == 'True'
            elif line.startswith('GPU_NAME='):
                result['gpu_name'] = line.split('=', 1)[1]
            elif line.startswith('GPU_MEMORY_GB='):
                result['gpu_memory_gb'] = float(line.split('=')[1])
            elif line.startswith('GPU_ERROR='):
                result['error'] = line.split('=', 1)[1]
    else:
        result['error'] = f"Failed to check GPU in container: {output}"

    return result

def check_gpu_service_usage() -> dict[str, Any]:
    """Проверить использование GPU сервиса в коде."""
    gpu_files = [
        'python-worker/services/gpu_compute_service.py',
        'python-worker/handlers/base_orderflow_handler.py',
        'python-worker/of/candle_of_worker.py',
        'python-worker/core/unified_signal_generator.py',
        'python-worker/services/book_analytics_service.py',
    ]

    usage = {
        'files_using_gpu': [],
        'methods_called': [],
        'total_usage_count': 0
    }

    gpu_methods = [
        'compute_robust_zscore_mad',
        'compute_delta_batch',
        'compute_z_scores',
        'compute_ema_batch',
        'compute_rsi_batch',
        'compute_macd_batch',
        'compute_atr_batch',
        'compute_obi_metrics_batch',
        'process_candles_batch',
        'compute_rolling_mean_std',
    ]

    for file_path in gpu_files:
        if not os.path.exists(file_path):
            continue

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

            if 'gpu_service' in content.lower() or 'gpu_compute_service' in content.lower():
                usage['files_using_gpu'].append(file_path)

                for method in gpu_methods:
                    if method in content:
                        usage['methods_called'].append(f"{os.path.basename(file_path)}:{method}")
                        usage['total_usage_count'] += 1

    return usage

def main():
    """Главная функция."""
    print("=" * 80)
    print("🔍 Проверка использования GPU ресурсов в scanner_infra")
    print("=" * 80)
    print()

    # 1. Проверка GPU на хосте
    print("📊 1. GPU на хосте (nvidia-smi):")
    print("-" * 80)
    gpu_info = check_nvidia_smi()
    if gpu_info:
        print(f"   ✅ GPU: {gpu_info['name']}")
        print(f"   📈 Utilization: {gpu_info['utilization_gpu']}% (GPU), {gpu_info['utilization_memory']}% (Memory)")
        print(f"   💾 Memory: {gpu_info['memory_used_mb']} MB / {gpu_info['memory_total_mb']} MB ({gpu_info['memory_used_percent']}%)")
        print(f"   🌡️  Temperature: {gpu_info['temperature']}°C")
        if gpu_info['power_draw'] > 0:
            print(f"   ⚡ Power: {gpu_info['power_draw']} W")
    else:
        print("   ⚠️  nvidia-smi не доступен (возможно нет GPU или драйверов)")
    print()

    # 2. Проверка контейнеров
    print("🐳 2. Docker контейнеры с GPU:")
    print("-" * 80)
    containers = check_docker_gpu_containers()
    if containers:
        for container in containers:
            print(f"   📦 {container['name']}: {container['status']}")
            gpu_status = check_gpu_in_container(container['name'])
            if gpu_status:
                if gpu_status['gpu_available']:
                    print(f"      ✅ GPU доступен: {gpu_status['gpu_name']}")
                    if gpu_status['gpu_memory_gb']:
                        print(f"      💾 GPU Memory: {gpu_status['gpu_memory_gb']:.2f} GB")
                else:
                    print("      ⚠️  GPU недоступен")
                    if gpu_status['error']:
                        print(f"      ❌ Ошибка: {gpu_status['error']}")
    else:
        print("   ⚠️  Контейнеры с GPU не найдены")
    print()

    # 3. Проверка использования в коде
    print("💻 3. Использование GPU в коде:")
    print("-" * 80)
    usage = check_gpu_service_usage()
    print(f"   📁 Файлов с GPU: {len(usage['files_using_gpu'])}")
    for file in usage['files_using_gpu']:
        print(f"      - {file}")
    print(f"   🔧 Методов GPU: {len(usage['methods_called'])}")
    for method in set(usage['methods_called']):
        print(f"      - {method}")
    print(f"   📊 Всего использований: {usage['total_usage_count']}")
    print()

    # 4. Итоговая сводка
    print("=" * 80)
    print("📋 Итоговая сводка:")
    print("=" * 80)

    if gpu_info:
        if gpu_info['utilization_gpu'] > 10:
            print(f"   ✅ GPU активно используется ({gpu_info['utilization_gpu']}%)")
        elif gpu_info['utilization_gpu'] > 0:
            print(f"   ⚠️  GPU используется слабо ({gpu_info['utilization_gpu']}%)")
        else:
            print("   ⚠️  GPU не используется (0%)")

        if gpu_info['memory_used_percent'] > 10:
            print(f"   ✅ GPU память используется ({gpu_info['memory_used_percent']}%)")
        else:
            print(f"   ⚠️  GPU память используется слабо ({gpu_info['memory_used_percent']}%)")
    else:
        print("   ⚠️  GPU недоступен на хосте")

    if containers:
        print(f"   ✅ Найдено контейнеров с GPU: {len(containers)}")
    else:
        print("   ⚠️  Контейнеры с GPU не найдены")

    if usage['total_usage_count'] > 0:
        print(f"   ✅ GPU используется в коде ({usage['total_usage_count']} мест)")
    else:
        print("   ⚠️  GPU не используется в коде")

    print()

if __name__ == '__main__':
    main()

