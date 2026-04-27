#!/usr/bin/env python3
"""
Скрипт для проверки использования GPU вычислений в scanner_infra.

Проверяет:
1. Текущее использование GPU (utilization, memory)
2. Вызовы GPU методов в коде
3. Логи с информацией о GPU вычислениях
4. Статистику использования GPU методов
"""

import os
import subprocess
from typing import Any
from pathlib import Path

# Добавляем путь к python-worker
project_root = Path(__file__).parent.parent


def check_nvidia_smi() -> dict[str, Any]:
    """Проверить статус GPU через nvidia-smi."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            parts = result.stdout.strip().split(", ")
            if len(parts) >= 5:
                return {
                    "available": True,
                    "utilization_gpu": int(parts[0]),
                    "utilization_memory": int(parts[1]),
                    "memory_used_mb": int(parts[2]),
                    "memory_total_mb": int(parts[3]),
                    "temperature": int(parts[4])
                }
    except Exception as e:
        return {"available": False, "error": str(e)}
    return {"available": False, "error": "nvidia-smi did not return parseable output"}


def check_gpu_service() -> dict[str, Any]:
    """Проверить доступность GPU сервиса."""
    try:
        from services.gpu_compute_service import get_gpu_service
        service = get_gpu_service()

        return {
            "available": True,
            "use_gpu": service.use_gpu,
            "device_info": service.get_device_info() if service.is_gpu_available() else None,
            "gpu_available": service.is_gpu_available()
        }
    except Exception as e:
        return {"available": False, "error": str(e)}


def count_gpu_method_calls() -> dict[str, Any]:
    """Подсчитать количество вызовов GPU методов в коде."""
    gpu_methods = [
        "compute_robust_zscore_mad",
        "compute_z_scores",
        "compute_rolling_mean_std",
        "compute_delta_batch",
        "compute_cvd",
        "compute_atr_batch",
        "compute_ema_batch",
        "compute_rsi_batch",
        "compute_macd_batch",
        "compute_obi_batch",
        "process_candles_batch",
        "compute_depth_sum_batch",
        "compute_l2_metrics_batch",
        "_to_gpu",
    ]

    usage = {}
    python_worker_dir = project_root / "python-worker"

    if not python_worker_dir.exists():
        return {"error": "python-worker directory not found"}

    for method in gpu_methods:
        usage[method] = {
            "files": [],
            "count": 0
        }

        # Ищем все Python файлы
        for py_file in python_worker_dir.rglob("*.py"):
            try:
                content = py_file.read_text(encoding="utf-8")

                # Подсчитываем количество вызовов
                count = content.count(method)
                if count > 0:
                    relative_path = str(py_file.relative_to(project_root))
                    usage[method]["files"].append({
                        "file": relative_path,
                        "count": count
                    })
                    usage[method]["count"] += count
            except Exception:
                pass

    return usage


def check_container_logs(container_name: str = "scanner_infra-multi-symbol-orderflow-1") -> dict[str, Any]:
    """Проверить логи контейнера на наличие GPU информации."""
    try:
        # Проверяем последние 200 строк логов
        result = subprocess.run(
            ["docker", "logs", "--tail", "200", container_name],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            return {"available": False, "error": "Container not found or error"}

        logs = result.stdout
        gpu_mentions = []

        gpu_keywords = [
            "GPU", "gpu", "cupy", "CUDA", "acceleration", "compute_",
            "batch", "🚀", "💻", "📊"
        ]

        lines = logs.split("\n")
        for i, line in enumerate(lines):
            for keyword in gpu_keywords:
                if keyword.lower() in line.lower():
                    gpu_mentions.append({
                        "line": i + 1,
                        "content": line.strip()[:150]  # Первые 150 символов
                    })
                    break

        return {
            "available": True,
            "total_lines": len(lines),
            "gpu_mentions": gpu_mentions[:50],  # Первые 50 упоминаний
            "gpu_mention_count": len(gpu_mentions)
        }
    except Exception as e:
        return {"available": False, "error": str(e)}


def analyze_gpu_usage_patterns() -> dict[str, Any]:
    """Анализировать паттерны использования GPU."""
    patterns = {
        "batch_processing": {
            "files": [],
            "methods": ["process_candles_batch", "compute_*_batch"]
        },
        "rolling_calculations": {
            "files": [],
            "methods": ["compute_rolling_mean_std", "compute_z_scores"]
        },
        "orderflow": {
            "files": [],
            "methods": ["compute_delta_batch", "compute_cvd", "compute_obi_batch"]
        }
    }

    python_worker_dir = project_root / "python-worker"

    for pattern_name, pattern_info in patterns.items():
        for py_file in python_worker_dir.rglob("*.py"):
            try:
                content = py_file.read_text(encoding="utf-8")

                for method_pattern in pattern_info["methods"]:
                    if method_pattern.endswith("_batch"):
                        # Ищем методы с _batch
                        if f"{method_pattern}" in content or method_pattern.replace("*", "") in content:
                            relative_path = str(py_file.relative_to(project_root))
                            if relative_path not in patterns[pattern_name]["files"]:
                                patterns[pattern_name]["files"].append(relative_path)
                    else:
                        if method_pattern in content:
                            relative_path = str(py_file.relative_to(project_root))
                            if relative_path not in patterns[pattern_name]["files"]:
                                patterns[pattern_name]["files"].append(relative_path)
            except Exception:
                pass

    return patterns


def main():
    """Главная функция."""
    print("=" * 80)
    print("🔍 Проверка использования GPU вычислений в scanner_infra")
    print("=" * 80)
    print()

    # 1. Проверка GPU через nvidia-smi
    print("📊 1. Статус GPU (nvidia-smi)")
    print("-" * 80)
    gpu_status = check_nvidia_smi()
    if gpu_status.get("available"):
        print("   ✅ GPU доступен")
        print(f"   Utilization: {gpu_status['utilization_gpu']}%")
        print(f"   Memory: {gpu_status['memory_used_mb']} MB / {gpu_status['memory_total_mb']} MB ({gpu_status['memory_used_mb']*100//gpu_status['memory_total_mb']}%)")
        print(f"   Temperature: {gpu_status['temperature']}°C")
    else:
        print(f"   ❌ GPU недоступен: {gpu_status.get('error', 'Unknown error')}")
    print()

    # 2. Проверка GPU сервиса
    print("🔧 2. Статус GPU сервиса")
    print("-" * 80)
    service_status = check_gpu_service()
    if service_status.get("available"):
        print(f"   GPU enabled: {service_status.get('use_gpu', False)}")
        print(f"   GPU available: {service_status.get('gpu_available', False)}")
        if service_status.get("device_info"):
            device = service_status["device_info"]
            print(f"   Device: {device.get('name', 'Unknown')}")
            print(f"   Memory: {device.get('memory_total', 0) / 1024**3:.2f} GB")
    else:
        print(f"   ❌ Ошибка: {service_status.get('error', 'Unknown error')}")
    print()

    # 3. Подсчет вызовов GPU методов
    print("📈 3. Использование GPU методов в коде")
    print("-" * 80)
    method_usage = count_gpu_method_calls()
    if "error" not in method_usage:
        total_calls = 0
        for method, info in method_usage.items():
            if info["count"] > 0:
                total_calls += info["count"]
                print(f"   {method}: {info['count']} вызов(ов) в {len(info['files'])} файл(ах)")
                if len(info['files']) <= 3:
                    for file_info in info['files']:
                        print(f"      - {file_info['file']}: {file_info['count']} раз(а)")

        if total_calls == 0:
            print("   ⚠️  GPU методы не найдены в коде")
        else:
            print(f"\n   Всего: {total_calls} вызов(ов) GPU методов")
    print()

    # 4. Анализ паттернов использования
    print("🔍 4. Паттерны использования GPU")
    print("-" * 80)
    patterns = analyze_gpu_usage_patterns()
    for pattern_name, pattern_info in patterns.items():
        file_count = len(pattern_info["files"])
        if file_count > 0:
            print(f"   {pattern_name}: {file_count} файл(ов)")
            for file_path in pattern_info["files"][:5]:  # Показываем первые 5
                print(f"      - {file_path}")
            if file_count > 5:
                print(f"      ... и еще {file_count - 5} файл(ов)")
    print()

    # 5. Проверка логов контейнера
    print("📋 5. Анализ логов контейнера")
    print("-" * 80)
    container_name = os.getenv("CONTAINER_NAME", "scanner_infra-multi-symbol-orderflow-1")
    logs_info = check_container_logs(container_name)
    if logs_info.get("available"):
        print(f"   Всего строк в логах: {logs_info['total_lines']}")
        print(f"   Упоминаний GPU: {logs_info['gpu_mention_count']}")
        if logs_info['gpu_mention_count'] > 0:
            print("\n   Последние упоминания GPU:")
            for mention in logs_info['gpu_mentions'][:10]:
                print(f"      Line {mention['line']}: {mention['content']}")
    else:
        print(f"   ⚠️  Не удалось получить логи: {logs_info.get('error', 'Unknown error')}")
    print()

    # Итоговая оценка
    print("=" * 80)
    print("📊 Итоговая оценка")
    print("=" * 80)

    if gpu_status.get("available") and gpu_status.get("utilization_gpu", 0) > 0:
        utilization = gpu_status["utilization_gpu"]
        if utilization > 50:
            print("   ✅ GPU активно используется (>50%)")
        elif utilization > 20:
            print(f"   ⚠️  GPU используется умеренно ({utilization}%)")
        else:
            print(f"   ⚠️  GPU используется слабо ({utilization}%)")
    else:
        print("   ❌ GPU не используется или недоступен")

    print()


if __name__ == "__main__":
    main()

