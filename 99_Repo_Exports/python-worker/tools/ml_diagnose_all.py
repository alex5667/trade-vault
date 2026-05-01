from __future__ import annotations
"""
Комплексная диагностика ML: запускает все проверки.

1. Проверка конфигурации
2. Диагностика ошибок
3. Диагностика латентности
4. Список pending предложений guard
"""


import argparse
import os
import subprocess
import sys

# Путь к директории со скриптами
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def run_script(script_name: str, args: list[str] = None) -> int:
    """Запустить Python скрипт и вернуть код возврата."""
    script_path = os.path.join(SCRIPT_DIR, script_name)
    cmd = [sys.executable, script_path] + (args or [])
    print(f"\n{'='*80}")
    print(f"Запуск: {' '.join(cmd)}")
    print(f"{'='*80}\n")
    result = subprocess.run(cmd)
    return result.returncode


def main() -> None:
    ap = argparse.ArgumentParser(description="Комплексная диагностика ML")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--window-min", type=int, default=60, help="Окно анализа в минутах")
    ap.add_argument("--skip-config", action="store_true", help="Пропустить проверку конфигурации")
    ap.add_argument("--skip-errors", action="store_true", help="Пропустить диагностику ошибок")
    ap.add_argument("--skip-latency", action="store_true", help="Пропустить диагностику латентности")
    ap.add_argument("--skip-guard", action="store_true", help="Пропустить список guard предложений")
    args = ap.parse_args()

    common_args = ["--redis-url", args.redis_url]

    print("\n" + "="*80)
    print("ML COMPREHENSIVE DIAGNOSTICS")
    print("="*80)

    errors = []

    # 1. Проверка конфигурации
    if not args.skip_config:
        ret = run_script("ml_check_config.py", common_args)
        if ret != 0:
            errors.append("ml_check_config.py")

    # 2. Диагностика ошибок
    if not args.skip_errors:
        ret = run_script("ml_diagnose_errors.py", common_args + ["--window-min", str(args.window_min)])
        if ret != 0:
            errors.append("ml_diagnose_errors.py")

    # 3. Диагностика латентности
    if not args.skip_latency:
        ret = run_script("ml_diagnose_latency.py", common_args + ["--window-min", str(args.window_min)])
        if ret != 0:
            errors.append("ml_diagnose_latency.py")

    # 4. Список guard предложений
    if not args.skip_guard:
        ret = run_script("ml_guard_approve.py", common_args + ["--action", "list"])
        if ret != 0:
            errors.append("ml_guard_approve.py")

    # Итоги
    print("\n" + "="*80)
    print("ИТОГИ ДИАГНОСТИКИ")
    print("="*80)
    if errors:
        print(f"⚠️  Ошибки в скриптах: {', '.join(errors)}")
        sys.exit(1)
    else:
        print("✅ Все проверки завершены")
        sys.exit(0)


if __name__ == "__main__":
    main()

