from __future__ import annotations
""",
Безопасный промоут challenger в champion для ML Confirm Gate.

ВАЖНО: После промоута challenger обычно пустой. Если вы повторите этот скрипт позже,
то v станет пустым и вы запишете пустой champion → снова вернётся ERR_NO_CFG.

Поэтому используется "безопасный" вариант с проверкой на пустой challenger.

Использование:
    python3 -m tools.promote_ml_confirm_champion_safe
    python3 -m tools.promote_ml_confirm_champion_safe --redis-url redis://redis-worker-1:6379/0
    python3 -m tools.promote_ml_confirm_champion_safe --dry-run  # только проверка, без изменений
""",


import argparse
import json
import os
import sys
from typing import Any, Dict, Optional

import redis


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Безопасный промоут challenger в champion для ML Confirm Gate",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  # Промоут challenger в champion
  python3 -m tools.promote_ml_confirm_champion_safe

  # Только проверка (dry-run)
  python3 -m tools.promote_ml_confirm_champion_safe --dry-run

  # С кастомным Redis
  python3 -m tools.promote_ml_confirm_champion_safe --redis-url redis://localhost:6379/0

ВАЖНО: После промоута challenger обычно пустой. Если вы повторите этот скрипт позже,
то v станет пустым и вы запишете пустой champion → снова вернётся ERR_NO_CFG.
        """,
    )
    ap.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        help="Redis URL (default: REDIS_URL env or redis://redis-worker-1:6379/0)",
    )
    ap.add_argument(
        "--champion-key",
        default=os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion"),
        help="Champion config key (default: ML_CFG_CHAMPION_KEY env or cfg:ml_confirm:champion)",
    )
    ap.add_argument(
        "--challenger-key",
        default=os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger"),
        help="Challenger config key (default: ML_CFG_CHALLENGER_KEY env or cfg:ml_confirm:challenger)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Только проверка, без изменений в Redis",
    )
    ap.add_argument(
        "--kind",
        type=str,
        default=None,
        help="Опциональный kind модели (например, meta_lr) для обновления специфичного ключа",
    )
    args = ap.parse_args()

    champion_key = args.champion_key
    challenger_key = args.challenger_key

    if args.kind:
        champion_key = f"{args.champion_key}:{args.kind}"
        challenger_key = f"{args.challenger_key}:{args.kind}"
        print(f"Используем per-kind ключи: {champion_key} и {challenger_key}")

    # Подключение к Redis
    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    print(f"\n{'='*80}")
    print("БЕЗОПАСНЫЙ ПРОМОУТ CHALLENGER → CHAMPION")
    print(f"{'='*80}\n")

    # Проверка текущего champion
    print("📋 Шаг 1: Проверка текущего champion")
    print("-" * 80)
    champion_raw = r.get(champion_key)
    if champion_raw:
        try:
            champion = json.loads(champion_raw)
            champion_len = len(champion_raw)
            print(f"✅ Champion существует (длина: {champion_len} байт)")
            print(f"   kind: {champion.get('kind', 'unknown')}")
            print(f"   model_path: {champion.get('model_path', 'unknown')}")
            print(f"   run_id: {champion.get('run_id', 'unknown')}")
        except Exception as e:
            print(f"⚠️  Champion существует, но невалидный JSON: {e}")
            champion = None
            champion_len = len(champion_raw) if champion_raw else 0
    else:
        print("⚠️  Champion не найден (пустой)")
        champion = None
        champion_len = 0

    print()

    # Проверка challenger
    print("📋 Шаг 2: Проверка challenger")
    print("-" * 80)
    challenger_raw = r.get(challenger_key)
    if challenger_raw:
        challenger_len = len(challenger_raw)
        try:
            challenger = json.loads(challenger_raw)
            print(f"✅ Challenger существует (длина: {challenger_len} байт)")
            print(f"   kind: {challenger.get('kind', 'unknown')}")
            print(f"   model_path: {challenger.get('model_path', 'unknown')}")
            print(f"   run_id: {challenger.get('run_id', 'unknown')}")
        except Exception as e:
            print(f"❌ Challenger существует, но невалидный JSON: {e}")
            print(f"   Промоут невозможен из-за невалидного JSON")
            sys.exit(1)
    else:
        print("❌ Challenger не найден (пустой)")
        print("   Промоут невозможен: challenger пустой")
        print("   Это нормально, если challenger уже был промоутнут ранее")
        sys.exit(0)

    print()

    # Проверка на пустой challenger (безопасность)
    if not challenger_raw or len(challenger_raw.strip()) == 0:
        print("❌ Challenger пустой - промоут пропущен (безопасность)")
        print("   Если вы повторите этот скрипт позже, то v станет пустым")
        print("   и вы запишете пустой champion → снова вернётся ERR_NO_CFG")
        sys.exit(0)

    # Сравнение champion и challenger
    print("📋 Шаг 3: Сравнение champion и challenger")
    print("-" * 80)
    if champion:
        champion_run_id = champion.get("run_id", "")
        challenger_run_id = challenger.get("run_id", "")
        if champion_run_id == challenger_run_id:
            print(f"⚠️  Champion и challenger имеют одинаковый run_id: {champion_run_id}")
            print("   Промоут не требуется (champion уже актуален)")
        else:
            print(f"   Champion run_id:  {champion_run_id}")
            print(f"   Challenger run_id: {challenger_run_id}")
            print("   ✅ Разные run_id - промоут имеет смысл")
    else:
        print("   Champion отсутствует - промоут создаст новый champion")
    print()

    # Промоут
    if args.dry_run:
        print("📋 Шаг 4: DRY-RUN (промоут не выполнен)")
        print("-" * 80)
        print("✅ DRY-RUN: Промоут был бы выполнен")
        print(f"   Был бы выполнен:")
        print(f"     redis-cli SET {champion_key} <challenger_value>")
        print(f"     redis-cli DEL {challenger_key}")
        print(f"     redis-cli STRLEN {champion_key}")
        print()
        print("   Для реального промоута запустите без --dry-run")
    else:
        print("📋 Шаг 4: Выполнение промоута")
        print("-" * 80)
        try:
            # Безопасный промоут: проверяем, что challenger не пустой
            if not challenger_raw or len(challenger_raw.strip()) == 0:
                print("❌ Challenger пустой - промоут пропущен (безопасность)")
                sys.exit(0)

            # Устанавливаем champion
            r.set(champion_key, challenger_raw)
            print(f"✅ Champion обновлён: {champion_key}")

            # Удаляем challenger
            r.delete(challenger_key)
            print(f"✅ Challenger удалён: {challenger_key}")

            # Проверка длины champion после промоута
            champion_after = r.get(champion_key)
            if champion_after:
                champion_after_len = len(champion_after)
                print(f"✅ Проверка: champion длина после промоута = {champion_after_len} байт")
            else:
                print("❌ ОШИБКА: champion стал пустым после промоута!")
                sys.exit(1)

        except Exception as e:
            print(f"❌ Ошибка при промоуте: {e}")
            sys.exit(1)

    print()
    print(f"{'='*80}")
    print("✅ ПРОМОУТ ЗАВЕРШЁН УСПЕШНО")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()

