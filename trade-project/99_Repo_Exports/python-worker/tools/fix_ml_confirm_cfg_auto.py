#!/usr/bin/env python3
from __future__ import annotations

"""
Автоматическое исправление конфигурации ML Confirm Gate.

Находит модель, проверяет её валидность и создает конфигурацию в Redis.
"""

import glob
import json
import os
import sys
import time
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis


def find_latest_model() -> str | None:
    """Находит последнюю модель в стандартных местах."""
    search_paths = [
        "/var/lib/trade/ml_models/tb_v10_4_*/model.joblib",
        "/var/lib/trade/ml_models/*/model.joblib",
        "/var/lib/trade/of_reports/models/model.joblib",
        "/var/lib/trade/ml_models/model.joblib",
    ]

    all_models: list[tuple[str, float]] = []

    for pattern in search_paths:
        matches = glob.glob(pattern)
        for model_path in matches:
            if os.path.exists(model_path) and os.path.isfile(model_path):
                mtime = os.path.getmtime(model_path)
                all_models.append((model_path, mtime))

    if not all_models:
        return None

    # Сортируем по времени модификации (последняя первая)
    all_models.sort(key=lambda x: x[1], reverse=True)
    return all_models[0][0]


def validate_model(model_path: str) -> bool:
    """Проверяет, что модель валидна (можно загрузить joblib)."""
    try:
        import joblib
        model = joblib.load(model_path)
        print(f"   ✅ Model validation OK (type: {type(model).__name__})")
        return True
    except ImportError:
        print("   ⚠️  joblib not available, skipping validation")
        return True  # Пропускаем валидацию, если joblib недоступен
    except Exception as e:
        print(f"   ❌ Model validation failed: {e}")
        return False


def create_champion_config(model_path: str) -> dict[str, Any]:
    """Создает минимальную конфигурацию champion."""
    return {
        "kind": "util_mh_v1",
        "run_id": f"bootstrap_{int(time.time())}",
        "created_ms": get_ny_time_millis(),
        "model_path": model_path,
        "schema_version": 1,
        "mode": "SHADOW",
        "enforce_share": 0.0,
        "util_floors": {
            "global": {
                "floor": -0.05,
                "n_take": 0,
                "take_rate": 1.0,
                "mean_util": 0.0,
                "sum_util": 0.0
            },
            "by_bucket": {
                "trend": {
                    "floor": -0.05,
                    "n_take": 0,
                    "take_rate": 1.0,
                    "mean_util": 0.0,
                    "sum_util": 0.0
                },
                "other": {
                    "floor": -0.05,
                    "n_take": 0,
                    "take_rate": 1.0,
                    "mean_util": 0.0,
                    "sum_util": 0.0
                }
            },
            "horizons": [60000, 180000, 300000],
            "unc_k": 0.5
        }
    }


def main() -> None:
    """Main entry point."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")

    print("=" * 80)
    print("ML Confirm Configuration Auto-Fix")
    print("=" * 80)
    print(f"Redis URL: {redis_url}")
    print(f"Champion key: {champion_key}")
    print()

    # Подключение к Redis
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.ping()
        print("✅ Redis connection OK")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        sys.exit(1)

    # Проверка существующей конфигурации
    print("\n📋 Step 1: Checking existing configuration")
    print("-" * 80)
    existing_raw = r.get(champion_key)
    if existing_raw:
        try:
            existing_cfg = json.loads(existing_raw)
            model_path = existing_cfg.get('model_path', '')
            print("✅ Champion config exists")
            print(f"   kind: {existing_cfg.get('kind', 'unknown')}")
            print(f"   run_id: {existing_cfg.get('run_id', 'unknown')}")
            print(f"   model_path: {model_path}")

            if model_path and os.path.exists(model_path):
                print("✅ Model file exists")
                print("   Configuration is valid, no action needed")
                sys.exit(0)
            elif model_path:
                print(f"⚠️  Model file NOT found: {model_path}")
                print("   Will search for alternative model...")
            else:
                print("⚠️  model_path is empty in config")
                print("   Will search for model and update config...")
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in champion config: {e}")
            print("   Will recreate configuration...")
            existing_cfg = None
    else:
        print("❌ Champion config NOT found")
        existing_cfg = None

    # Поиск модели
    print("\n📋 Step 2: Finding model file")
    print("-" * 80)
    model_path = find_latest_model()

    if not model_path:
        print("❌ No model file found")
        print("   Searched paths:")
        search_paths = [
            "/var/lib/trade/ml_models/tb_v10_4_*/model.joblib",
            "/var/lib/trade/ml_models/*/model.joblib",
            "/var/lib/trade/of_reports/models/model.joblib",
            "/var/lib/trade/ml_models/model.joblib",
        ]
        for path in search_paths:
            print(f"     - {path}")
        sys.exit(1)

    print(f"✅ Found model: {model_path}")
    stat = os.stat(model_path)
    print(f"   Size: {stat.st_size:,} bytes")
    print(f"   Modified: {time.ctime(stat.st_mtime)}")

    # Валидация модели
    if not validate_model(model_path):
        print("❌ Model validation failed, aborting")
        sys.exit(1)

    # Создание конфигурации
    print("\n📋 Step 3: Creating configuration")
    print("-" * 80)
    cfg = create_champion_config(model_path)

    # Если существующая конфигурация была валидной, сохраняем её параметры
    if existing_cfg and existing_cfg.get('model_path'):
        # Сохраняем util_floors из существующей конфигурации, если они есть
        if 'util_floors' in existing_cfg:
            cfg['util_floors'] = existing_cfg['util_floors']
            print("   Preserved util_floors from existing config")

    # Сохранение в Redis
    print("\n📋 Step 4: Saving to Redis")
    print("-" * 80)
    try:
        cfg_json = json.dumps(cfg, ensure_ascii=False, separators=(',', ':'))
        r.set(champion_key, cfg_json)

        # Проверка сохранения
        saved_raw = r.get(champion_key)
        if saved_raw:
            saved_cfg = json.loads(saved_raw)
            print("✅ Configuration saved successfully")
            print(f"   Key: {champion_key}")
            print(f"   Size: {len(cfg_json)} bytes")
            print(f"   kind: {saved_cfg.get('kind')}")
            print(f"   run_id: {saved_cfg.get('run_id')}")
            print(f"   model_path: {saved_cfg.get('model_path')}")
        else:
            print("❌ Configuration was not saved (verification failed)")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Failed to save configuration: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n" + "=" * 80)
    print("✅ CONFIGURATION FIX COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print("\nML gate should now be able to load the configuration.")
    print("Restart the service or wait for next config reload cycle.")


if __name__ == "__main__":
    main()

