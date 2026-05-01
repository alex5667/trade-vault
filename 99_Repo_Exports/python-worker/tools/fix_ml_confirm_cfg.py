#!/usr/bin/env python3
from __future__ import annotations
"""
Исправление ошибки ERR_NO_CFG в ML Confirm Gate.

Проверяет и исправляет конфигурацию в Redis:
1. Проверяет наличие cfg:ml_confirm:champion
2. Проверяет доступность файла модели
3. Создает минимальную конфигурацию, если нужно
"""

from utils.time_utils import get_ny_time_millis

import os
import sys
import json
import time
import redis
from typing import Dict, Any, Optional

def check_and_fix_config() -> bool:
    """Проверяет и исправляет конфигурацию ML Confirm."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
    
    print("=" * 60)
    print("ML Confirm Configuration Fix")
    print("=" * 60)
    print(f"Redis URL: {redis_url}")
    print(f"Champion key: {champion_key}")
    print()
    
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.ping()
        print("✅ Redis connection OK")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return False
    
    # Проверка существующей конфигурации
    raw = r.get(champion_key)
    if raw:
        try:
            cfg = json.loads(raw)
            print(f"✅ Champion config exists")
            print(f"   kind: {cfg.get('kind', 'unknown')}")
            print(f"   run_id: {cfg.get('run_id', 'unknown')}")
            model_path = cfg.get('model_path', '')
            print(f"   model_path: {model_path}")
            
            if model_path:
                # Проверка существования файла модели
                if os.path.exists(model_path):
                    stat = os.stat(model_path)
                    print(f"✅ Model file exists ({stat.st_size} bytes)")
                    return True
                else:
                    print(f"⚠️  Model file NOT found: {model_path}")
                    # Попробуем найти модель в стандартных местах
                    fallback_paths = [
                        "/var/lib/trade/of_reports/models/model.joblib",
                        "/var/lib/trade/ml_models/model.joblib",
                    ]
                    for fallback in fallback_paths:
                        if os.path.exists(fallback):
                            print(f"✅ Found fallback model: {fallback}")
                            # Обновляем конфигурацию с fallback путем
                            cfg['model_path'] = fallback
                            cfg['model_path_original'] = model_path
                            cfg['model_path_fallback_used'] = True
                            r.set(champion_key, json.dumps(cfg, ensure_ascii=False, separators=(',', ':')))
                            print(f"✅ Updated config with fallback model path")
                            return True
                    print(f"❌ No model file found anywhere")
                    return False
            else:
                print(f"⚠️  model_path is empty in config")
                return False
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in champion config: {e}")
            print(f"   Raw value (first 200 chars): {raw[:200]}")
            return False
    else:
        print(f"❌ Champion config NOT found")
        print(f"   Creating minimal configuration...")
        
        # Создаем минимальную конфигурацию для режима SHADOW
        # Ищем модель в стандартных местах
        model_paths = [
            "/var/lib/trade/ml_models/tb_v10_4_20260204_180848_830d27/model.joblib",
            "/var/lib/trade/of_reports/models/model.joblib",
            "/var/lib/trade/ml_models/model.joblib",
        ]
        
        found_model = None
        for path in model_paths:
            if os.path.exists(path):
                found_model = path
                break
        
        if not found_model:
            print(f"❌ No model file found. Cannot create config.")
            print(f"   Searched paths:")
            for path in model_paths:
                print(f"     - {path}")
            return False
        
        print(f"✅ Found model: {found_model}")
        
        # Создаем минимальную конфигурацию
        minimal_cfg: Dict[str, Any] = {
            "kind": "util_mh_v1",
            "run_id": "bootstrap_" + str(int(time.time())),
            "created_ms": get_ny_time_millis(),
            "model_path": found_model,
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
        
        try:
            cfg_json = json.dumps(minimal_cfg, ensure_ascii=False, separators=(',', ':'))
            r.set(champion_key, cfg_json)
            print(f"✅ Created minimal champion config")
            print(f"   kind: {minimal_cfg['kind']}")
            print(f"   model_path: {minimal_cfg['model_path']}")
            return True
        except Exception as e:
            print(f"❌ Failed to create config: {e}")
            return False


def main() -> None:
    """Main entry point."""
    success = check_and_fix_config()
    print()
    if success:
        print("=" * 60)
        print("✅ Configuration check passed")
        print("=" * 60)
        sys.exit(0)
    else:
        print("=" * 60)
        print("❌ Configuration check failed")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()

