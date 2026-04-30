#!/usr/bin/env python3
"""
Автоматическая инициализация ML Confirm конфигурации при старте сервиса.

Запускается как init скрипт перед основным сервисом.
Создает конфигурацию, если она отсутствует и модель найдена.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import sys
import json
import time
import glob
import redis

try:
    import joblib  # type: ignore
except Exception:
    joblib = None  # type: ignore
from typing import Optional

def find_latest_model() -> Optional[str]:
    """Находит последнюю модель в стандартных местах."""
    search_paths = [
        "/var/lib/trade/ml_models/tb_v10_4_*/model.joblib"
        "/var/lib/trade/ml_models/*/model.joblib"
        "/var/lib/trade/of_reports/models/meta_lr_*.json"
        "/var/lib/trade/of_reports/models/model.joblib"
        "/var/lib/trade/ml_models/model.joblib"
        # edge_stack_v1 champion/run paths (higher priority — listed last, sorted by mtime)
        "/var/lib/trade/ml_models/edge_stack_v1/champions/*.joblib"
        "/var/lib/trade/ml_models/edge_stack_v1/runs/*/*.joblib"
    ]
    
    all_models = []
    for pattern in search_paths:
        try:
            matches = glob.glob(pattern)
            for model_path in matches:
                if model_path.endswith(".report.json"):
                    continue
                if os.path.exists(model_path) and os.path.isfile(model_path):
                    mtime = os.path.getmtime(model_path)
                    all_models.append((model_path, mtime))
        except Exception as e:
            print(f"⚠️  ML Confirm init: Error searching pattern {pattern}: {e}", file=sys.stderr)
            continue
    
    if not all_models:
        # Log diagnostic info about what directories exist
        checked_dirs = [
            "/var/lib/trade/ml_models"
            "/var/lib/trade/of_reports/models"
        ]
        for dir_path in checked_dirs:
            if os.path.exists(dir_path):
                try:
                    files = os.listdir(dir_path)
                    print(f"ℹ️  ML Confirm init: Directory {dir_path} exists, contains {len(files)} items", file=sys.stderr)
                except Exception:
                    print(f"⚠️  ML Confirm init: Cannot list directory {dir_path}", file=sys.stderr)
            else:
                print(f"⚠️  ML Confirm init: Directory {dir_path} does not exist", file=sys.stderr)
        return None
    
    all_models.sort(key=lambda x: x[1], reverse=True)
    selected = all_models[0][0]
    print(f"ℹ️  ML Confirm init: Found {len(all_models)} model(s), selected: {selected}", file=sys.stderr)
    return selected


def ensure_ml_confirm_config() -> bool:
    """Проверяет и создает конфигурацию ML Confirm, если нужно."""
    # Проверяем режим ML gate
    mode = os.getenv("ML_CONFIRM_MODE", "SHADOW").upper()
    print(f"ℹ️  ML Confirm init: Mode={mode}", file=sys.stderr)
    
    if mode == "OFF":
        print(f"ℹ️  ML Confirm init: Mode is OFF, skipping config creation", file=sys.stderr)
        return True  # Не нужна конфигурация
    
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
    
    print(f"ℹ️  ML Confirm init: Connecting to Redis at {redis_url}", file=sys.stderr)
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=5)
        r.ping()
        print(f"✅ ML Confirm init: Redis connection successful", file=sys.stderr)
    except Exception as e:
        print(f"❌ ML Confirm init: Redis connection failed: {e}", file=sys.stderr)
        import traceback
        print(f"   Traceback: {traceback.format_exc()}", file=sys.stderr)
        return False
    
    # Проверяем существующую конфигурацию
    print(f"ℹ️  ML Confirm init: Checking existing config at {champion_key}", file=sys.stderr)
    existing = r.get(champion_key)
    if existing:
        try:
            cfg = json.loads(existing)
            model_path = cfg.get('model_path', '')
            if model_path and os.path.exists(model_path):
                print(f"✅ ML Confirm init: Existing config is valid (model: {model_path})", file=sys.stderr)
                return True
            else:
                print(f"⚠️  ML Confirm init: Existing config found but model path invalid or missing: {model_path}", file=sys.stderr)
        except json.JSONDecodeError as e:
            print(f"⚠️  ML Confirm init: Existing config is invalid JSON: {e}, will recreate", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  ML Confirm init: Error validating existing config: {e}, will recreate", file=sys.stderr)
    
    model_path = find_latest_model()
    if not model_path:
        print(f"⚠️  ML Confirm init: No model found, creating dummy model for SHADOW mode fallback...", file=sys.stderr)
        dummy_path = "/app/dummy_model_v1.json"
        try:
            with open(dummy_path, "w") as f:
                json.dump({
                    "kind": "util_mh_fastlinear_v1"
                    "feature_cols": ["f_spread_bps"]
                    "horizons_ms": [60000]
                    "weights": {
                        "60000": {"intercept": 0.0, "coef": [0.0], "unc": 0.0}
                    }
                }, f)
            print(f"✅  ML Confirm init: Created dummy model at {dummy_path}", file=sys.stderr)
            model_path = dummy_path
        except Exception as e:
            # Fallback to /tmp if /app is read-only
            dummy_path = "/tmp/dummy_model_v1.json"
            try:
                with open(dummy_path, "w") as f:
                    json.dump({
                        "kind": "util_mh_fastlinear_v1"
                        "feature_cols": ["f_spread_bps"]
                        "horizons_ms": [60000]
                        "weights": {
                            "60000": {"intercept": 0.0, "coef": [0.0], "unc": 0.0}
                        }
                    }, f)
                print(f"✅  ML Confirm init: Created dummy model at {dummy_path}", file=sys.stderr)
                model_path = dummy_path
            except Exception as e2:
                print(f"❌ ML Confirm init: Failed to create dummy model: {e2}", file=sys.stderr)
                return False
    
    # Валидируем модель перед использованием
    if not os.path.exists(model_path):
        print(f"❌ ML Confirm init: Model file does not exist: {model_path}", file=sys.stderr)
        return False
    
    if not os.path.isfile(model_path):
        print(f"❌ ML Confirm init: Model path is not a file: {model_path}", file=sys.stderr)
        return False
    
    # Создаем конфигурацию
    print(f"ℹ️  ML Confirm init: Creating config with model: {model_path}", file=sys.stderr)

    # Determine kind based on filename/content (and pack inspection when possible)
    kind = "util_mh_v1"
    if model_path.endswith(".json") and "meta_lr" in os.path.basename(model_path):
        kind = "meta_lr"
    elif joblib is not None and model_path.endswith(".joblib"):
        try:
            # Inspect the joblib pack to detect kind directly from model artifact
            pack = joblib.load(model_path)
            if isinstance(pack, dict) and isinstance(pack.get('kind'), str) and pack.get('kind'):
                kind = str(pack.get('kind'))
        except Exception:
            pass

    # edge_stack_v1 uses p_min / p_min_by_bucket schema — different from util_mh util_floors schema
    if kind == 'edge_stack_v1':
        cfg = {
            'kind': 'edge_stack_v1'
            'run_id': f'auto_init_{int(time.time())}'
            'created_ms': get_ny_time_millis()
            'model_path': model_path
            'schema_version': 1
            'mode': 'SHADOW'
            'enforce_share': 0.0
            'p_min': float(os.getenv('EDGE_STACK_P_MIN_DEFAULT', '0.55') or 0.55)
            'p_min_by_bucket': {}
            'hard_p_min_floor': float(os.getenv('EDGE_STACK_HARD_P_MIN_FLOOR', '0.0') or 0.0)
        }
    else:
        cfg = {
            "kind": kind
            "run_id": f"auto_init_{int(time.time())}"
            "created_ms": get_ny_time_millis()
            "model_path": model_path
            "schema_version": 1
            "mode": "SHADOW"
            "enforce_share": 0.0
            "util_floors": {
                "global": {
                    "floor": -0.05 if kind == "util_mh_v1" else 0.55
                    "n_take": 0
                    "take_rate": 1.0
                    "mean_util": 0.0
                    "sum_util": 0.0
                }
                "by_bucket": {
                    "trend": {
                        "floor": -0.05 if kind == "util_mh_v1" else 0.55
                        "n_take": 0
                        "take_rate": 1.0
                        "mean_util": 0.0
                        "sum_util": 0.0
                    }
                    "range": {
                        "floor": -0.05 if kind == "util_mh_v1" else 0.55
                        "n_take": 0
                        "take_rate": 1.0
                        "mean_util": 0.0
                        "sum_util": 0.0
                    }
                    "other": {
                        "floor": -0.05 if kind == "util_mh_v1" else 0.55
                        "n_take": 0
                        "take_rate": 1.0
                        "mean_util": 0.0
                        "sum_util": 0.0
                    }
                }
                "horizons": [60000, 180000, 300000]
                "unc_k": 0.5
            }
        }
    
    try:
        cfg_json = json.dumps(cfg, ensure_ascii=False, separators=(',', ':'))
        r.set(champion_key, cfg_json)
        print(f"✅ ML Confirm init: Successfully created config at {champion_key} (model: {model_path})", file=sys.stderr)
        # Verify it was saved correctly
        verify = r.get(champion_key)
        if verify:
            print(f"✅ ML Confirm init: Config verified in Redis (length: {len(verify)} chars)", file=sys.stderr)
        else:
            print(f"⚠️  ML Confirm init: Config saved but verification read returned None", file=sys.stderr)
        return True
    except Exception as e:
        print(f"❌ ML Confirm init: Failed to create config: {e}", file=sys.stderr)
        import traceback
        print(f"   Traceback: {traceback.format_exc()}", file=sys.stderr)
        return False


if __name__ == "__main__":
    success = ensure_ml_confirm_config()
    sys.exit(0 if success else 1)

