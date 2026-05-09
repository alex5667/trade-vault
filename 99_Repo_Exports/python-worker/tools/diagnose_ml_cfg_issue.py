#!/usr/bin/env python3
"""Диагностика проблемы ERR_NO_CFG в ML Confirm Gate."""

import json
import os
import sys

import redis

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def main():
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")

    print("=" * 70)
    print("ML Confirm Configuration Diagnostic")
    print("=" * 70)

    # 1. Проверка Redis подключения
    print("\n1. Redis Connection:")
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=5)
        r.ping()
        print(f"   ✅ Connected to {redis_url}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return 1

    # 2. Проверка конфигурации
    print(f"\n2. Configuration Check ({champion_key}):")
    raw = r.get(champion_key)
    if not raw:
        print("   ❌ Configuration NOT found")
        return 1

    print(f"   ✅ Configuration found (length: {len(raw)} bytes)")

    # 3. Парсинг JSON
    print("\n3. JSON Parsing:")
    try:
        cfg = json.loads(raw)
        print("   ✅ Valid JSON")
        print(f"   Kind: {cfg.get('kind')}")
        print(f"   Run ID: {cfg.get('run_id')}")
        print(f"   Model path: {cfg.get('model_path')}")
    except json.JSONDecodeError as e:
        print(f"   ❌ Invalid JSON: {e}")
        print(f"   First 200 chars: {raw[:200]}")
        return 1

    # 4. Проверка файла модели
    model_path = cfg.get('model_path', '')
    print("\n4. Model File Check:")
    if not model_path:
        print("   ❌ model_path is empty")
        return 1

    print(f"   Path: {model_path}")
    if os.path.exists(model_path):
        stat = os.stat(model_path)
        print(f"   ✅ File exists ({stat.st_size} bytes)")
    else:
        print("   ❌ File NOT found")
        return 1

    # 5. Попытка загрузки модели
    print("\n5. Model Loading:")
    try:
        import joblib
        model = joblib.load(model_path)
        print("   ✅ Model loaded")
        print(f"   Type: {type(model).__name__}")

        # Проверка методов для util_mh
        if cfg.get('kind', '').lower().startswith('util_mh'):
            has_util = hasattr(model, 'predict_util')
            has_unc = hasattr(model, 'predict_unc')
            print(f"   Has predict_util: {has_util}")
            print(f"   Has predict_unc: {has_unc}")
            if not has_util or not has_unc:
                print("   ❌ Model missing required methods for util_mh")
                return 1
    except ImportError:
        print("   ⚠️  joblib not available")
    except Exception as e:
        print(f"   ❌ Failed to load model: {e}")
        return 1

    # 6. Тест через MLConfirmGate
    print("\n6. MLConfirmGate Test:")
    try:
        from services.ml_confirm_gate import MLConfirmGate

        gate = MLConfirmGate.from_env()
        print(f"   ✅ Gate created (mode: {gate.mode})")

        # Принудительно обновляем кэш
        gate._cache_loaded_ms = 0
        gate._refresh_cache_if_needed()

        if gate._cfg:
            print("   ✅ Config loaded via gate")
            print(f"      Source: {gate._cfg_source}")
            print(f"      Kind: {gate._cfg.get('kind')}")
        else:
            print("   ❌ Config NOT loaded via gate")
            print(f"      Source: {gate._cfg_source}")
            print(f"      Parse error: {gate._cfg_parse_err}")
            print(f"      Model load error: {gate._model_load_error}")
            return 1

        if gate._model:
            print("   ✅ Model loaded via gate")
        else:
            print("   ⚠️  Model NOT loaded via gate")
            print(f"      Error: {gate._model_load_error}")

        # Тест check()
        print("\n7. Check() Test:")
        dec = gate.check(
            symbol="BTCUSDT",
            ts_ms=1000000,
            direction="LONG",
            scenario="range_meanrev",
            indicators={"spread_bps": 1.0},
            rule_score=0.7,
            rule_have=2,
            rule_need=2,
            cancel_spike_veto=0,
            ok_rule=1,
        )
        print(f"   Status: {dec.status}")
        print(f"   Allow: {dec.allow}")
        print(f"   Reason: {dec.reason}")

        if dec.status == "ERR_NO_CFG":
            print("   ❌ Still getting ERR_NO_CFG!")
            return 1
        else:
            print("   ✅ No ERR_NO_CFG error")

    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70)
    print("✅ All checks passed!")
    print("=" * 70)
    return 0

if __name__ == "__main__":
    sys.exit(main())

