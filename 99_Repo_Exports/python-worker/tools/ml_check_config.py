"""
Проверка конфигурации ML из Redis cfg:ml_confirm.

Проверяет:
- Существование model_path и meta_path
- Корректность enforce_share
- Режим работы (OFF/SHADOW/ENFORCE)
- Fail policy
- Canary настройки
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict

import redis


def main() -> None:
    ap = argparse.ArgumentParser(description="Проверка конфигурации ML")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--cfg-key", default=os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm"))
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    cfg = r.hgetall(args.cfg_key) or {}

    print(f"\n{'='*80}")
    print(f"ML CONFIG CHECK: {args.cfg_key}")
    print(f"{'='*80}")

    if not cfg:
        print(f"❌ Конфигурация не найдена в Redis key: {args.cfg_key}")
        return

    # Основные параметры
    mode = (cfg.get("mode") or os.getenv("ML_CONFIRM_MODE", "OFF")).upper()
    fail_policy = (cfg.get("fail_policy") or os.getenv("ML_CONFIRM_FAIL_POLICY", "CLOSED")).upper()
    model_path = cfg.get("model_path") or os.getenv("ML_CONFIRM_MODEL_PATH", "")
    meta_path = cfg.get("meta_path") or os.getenv("ML_CONFIRM_META_PATH", "")
    model_ver = cfg.get("model_ver") or cfg.get("ver") or os.getenv("ML_CONFIRM_MODEL_VER", "")

    # Canary
    enforce_share = cfg.get("enforce_share") or cfg.get("canary_share") or "1.0"
    try:
        enforce_share_f = float(enforce_share)
    except Exception:
        enforce_share_f = 1.0

    enforce_symbols = cfg.get("enforce_symbols") or cfg.get("canary_symbols") or ""
    sample_key_mode = cfg.get("sample_key_mode") or "sid"
    timebucket_sec = int(cfg.get("timebucket_sec") or 60)

    # p_min
    p_min_default = cfg.get("p_min_default") or os.getenv("ML_CONFIRM_P_MIN_DEFAULT", "0.55")
    try:
        p_min_f = float(p_min_default)
    except Exception:
        p_min_f = 0.55

    print(f"\n{'─'*80}")
    print(f"ОСНОВНЫЕ ПАРАМЕТРЫ:")
    print(f"{'─'*80}")
    print(f"  mode:           {mode}")
    print(f"  fail_policy:    {fail_policy}")
    print(f"  model_ver:      {model_ver or '(не указан)'}")
    print(f"  p_min_default:   {p_min_f}")

    # Проверка файлов
    print(f"\n{'─'*80}")
    print(f"ФАЙЛЫ МОДЕЛИ:")
    print(f"{'─'*80}")
    
    model_ok = False
    if model_path:
        if os.path.exists(model_path):
            size = os.path.getsize(model_path) / (1024 * 1024)  # MB
            print(f"  ✅ model_path: {model_path} ({size:.2f} MB)")
            model_ok = True
        else:
            print(f"  ❌ model_path: {model_path} (ФАЙЛ НЕ СУЩЕСТВУЕТ!)")
    else:
        print(f"  ⚠️  model_path: не указан")

    meta_ok = False
    if meta_path:
        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                try:
                    meta = json.load(f)
                    print(f"  ✅ meta_path: {meta_path}")
                    print(f"     model_ver в meta: {meta.get('model_ver', 'не указан')}")
                    print(f"     feature_names: {len(meta.get('feature_names', []))} features")
                    meta_ok = True
                except Exception as e:
                    print(f"  ❌ meta_path: {meta_path} (ОШИБКА ПРИ ЧТЕНИИ JSON: {e})")
        else:
            print(f"  ❌ meta_path: {meta_path} (ФАЙЛ НЕ СУЩЕСТВУЕТ!)")
    else:
        print(f"  ⚠️  meta_path: не указан")

    # Canary настройки
    print(f"\n{'─'*80}")
    print(f"CANARY НАСТРОЙКИ:")
    print(f"{'─'*80}")
    print(f"  enforce_share:  {enforce_share_f:.4f} ({100.0*enforce_share_f:.2f}%)")
    print(f"  enforce_symbols: {enforce_symbols or '(все символы)'}")
    print(f"  sample_key_mode: {sample_key_mode}")
    print(f"  timebucket_sec: {timebucket_sec}")

    # Freeze статус
    freeze_reason = cfg.get("freeze_reason") or ""
    freeze_ts_ms = cfg.get("freeze_ts_ms") or ""
    if freeze_reason:
        print(f"\n{'─'*80}")
        print(f"FREEZE СТАТУС:")
        print(f"{'─'*80}")
        print(f"  ⚠️  ЗАМОРОЖЕНО: {freeze_reason}")
        if freeze_ts_ms:
            try:
                import time
                ts = int(freeze_ts_ms) / 1000.0
                ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
                print(f"  Время заморозки: {ts_str}")
            except Exception:
                print(f"  Время заморозки: {freeze_ts_ms}")

    # Дополнительные параметры
    other_keys = [k for k in cfg.keys() if k not in [
        "mode", "fail_policy", "model_path", "meta_path", "model_ver",
        "enforce_share", "canary_share", "enforce_symbols", "canary_symbols",
        "sample_key_mode", "timebucket_sec", "p_min_default",
        "freeze_reason", "freeze_ts_ms", "unfreeze_ts_ms"
    ]]
    if other_keys:
        print(f"\n{'─'*80}")
        print(f"ДОПОЛНИТЕЛЬНЫЕ ПАРАМЕТРЫ:")
        print(f"{'─'*80}")
        for k in sorted(other_keys):
            v = cfg[k]
            if len(str(v)) > 80:
                v = str(v)[:80] + "..."
            print(f"  {k}: {v}")

    # Итоговые проверки
    print(f"\n{'─'*80}")
    print(f"ИТОГОВЫЕ ПРОВЕРКИ:")
    print(f"{'─'*80}")
    
    issues = []
    if not model_ok:
        issues.append("❌ model_path не существует или не указан")
    if not meta_ok:
        issues.append("❌ meta_path не существует или не указан")
    if mode not in ("OFF", "SHADOW", "ENFORCE"):
        issues.append(f"⚠️  Неизвестный режим: {mode}")
    if fail_policy not in ("OPEN", "CLOSED"):
        issues.append(f"⚠️  Неизвестный fail_policy: {fail_policy}")
    if enforce_share_f < 0.0 or enforce_share_f > 1.0:
        issues.append(f"⚠️  enforce_share вне диапазона [0,1]: {enforce_share_f}")
    if freeze_reason:
        issues.append(f"⚠️  ML заморожен: {freeze_reason}")

    if issues:
        for issue in issues:
            print(f"  {issue}")
    else:
        print(f"  ✅ Все проверки пройдены")

    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    main()

