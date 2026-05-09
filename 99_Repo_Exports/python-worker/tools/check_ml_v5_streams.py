#!/usr/bin/env python3
from __future__ import annotations

"""Проверка стримов ML Next Level v5: metrics:ml_confirm и metrics:ml_outcome.

P27: добавлено раскрытие payload/indicators (если writer кладёт расширенные поля в JSON)
и "soft"-проверка расширенных полей (schema pinning, exec-cost) без ломания
обратной совместимости.
"""


import os
from typing import Any

import redis

from tools.redis_window import _merge_payload_fields


def _short(v: Any, n: int = 60) -> str:
    s = str(v)
    return (s[:n] + "...") if len(s) > n else s


def check_stream(
    r: redis.Redis,
    stream: str,
    name: str,
    required_fields: list[str],
    optional_fields: list[str] | None = None,
) -> None:
    """Проверить наличие стрима и требуемых полей в последних сообщениях."""
    try:
        msgs = r.xrevrange(stream, count=10)
        if not msgs:
            print(f"⚠️  {name}: стрим пуст или не существует")
            return

        print(f"\n✅ {name} ({stream}): найдено {len(msgs)} последних сообщений")

        last_id, last_fields = msgs[0]
        flat = _merge_payload_fields(dict(last_fields))

        print(f"   Последнее сообщение ID: {last_id}")
        print(f"   Поля (flat): {', '.join(sorted(flat.keys()))}")

        missing_req = [f for f in required_fields if f not in flat]
        if missing_req:
            print(f"   ⚠️  Отсутствуют REQUIRED поля: {', '.join(missing_req)}")
        else:
            print(f"   ✅ REQUIRED поля присутствуют: {', '.join(required_fields)}")

        opt = optional_fields or []
        missing_opt = [f for f in opt if f not in flat]
        present_opt = [f for f in opt if f in flat]
        if opt:
            if missing_opt:
                print(f"   ⚠️  Отсутствуют OPTIONAL поля (P27 payload): {', '.join(missing_opt)}")
            print(f"   ℹ️  OPTIONAL присутствуют: {len(present_opt)}/{len(opt)}")

        print("   Пример значений:")
        for field in required_fields[:6]:
            val = flat.get(field, "N/A")
            print(f"     {field} = {_short(val)}")

        if present_opt:
            print("   Пример OPTIONAL:")
            for field in present_opt[:6]:
                val = flat.get(field, "N/A")
                print(f"     {field} = {_short(val)}")

    except Exception as e:
        print(f"❌ {name}: ошибка при чтении стрима - {e}")


def check_pred_cache(r: redis.Redis) -> None:
    """Проверить наличие pred cache записей."""
    try:
        keys = r.keys("ml:pred:*")
        if keys:
            print(f"\n✅ ml:pred cache: найдено {len(keys)} записей")
            sample_key = keys[0]
            val = r.get(sample_key)
            if val:
                import json

                try:
                    data = json.loads(val)
                    print(f"   Пример ключа: {sample_key}")
                    print(f"   Поля в cache: {', '.join(sorted(data.keys()))}")
                except Exception:
                    print(f"   Пример ключа: {sample_key} (не JSON)")
        else:
            print("\n⚠️  ml:pred cache: записей не найдено")
    except Exception as e:
        print(f"❌ ml:pred cache: ошибка - {e}")


def main() -> None:
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

    print("=" * 60)
    print("Проверка стримов ML Next Level v5")
    print("=" * 60)

    # metrics:ml_confirm (writer: gate v5)
    check_stream(
        r,
        os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm"),
        "ML Confirm Gate v5",
        required_fields=[
            "ts_ms",
            "sid",
            "symbol",
            "bucket",
            "mode",
            "enforce",
            "share_used",
            "p_edge",
            "p_min",
            "exec_risk_norm",
            "model_ver",
        ],
        optional_fields=[
            # schema pinning (P25/P27)
            "schema_name",
            "schema_version",
            "schema_hash",
            "model_sig",
            # exec-cost (P27)
            "spread_bps",
            "expected_slippage_bps",
            "exec_risk_bps",
            "exec_risk_ref_bps",
            "exec_pen",
            # gate context
            "direction",
            "scenario_v4",
            "ok_rule",
            "missing",
            "lat_ms",
            "latency_us",
        ],
    )

    # metrics:ml_outcome (writer: outcome joiner v3)
    check_stream(
        r,
        os.getenv("ML_OUTCOME_METRICS_STREAM", "metrics:ml_outcome"),
        "ML Outcome Joiner v3",
        required_fields=[
            "ts_ms",
            "sid",
            "symbol",
            "bucket",
            "y",
            "r_mult",
            "p_edge",
            "brier",
            "exec_risk_norm",
            "share_used",
        ],
        optional_fields=[
            # schema pinning / join debug
            "model_ver",
            "model_sig",
            "schema_hash",
            "joined",
            "lag_ms",
            # exec-cost (if outcome joiner forwards it)
            "spread_bps",
            "expected_slippage_bps",
            "exec_risk_bps",
            "exec_pen",
        ],
    )

    check_pred_cache(r)

    print("\n" + "=" * 60)
    print("Проверка завершена")
    print("=" * 60)


if __name__ == "__main__":
    main()

