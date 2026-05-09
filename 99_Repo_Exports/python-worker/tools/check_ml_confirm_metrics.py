from __future__ import annotations

"""
Проверка последних событий metrics:ml_confirm с детальными значениями.

Самый надёжный способ — через redis-py (вы сразу увидите status/err/reason/model_run_id).

Использование:
    python3 -m tools.check_ml_confirm_metrics --count 5
    python3 -m tools.check_ml_confirm_metrics --count 10 --redis-url redis://redis-worker-1:6379/0

Что должно быть после промоута:
    - status не ERR_NO_CFG
    - kind=util_mh_v1
    - model_run_id либо = вашему run_id (20260204_133025_708ce5), либо непустой
    - err/reason пустые или "ok"
"""


import argparse
import os

import redis

from tools.redis_window import _merge_payload_fields


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Проверка последних событий metrics:ml_confirm",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=r"""
Примеры:
  # Проверить последние 5 событий
  python3 -m tools.check_ml_confirm_metrics --count 5

  # Проверить последние 10 событий с кастомным Redis
  python3 -m tools.check_ml_confirm_metrics --count 10 --redis-url redis://localhost:6379/0

Что должно быть после промоута:
  - status не ERR_NO_CFG
  - kind=util_mh_v1
  - model_run_id либо = вашему run_id, либо непустой
  - err/reason пустые или "ok"
        """
    )
    ap.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        help="Redis URL (default: REDIS_URL env or redis://redis-worker-1:6379/0)",
    )
    ap.add_argument(
        "--stream",
        default=os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm"),
        help="Metrics stream name (default: ML_CONFIRM_METRICS_STREAM env or metrics:ml_confirm)",
    )
    ap.add_argument(
        "--count",
        type=int,
        default=5,
        help="Количество последних событий для проверки (default: 5)",
    )
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    items = r.xrevrange(args.stream, count=args.count)

    if not items:
        print(f"❌ Нет событий в stream {args.stream}")
        return

    # Основные поля (ожидаются в плоском schema)
    core_keys = [
        "ts_ms",
        "symbol",
        "sid",
        "bucket",
        "direction",
        "scenario_v4",
        "mode",
        "enforce",
        "share_used",
        "ok_rule",
        "missing",
        "kind",
        "status",
        "err",
        "reason",
        "model_run_id",
        "lat_ms",
        "latency_us",
        "missing_n",
        "p_edge",
        "p_min",
        "conf",
    ]

    # Расширенные поля (часто лежат в payload/indicators; показываем если есть)
    extra_keys = [
        # schema / signature pinning
        "schema_name",
        "schema_version",
        "schema_hash",
        "model_sig",
        "model_ver",
        # exec-cost
        "spread_bps",
        "expected_slippage_bps",
        "exec_risk_norm",
        "exec_risk_bps",
        "exec_risk_ref_bps",
        "exec_pen",
        # dq / staleness (если writer кладёт)
        "tick_time_age_p99_ms",
        "book_stale_p99_ms",
        "dq_flag_rate",
    ]

    print(f"\n{'='*80}")
    print(f"ПОСЛЕДНИЕ {len(items)} СОБЫТИЙ ИЗ {args.stream}")
    print(f"{'='*80}\n")

    status_counts: dict[str, int] = {}
    err_counts: dict[str, int] = {}
    kind_counts: dict[str, int] = {}
    model_run_ids: set[str] = set()

    for msg_id, raw_fields in items:
        fields = _merge_payload_fields(dict(raw_fields))

        print(f"ID: {msg_id}")
        print("-" * 80)

        shown_any = False
        for k in core_keys:
            v = fields.get(k)
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            print(f"  {k:20s}: {v}")
            shown_any = True

        # extras (only if present)
        extras = [(k, fields.get(k)) for k in extra_keys if k in fields]
        if extras:
            print("  -- extras --")
            for k, v in extras:
                if v is None:
                    continue
                if isinstance(v, str) and not v.strip():
                    continue
                print(f"  {k:20s}: {v}")

        if not shown_any:
            # fallback: show available keys
            print("  (нет core полей; доступны ключи)")
            print("  keys:", ", ".join(sorted(fields.keys())))

        # stats
        status = (fields.get("status", "") or "").strip()
        if status:
            status_counts[status] = status_counts.get(status, 0) + 1

        err = (fields.get("err", "") or "").strip()
        if err:
            err_counts[err] = err_counts.get(err, 0) + 1

        kind = (fields.get("kind", "") or "").strip()
        if kind:
            kind_counts[kind] = kind_counts.get(kind, 0) + 1

        model_run_id = (fields.get("model_run_id", "") or "").strip()
        if model_run_id:
            model_run_ids.add(model_run_id)

        print()

    # Сводка
    print(f"{'='*80}")
    print("СВОДКА")
    print(f"{'='*80}")

    # Проверка статусов
    print("\n📊 Статусы:")
    if status_counts:
        for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
            marker = "✅" if status != "ERR_NO_CFG" else "❌"
            print(f"  {marker} {status:30s}: {count}")
    else:
        print("  ⚠️  Нет данных о статусах")

    # Проверка ошибок
    print("\n📊 Ошибки (err):")
    if err_counts:
        for err, count in sorted(err_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  ❌ {err:30s}: {count}")
    else:
        print("  ✅ Нет ошибок")

    # Проверка kind
    print("\n📊 Типы моделей (kind):")
    if kind_counts:
        for kind, count in sorted(kind_counts.items(), key=lambda x: x[1], reverse=True):
            marker = "✅" if kind == "util_mh_v1" else "⚠️"
            print(f"  {marker} {kind:30s}: {count}")
    else:
        print("  ⚠️  Нет данных о типах моделей")

    # Проверка model_run_id
    print("\n📊 Model Run IDs:")
    if model_run_ids:
        for run_id in sorted(model_run_ids):
            print(f"  ✅ {run_id}")
    else:
        print("  ⚠️  Нет model_run_id")

    # Итоговая оценка
    print(f"\n{'='*80}")
    print("ИТОГОВАЯ ОЦЕНКА")
    print(f"{'='*80}")

    has_err_no_cfg = "ERR_NO_CFG" in status_counts
    has_util_mh_v1 = "util_mh_v1" in kind_counts
    has_model_run_id = len(model_run_ids) > 0
    has_errors = len(err_counts) > 0

    issues: list[str] = []
    if has_err_no_cfg:
        issues.append("❌ status=ERR_NO_CFG обнаружен")
    if not has_util_mh_v1:
        issues.append("⚠️  kind=util_mh_v1 не найден")
    if not has_model_run_id:
        issues.append("⚠️  model_run_id пустой или отсутствует")
    if has_errors:
        issues.append(f"⚠️  Обнаружены ошибки: {', '.join(sorted(err_counts.keys()))}")

    if not issues:
        print("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ")
        print("   - status не ERR_NO_CFG")
        print("   - kind=util_mh_v1")
        print("   - model_run_id присутствует")
        print("   - err/reason пустые или 'ok'")
    else:
        print("⚠️  ОБНАРУЖЕНЫ ПРОБЛЕМЫ:")
        for issue in issues:
            print(f"   {issue}")

    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    main()

