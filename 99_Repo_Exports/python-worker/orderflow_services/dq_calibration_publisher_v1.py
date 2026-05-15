from __future__ import annotations

"""dq_calibration_publisher_v1.py

Применяет вывод dq_gate_calibrator_v1 (Redis key `cfg:dq_gate:v1:calibration`)
к dynamic_cfg, выставляя HSET по каждому весу/порогу в `settings:dynamic_cfg`,
откуда их читает services/orderflow/configuration.py при сборке cfg2.

Идемпотентность
  Хранит last_applied_run_id и last_applied_ms в Redis-ключе
  V14_DQ_CAL_PUBLISHER_STATE_KEY (default cfg:dq_gate:v1:publisher_state).
  Если run_id калибровки совпадает — публикация пропускается.

Безопасность
  - skip если `gates_passed != true`
  - skip если `calibrated_ms` старее MAX_AGE_HOURS (default 48h)
  - skip если кандидатные веса не проходят bounds-валидацию
  - перед HSET сохраняет предыдущие значения в `cfg:dq_gate:v1:publisher_rollback`

Run
  python -m orderflow_services.dq_calibration_publisher_v1
  python -m orderflow_services.dq_calibration_publisher_v1 --apply 0    # dry-run
"""

import argparse
import json
import os
import time
from typing import Any

# Mirror of bounds from dq_gate_calibrator_v1 — duplicated to keep this
# publisher independent (it must validate even if calibrator module is absent).
WEIGHT_BOUNDS: dict[str, tuple[float, float]] = {
    "dq_pen_weight_gap_soft": (0.50, 0.98),
    "dq_pen_weight_gap_hard": (0.05, 0.60),
    "dq_pen_weight_tick_seq_soft": (0.50, 0.98),
    "dq_pen_weight_tick_seq_hard": (0.05, 0.70),
    "dq_pen_weight_book_seq_soft": (0.50, 0.98),
    "dq_pen_weight_book_seq_hard": (0.05, 0.60),
    "dq_pen_weight_nan_soft": (0.30, 0.95),
    "dq_pen_weight_nan_hard": (0.05, 0.60),
    "dq_pen_weight_stuck_soft": (0.40, 0.98),
    "dq_pen_weight_stuck_hard": (0.05, 0.60),
    "dq_pen_weight_latency_soft": (0.05, 0.50),
    "dq_pen_weight_skew_now_soft": (0.40, 0.95),
    "dq_pen_weight_skew_stream_soft": (0.50, 0.98),
}


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[{ts}] [dq_calibration_publisher] {msg}", flush=True)


def _validate_weights(weights: dict[str, Any]) -> tuple[dict[str, float], list[str]]:
    """Filter+coerce weights to floats in bounds. Returns (valid, errors)."""
    valid: dict[str, float] = {}
    errors: list[str] = []
    for k, bounds in WEIGHT_BOUNDS.items():
        if k not in weights:
            errors.append(f"missing key {k}")
            continue
        try:
            v = float(weights[k])
        except Exception:
            errors.append(f"{k} not a float: {weights[k]!r}")
            continue
        lo, hi = bounds
        if not (lo <= v <= hi):
            errors.append(f"{k}={v} out of bounds [{lo},{hi}]")
            continue
        valid[k] = v
    return valid, errors


def _notify(redis_main_url: str, text: str, severity: str, dedup_key: str | None,
            notify_stream: str) -> None:
    try:
        import redis as redis_lib
        r = redis_lib.Redis.from_url(redis_main_url, decode_responses=True)
        if dedup_key:
            d_key = f"dedup:reporting:{dedup_key}"
            if not r.set(d_key, "1", nx=True, ex=6 * 3600):
                return
        r.xadd(notify_stream, {
            "type": "report",
            "text": text,
            "parse_mode": "HTML",
            "source": "dq_calibration_publisher_v1",
            "severity": severity,
            "timestamp": str(int(time.time() * 1000)),
            **({"dedup_key": dedup_key} if dedup_key else {}),
        }, maxlen=5000)
    except Exception as exc:
        _log(f"notify error: {exc}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Apply DQ Gate calibration to settings:dynamic_cfg")
    ap.add_argument("--redis-url", default=_env("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--redis-main-url",
                    default=_env("REDIS_MAIN_URL", _env("REDIS_URL", "redis://redis:6379/0")))
    ap.add_argument("--cal-key", default=_env("V14_DQ_CAL_KEY", "cfg:dq_gate:v1:calibration"))
    ap.add_argument("--dynamic-cfg-key",
                    default=_env("DYNAMIC_CFG_KEY", "settings:dynamic_cfg"))
    ap.add_argument("--state-key",
                    default=_env("V14_DQ_CAL_PUBLISHER_STATE_KEY",
                                 "cfg:dq_gate:v1:publisher_state"))
    ap.add_argument("--rollback-key",
                    default=_env("V14_DQ_CAL_PUBLISHER_ROLLBACK_KEY",
                                 "cfg:dq_gate:v1:publisher_rollback"))
    ap.add_argument("--max-age-hours", type=float,
                    default=_env_float("V14_DQ_CAL_PUBLISHER_MAX_AGE_H", 48.0))
    ap.add_argument("--publish-thresholds", type=int,
                    default=_env_int("V14_DQ_CAL_PUBLISHER_PUBLISH_THR", 0),
                    help="1 = also HSET threshold recommendations (rec_dq_* → dq_tick_gap_p95_soft_ms etc.). Default 0.")
    ap.add_argument("--notify-stream", default=_env("NOTIFY_STREAM", "notify:telegram"))
    ap.add_argument("--apply", type=int, default=_env_int("V14_DQ_CAL_PUBLISHER_APPLY", 1),
                    help="1 = HSET to Redis; 0 = dry-run (log only)")
    args = ap.parse_args()

    try:
        import redis as redis_lib
    except Exception as exc:
        _log(f"redis lib missing: {exc}")
        return 1

    try:
        r = redis_lib.Redis.from_url(args.redis_url, decode_responses=True)
    except Exception as exc:
        _log(f"redis connect failed: {exc}")
        return 1

    raw = r.get(args.cal_key)
    if not raw:
        _log(f"calibration key {args.cal_key} not set — nothing to publish")
        return 0

    try:
        payload: dict[str, Any] = json.loads(str(raw))
    except Exception as exc:
        _log(f"calibration JSON parse error: {exc}")
        return 1

    run_id = str(payload.get("run_id") or "")
    calibrated_ms = int(payload.get("calibrated_ms") or 0)
    gates_passed = bool(payload.get("gates_passed"))
    weights = payload.get("weights") or {}
    thresholds = payload.get("thresholds") or {}

    if not gates_passed:
        _log(f"gates_passed=False, skipping. blockers={payload.get('blockers')}")
        return 0

    now_ms = int(time.time() * 1000)
    age_h = (now_ms - calibrated_ms) / 3_600_000 if calibrated_ms > 0 else 1e9
    if age_h > args.max_age_hours:
        _log(f"calibration too stale: age={age_h:.1f}h > max={args.max_age_hours}h")
        return 0

    # Idempotency
    state_raw = r.get(args.state_key)
    state: dict[str, Any] = {}
    if state_raw:
        try:
            state = json.loads(str(state_raw))
        except Exception:
            state = {}
    if state.get("last_applied_run_id") == run_id:
        _log(f"run_id {run_id} already applied, skipping")
        return 0

    # Validate weights
    valid_weights, errors = _validate_weights(weights)
    if errors:
        _log(f"validation errors: {errors}")
        if len(valid_weights) == 0:
            return 1

    # Threshold recommendations → cfg2 keys mapping
    thr_pairs: list[tuple[str, str]] = []
    if args.publish_thresholds and thresholds:
        thr_map = {
            "rec_dq_tick_gap_p95_soft_ms": "dq_tick_gap_p95_soft_ms",
            "rec_dq_tick_gap_p95_hard_ms": "dq_tick_gap_p95_hard_ms",
            "rec_dq_tick_missing_seq_soft": "dq_tick_missing_seq_soft",
            "rec_dq_tick_missing_seq_hard": "dq_tick_missing_seq_hard",
            "rec_dq_book_missing_seq_soft": "dq_book_missing_seq_soft",
            "rec_dq_book_missing_seq_hard": "dq_book_missing_seq_hard",
        }
        for src, dst in thr_map.items():
            if src in thresholds:
                try:
                    thr_pairs.append((dst, f"{float(thresholds[src]):g}"))
                except Exception:
                    pass

    all_pairs: list[tuple[str, str]] = [(k, f"{v:g}") for k, v in valid_weights.items()]
    all_pairs += thr_pairs

    if not all_pairs:
        _log("nothing to publish")
        return 0

    # Read previous values for rollback
    prev_values: dict[str, str | None] = {}
    try:
        from typing import cast as _cast
        for k, _ in all_pairs:
            prev_values[k] = _cast("str | None", r.hget(args.dynamic_cfg_key, k))
    except Exception as exc:
        _log(f"hget for rollback failed: {exc}")
        prev_values = {}

    _log(f"applying {len(all_pairs)} keys to {args.dynamic_cfg_key} (run_id={run_id}, age={age_h:.1f}h)")
    for k, v in all_pairs:
        _log(f"  HSET {args.dynamic_cfg_key} {k} {v}  (was: {prev_values.get(k)})")

    if not args.apply:
        _log("dry-run (--apply=0): no HSET performed")
        return 0

    # Persist rollback snapshot BEFORE applying
    try:
        rollback_payload = {
            "ts_ms": now_ms,
            "run_id": run_id,
            "dynamic_cfg_key": args.dynamic_cfg_key,
            "previous": prev_values,
        }
        r.set(args.rollback_key,
              json.dumps(rollback_payload, ensure_ascii=False, separators=(",", ":")))
    except Exception as exc:
        _log(f"failed to persist rollback snapshot: {exc}")
        return 1

    # Apply HSETs
    try:
        pipe = r.pipeline(transaction=False)
        for k, v in all_pairs:
            pipe.hset(args.dynamic_cfg_key, k, v)
        pipe.execute()
    except Exception as exc:
        _log(f"HSET pipeline failed: {exc}")
        return 1

    # Update state
    state.update({
        "last_applied_run_id": run_id,
        "last_applied_ms": now_ms,
        "n_keys": len(all_pairs),
        "calibrated_ms": calibrated_ms,
    })
    try:
        r.set(args.state_key,
              json.dumps(state, ensure_ascii=False, separators=(",", ":")))
    except Exception as exc:
        _log(f"state write failed: {exc}")

    # Notify
    lines = [
        "✅ <b>DQ Calibration Publisher — применено</b>",
        "",
        f"<b>Run:</b> <code>{run_id}</code>",
        f"<b>Возраст калибровки:</b> {age_h:.1f}ч",
        f"<b>Ключей применено:</b> {len(all_pairs)} (weights={len(valid_weights)}, thresholds={len(thr_pairs)})",
        "",
        "<b>Веса:</b>",
    ]
    for k, v in sorted(valid_weights.items()):
        prev = prev_values.get(k) or "(default)"
        lines.append(f"  {k}: <code>{prev}</code> → <code>{v:.3f}</code>")
    if thr_pairs:
        lines.append("")
        lines.append("<b>Thresholds:</b>")
        for k, v in thr_pairs:
            prev = prev_values.get(k) or "(default)"
            lines.append(f"  {k}: <code>{prev}</code> → <code>{v}</code>")
    _notify(args.redis_main_url, "\n".join(lines),
            severity="info",
            dedup_key=f"dq_cal_pub_{run_id}",
            notify_stream=args.notify_stream)

    _log(f"published run_id={run_id} keys={len(all_pairs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
