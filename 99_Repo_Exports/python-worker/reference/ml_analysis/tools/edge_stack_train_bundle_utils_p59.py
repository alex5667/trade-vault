"""P59 helpers for edge_stack_v1 nightly training bundle.

Goals:
  - deterministic validation of dataset builder output
  - atomic artifact writes
  - minimal Redis metrics recording (for exporters/alerts)
  - optional monitoring-smoke gate helpers (auto-promote safety)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple, Optional

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def now_ms() -> int:
    return get_ny_time_millis()


def atomic_write_text(path: str, s: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(s)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_write_json(path: str, obj: Any) -> None:
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2))


def atomic_copy(src: str, dst: str) -> None:
    # copy to tmp then replace (atomic on same fs)
    import shutil

    os.makedirs(os.path.dirname(os.path.abspath(dst)) or ".", exist_ok=True)
    tmp = f"{dst}.tmp"
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


@dataclass
class DatasetValidation:
    ok: bool
    reason: str
    joined: int
    pos_rate: float


@dataclass
class SmokeGate:
    ok: bool
    reason: str
    age_s: int


def read_monitoring_smoke_gate(
    redis_url: str,
    key: str = "metrics:monitoring_smoke:last",
    max_age_s: int = 21600,
    fail_mode: str = "fail_closed",
) -> SmokeGate:
    """Read monitoring smoke status from Redis and return a gate decision.

    fail_mode:
      - fail_closed: missing/err => ok=False
      - fail_open:   missing/err => ok=True
    """
    fail_mode = (fail_mode or "fail_closed").strip().lower()
    try:
        r = redis_client(redis_url)
        d = r.hgetall(key) or {}
        if not d:
            return SmokeGate(ok=(fail_mode == "fail_open"), reason="missing", age_s=10**9)
        success = str(d.get("success", "0"))
        updated_ts_ms = int(float(d.get("updated_ts_ms", "0") or 0))
        age_s = int(max(0, (get_ny_time_millis() - updated_ts_ms) / 1000)) if updated_ts_ms > 0 else 10**9
        if age_s > int(max_age_s):
            return SmokeGate(ok=(fail_mode == "fail_open"), reason=f"stale age_s={age_s} > {max_age_s}", age_s=age_s)
        if success not in ("1", "true", "yes"):
            return SmokeGate(ok=False, reason=f"failed reason={d.get('reason','failed')}", age_s=age_s)
        return SmokeGate(ok=True, reason="ok", age_s=age_s)
    except Exception as e:
        return SmokeGate(ok=(fail_mode == "fail_open"), reason=f"error:{type(e).__name__}", age_s=10**9)


def validate_dataset_report(
    report: Dict[str, Any],
    min_joined: int = 200,
    pos_rate_min: float = 0.05,
    pos_rate_max: float = 0.60,
) -> DatasetValidation:
    joined = int(report.get("joined", 0) or 0)
    pos_rate = float(report.get("pos_rate", 0.0) or 0.0)

    if joined < int(min_joined):
        return DatasetValidation(False, f"dataset_too_small joined={joined} < {min_joined}", joined, pos_rate)

    if not (float(pos_rate_min) <= pos_rate <= float(pos_rate_max)):
        return DatasetValidation(
            False, f"pos_rate_out_of_range pos_rate={pos_rate:.6f} not in [{pos_rate_min},{pos_rate_max}]", joined, pos_rate
        )

    return DatasetValidation(True, "ok", joined, pos_rate)


@dataclass
class TrainValidation:
    ok: bool
    reason: str
    brier: float
    ece: float


def validate_train_report(
    report: Dict[str, Any],
    *,
    brier_max: float = 0.30,
    ece_max: float = 0.08,
) -> TrainValidation:
    """Validate OOF train report for edge_stack_v1.

    Only uses stable metrics that exist in the training report:
      report["oof"]["meta"]["brier"], report["oof"]["meta"]["ece"]

    If missing, returns fail (treat as broken tool output).
    """
    try:
        oof = report.get("oof") or {}
        meta = oof.get("meta") or {}
        brier = float(meta.get("brier", 0.0) or 0.0)
        ece = float(meta.get("ece", 0.0) or 0.0)
    except Exception:
        brier, ece = 0.0, 0.0

    if "oof" not in report or "meta" not in (report.get("oof") or {}):
        return TrainValidation(False, "missing_oof_meta", brier, ece)

    if brier > float(brier_max):
        return TrainValidation(False, f"brier_too_high brier={brier:.6f} > {brier_max}", brier, ece)

    if ece > float(ece_max):
        return TrainValidation(False, f"ece_too_high ece={ece:.6f} > {ece_max}", brier, ece)

    return TrainValidation(True, "ok", brier, ece)


def redis_client(redis_url: str):
    if redis is None:
        raise RuntimeError("redis-py is required")
    return redis.Redis.from_url(redis_url, decode_responses=True)


def write_train_metrics(
    redis_url: str,
    key: str,
    mapping: Dict[str, Any],
) -> None:
    try:
        r = redis_client(redis_url)
        flat: Dict[str, str] = {}
        for k, v in mapping.items():
            if v is None:
                continue
            if isinstance(v, (dict, list)):
                flat[k] = json.dumps(v, ensure_ascii=False, sort_keys=True)
            else:
                flat[k] = str(v)
        if flat:
            r.hset(key, mapping=flat)
            r.hset(key, mapping={"updated_ts_ms": str(now_ms())})
    except Exception:
        # Metrics are best-effort; never fail the training bundle due to Redis errors.
        return
