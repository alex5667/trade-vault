from __future__ import annotations
"""P82: Staleness gates for enforce-bucket automation.

Goal:
  If data sources are stale (Redis stream lag, DB view stale, status files stale),
  promoter/freezer/rollback must NOT apply changes.

This module is intentionally dependency-light and deterministic.

ENV defaults (conservative):
  ENFORCE_MAX_REDIS_STREAM_AGE_SEC=900
  ENFORCE_MIN_REDIS_EVENTS=50
  ENFORCE_MAX_DB_VIEW_AGE_SEC=1800
  ENFORCE_MIN_DB_ROWS=80
  ENFORCE_MAX_STATUS_FILE_AGE_SEC=1800

  ENFORCE_REDIS_STREAM=metrics:of_gate
  ENFORCE_REDIS_STREAM_SCAN=2000
  ENFORCE_DB_VIEW_NAME=v_exec_slippage_eval
  ENFORCE_DB_VIEW_TS_COL=ts

Return shape:
  {
    "blocked": bool,
    "severity": "soft"|"hard",
    "reasons": ["..."]
    "checks": { ... details ... }
  }
"""


import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


def _now_s() -> int:
    return int(time.time())


def _env_int(name: str, default: int) -> int:
    v = str(os.getenv(name, str(default))).strip()
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return str(v).strip() if v is not None and str(v).strip() else str(default)


def check_status_file_staleness(path: str, max_age_sec: int) -> Dict[str, Any]:
    """Checks mtime age of a local status/report file."""
    out: Dict[str, Any] = {
        "ok": True,
        "path": path,
        "age_sec": None,
        "max_age_sec": max_age_sec,
        "reason": "ok",
    }
    p = str(path or "").strip()
    if not p:
        out["ok"] = False
        out["reason"] = "empty_path"
        return out
    try:
        st = os.stat(p)
        age = max(0, _now_s() - int(st.st_mtime))
        out["age_sec"] = int(age)
        if age > max_age_sec:
            out["ok"] = False
            out["reason"] = "stale"
        return out
    except FileNotFoundError:
        out["ok"] = False
        out["reason"] = "missing"
        return out
    except Exception as e:
        out["ok"] = False
        out["reason"] = f"error:{type(e).__name__}"
        return out


def _stream_last_entry(redis_client: Any, stream: str) -> Tuple[Optional[str], Optional[int]]:
    """Returns (last_id, last_ts_ms) or (None, None)."""
    try:
        # XREVRANGE stream + - COUNT 1
        rows = redis_client.xrevrange(stream, "+", "-", count=1)
        if not rows:
            return None, None
        last_id = rows[0][0]
        # id is like 'ms-seq'
        ms = int(str(last_id).split("-")[0])
        return str(last_id), ms
    except Exception:
        return None, None


def check_redis_stream_staleness(
    redis_client: Any,
    stream: str,
    max_age_sec: int,
    min_events: int,
    scan: int,
) -> Dict[str, Any]:
    """Checks staleness and basic liveness of a Redis stream."""
    out: Dict[str, Any] = {
        "ok": True,
        "stream": stream,
        "age_sec": None,
        "max_age_sec": max_age_sec,
        "min_events": min_events,
        "scan": scan,
        "last_id": None,
        "reason": "ok",
    }
    try:
        info = redis_client.xinfo_stream(stream)
        length = int(info.get("length", 0) or 0)
        out["length"] = length
        if length < min_events:
            out["ok"] = False
            out["reason"] = "too_few_events"
            return out
        last_id, last_ms = _stream_last_entry(redis_client, stream)
        out["last_id"] = last_id
        if last_ms is None:
            out["ok"] = False
            out["reason"] = "no_last_id"
            return out
        age = max(0, _now_s() - int(last_ms / 1000))
        out["age_sec"] = int(age)
        if age > max_age_sec:
            out["ok"] = False
            out["reason"] = "stale"
        return out
    except Exception as e:
        out["ok"] = False
        out["reason"] = f"error:{type(e).__name__}"
        return out


def check_db_view_staleness(
    conn: Any,
    view: str,
    ts_col: str,
    max_age_sec: int,
    min_rows: int,
) -> Dict[str, Any]:
    """Checks freshness of a DB view/MV by last timestamp and row count in recent window."""
    out: Dict[str, Any] = {
        "ok": True,
        "view": view,
        "ts_col": ts_col,
        "age_sec": None,
        "max_age_sec": max_age_sec,
        "min_rows": min_rows,
        "last_ts": None,
        "rows": None,
        "reason": "ok",
    }
    v = str(view or "").strip()
    c = str(ts_col or "").strip()
    if not v or not c:
        out["ok"] = False
        out["reason"] = "empty_view_or_ts_col"
        return out
    try:
        with conn.cursor() as cur:
            cur.execute(f"select max({c}) from {v}")
            last = cur.fetchone()[0]
            out["last_ts"] = str(last) if last is not None else None
            if last is None:
                out["ok"] = False
                out["reason"] = "no_last_ts"
                return out
            # Try to compute age_sec using epoch extraction (works for timestamptz).
            cur.execute(f"select extract(epoch from (now() - max({c})))::bigint from {v}")
            age = cur.fetchone()[0]
            out["age_sec"] = int(age) if age is not None else None
            if out["age_sec"] is None:
                out["ok"] = False
                out["reason"] = "age_calc_failed"
                return out
            if int(out["age_sec"]) > max_age_sec:
                out["ok"] = False
                out["reason"] = "stale"
                return out

            # Row count in recent window of max_age_sec.
            cur.execute(
                f"select count(*) from {v} where {c} >= now() - interval '{int(max_age_sec)} seconds'"
            )
            rows = int(cur.fetchone()[0] or 0)
            out["rows"] = rows
            if rows < min_rows:
                out["ok"] = False
                out["reason"] = "too_few_rows"
            return out
    except Exception as e:
        out["ok"] = False
        out["reason"] = f"error:{type(e).__name__}"
        return out


def should_block_apply(checks: Dict[str, Dict[str, Any]]) -> Tuple[bool, str, List[str]]:
    """Decision policy: hard block on infra errors; soft block on 'not enough data' or staleness."""
    reasons: List[str] = []
    hard = False
    for name, r in checks.items():
        if r.get("ok") is True:
            continue
        reason = str(r.get("reason") or "failed")
        reasons.append(f"{name}:{reason}")
        # hard on exceptions/connectivity
        if reason.startswith("error"):
            hard = True
        if reason in ("empty_view_or_ts_col", "empty_path"):
            hard = True

    if not reasons:
        return False, "soft", []
    return True, ("hard" if hard else "soft"), reasons


def run_staleness_gates(
    *,
    redis_client: Optional[Any] = None,
    db_conn: Optional[Any] = None,
    status_files: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Run all configured gates and return structured result."""
    status_files = status_files or {}

    max_redis_age = _env_int("ENFORCE_MAX_REDIS_STREAM_AGE_SEC", 900)
    min_redis_events = _env_int("ENFORCE_MIN_REDIS_EVENTS", 50)
    redis_stream = _env_str("ENFORCE_REDIS_STREAM", "metrics:of_gate")
    redis_scan = _env_int("ENFORCE_REDIS_STREAM_SCAN", 2000)

    max_db_age = _env_int("ENFORCE_MAX_DB_VIEW_AGE_SEC", 1800)
    min_db_rows = _env_int("ENFORCE_MIN_DB_ROWS", 80)
    db_view = _env_str("ENFORCE_DB_VIEW_NAME", "v_exec_slippage_eval")
    db_ts_col = _env_str("ENFORCE_DB_VIEW_TS_COL", "ts")

    max_file_age = _env_int("ENFORCE_MAX_STATUS_FILE_AGE_SEC", 1800)

    checks: Dict[str, Dict[str, Any]] = {}
    if redis_client is not None:
        checks["redis_stream"] = check_redis_stream_staleness(
            redis_client, redis_stream, max_redis_age, min_redis_events, redis_scan
        )
    else:
        checks["redis_stream"] = {"ok": False, "reason": "no_redis"}

    if db_conn is not None:
        checks["db_view"] = check_db_view_staleness(db_conn, db_view, db_ts_col, max_db_age, min_db_rows)
    else:
        checks["db_view"] = {"ok": False, "reason": "no_db"}

    for k, p in status_files.items():
        checks[f"file_{k}"] = check_status_file_staleness(p, max_file_age)

    blocked, severity, reasons = should_block_apply(checks)
    return {
        "blocked": bool(blocked),
        "severity": str(severity),
        "reasons": list(reasons),
        "checks": checks,
        "ts_s": _now_s(),
        "cfg": {
            "redis_stream": redis_stream,
            "db_view": db_view,
            "db_ts_col": db_ts_col,
        },
    }


__all__ = [
    "check_status_file_staleness",
    "check_redis_stream_staleness",
    "check_db_view_staleness",
    "should_block_apply",
    "run_staleness_gates",
]
