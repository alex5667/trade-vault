#!/usr/bin/env python3
from __future__ import annotations
"""of_gate_timescale_policy_probe_v1.py

Probe TimescaleDB policy/job health for OF-gate rollups.

Checks (if Timescale extension is present):
  - retention policy exists for hypertables:
      - of_gate_metrics
      - of_gate_metrics_quarantine
  - refresh policy exists for continuous aggregates:
      - of_gate_ok_rate_5m
      - of_gate_ok_rate_1h

Writes (best-effort) a Redis hash for Prometheus exporter:
  key: metrics:of_gate_timescale_policies

ENV:
  TRADES_DB_DSN (or PG_DSN / DATABASE_URL) [required]
  REDIS_URL (optional)

  ENABLE probe gating:
    OF_GATE_TIMESCALE_POLICY_EXPECT=1 (default 1)

  Redis key:
    OF_GATE_TIMESCALE_POLICIES_METRICS_KEY=metrics:of_gate_timescale_policies

Exit:
  0: ok
  2: missing/disabled policies (when expect_timescale=1) OR db error
"""

from utils.time_utils import get_ny_time_millis

import datetime as dt
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg2  # type: ignore

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def env(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default


def now_ms() -> int:
    return get_ny_time_millis()


def _b(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", "replace")
        except Exception:
            return ""
    return str(v)


def _i(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "replace")
        return int(float(v))
    except Exception:
        return default


def _bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = _b(v).strip().lower()
    if s in ("1", "true", "t", "yes", "y", "on"):
        return True
    if s in ("0", "false", "f", "no", "n", "off"):
        return False
    return None


def _cols(conn, schema: str, table: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
            """
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]


def _timescale_present(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname='timescaledb' LIMIT 1")
        return cur.fetchone() is not None


def _fetch_cagg_mat_names(conn) -> Dict[str, str]:
    """Return mapping: view_name -> materialization_hypertable_name (best-effort)."""
    out: Dict[str, str] = {}
    try:
        cols = _cols(conn, "timescaledb_information", "continuous_aggregates")
        # common columns across TS versions:
        #  - view_name
        #  - materialization_hypertable_name (preferred)
        #  - materialization_hypertable
        view_col = "view_name" if "view_name" in cols else None
        mat_col = None
        for c in (
            "materialization_hypertable_name",
            "materialization_hypertable",
            "materialized_hypertable_name",
        ):
            if c in cols:
                mat_col = c
                break
        if not view_col or not mat_col:
            return out

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {view_col}, {mat_col} FROM timescaledb_information.continuous_aggregates"
            )
            for v, m in cur.fetchall():
                vs = _b(v)
                ms = _b(m)
                if vs and ms:
                    out[vs] = ms
    except Exception:
        return out
    return out


def _jobs(conn) -> Tuple[List[str], List[Dict[str, Any]]]:
    cols = _cols(conn, "timescaledb_information", "jobs")
    # fetch subset of columns if possible
    wanted = [c for c in ("job_id", "proc_name", "proc_schema", "scheduled", "config", "hypertable_schema", "hypertable_name") if c in cols]
    if not wanted:
        return cols, []
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(wanted)} FROM timescaledb_information.jobs")
        rows = []
        for r in cur.fetchall():
            d: Dict[str, Any] = {}
            for i, c in enumerate(wanted):
                d[c] = r[i]
            rows.append(d)
        return wanted, rows


def _match_jobs(
    jobs: List[Dict[str, Any]],
    proc_contains: str,
    hypertable_names: List[str] = None,
    config_contains: List[str] = None,
) -> List[Dict[str, Any]]:
    hypertable_names = hypertable_names or []
    config_contains = config_contains or []

    out: List[Dict[str, Any]] = []
    for j in jobs:
        pn = _b(j.get("proc_name")).lower()
        if proc_contains.lower() not in pn:
            continue

        ht = _b(j.get("hypertable_name"))
        cfg = _b(j.get("config"))

        ok_ht = True
        if hypertable_names:
            ok_ht = ht in hypertable_names

        ok_cfg = True
        if config_contains:
            t = cfg.lower()
            ok_cfg = any(s.lower() in t for s in config_contains)

        if ok_ht and ok_cfg:
            out.append(j)
    return out


def _hset_redis(redis_url: str, key: str, mapping: Dict[str, Any]) -> None:
    if not redis or not redis_url:
        return
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        out = {str(k): str(v) for k, v in mapping.items() if v is not None}
        if out:
            r.hset(key, mapping=out)
    except Exception:
        return


def main() -> None:
    dsn = env("TRADES_DB_DSN", "PG_DSN", "DATABASE_URL", default="")
    if not dsn:
        print("TRADES_DB_DSN is required", file=sys.stderr)
        raise SystemExit(2)

    redis_url = env("REDIS_URL", default="")
    key = env("OF_GATE_TIMESCALE_POLICIES_METRICS_KEY", default="metrics:of_gate_timescale_policies")
    expect_ts = env("OF_GATE_TIMESCALE_POLICY_EXPECT", default="1")
    expect_timescale = 1 if str(expect_ts).strip() == "1" else 0

    payload: Dict[str, Any] = {
        "last_run_ts_ms": now_ms(),
        "expect_timescale": expect_timescale,
        "ok": 0,
        "timescale_present": 0,
        "missing_count": 0,
        "disabled_count": 0,
    }

    required_policies = [
        "retention_of_gate_metrics",
        "retention_of_gate_metrics_quarantine",
        "refresh_of_gate_ok_rate_5m",
        "refresh_of_gate_ok_rate_1h",
    ]

    present: Dict[str, int] = {p: 0 for p in required_policies}
    disabled: Dict[str, int] = {p: 0 for p in required_policies}

    missing_list: List[str] = []
    disabled_list: List[str] = []

    try:
        conn = psycopg2.connect(dsn)
        try:
            ts_present = _timescale_present(conn)
            payload["timescale_present"] = 1 if ts_present else 0
            if not ts_present:
                payload["ok"] = 1 if expect_timescale == 0 else 0
                payload["error"] = "timescaledb_extension_missing"
            else:
                _wanted_cols, jobs = _jobs(conn)

                # cagg materialization names (best-effort). If we can detect them,
                # prefer hypertable_name matching for refresh policies.
                mats = _fetch_cagg_mat_names(conn)
                mat_5m = mats.get("of_gate_ok_rate_5m", "")
                mat_1h = mats.get("of_gate_ok_rate_1h", "")
                mat_names = [x for x in (mat_5m, mat_1h) if x]

                # Retention policies
                ret_jobs_metrics = _match_jobs(
                    jobs,
                    proc_contains="policy_retention",
                    hypertable_names=["of_gate_metrics"],
                    config_contains=["of_gate_metrics"],
                )
                ret_jobs_quarantine = _match_jobs(
                    jobs,
                    proc_contains="policy_retention",
                    hypertable_names=["of_gate_metrics_quarantine"],
                    config_contains=["of_gate_metrics_quarantine"],
                )

                # Refresh policies: try mat hypertable name first; fallback to config search.
                refresh_jobs_5m = []
                refresh_jobs_1h = []
                if mat_5m:
                    refresh_jobs_5m = _match_jobs(
                        jobs,
                        proc_contains="policy_refresh_continuous_aggregate",
                        hypertable_names=[mat_5m],
                        config_contains=[],
                    )
                if not refresh_jobs_5m:
                    refresh_jobs_5m = _match_jobs(
                        jobs,
                        proc_contains="policy_refresh_continuous_aggregate",
                        hypertable_names=[],
                        config_contains=["of_gate_ok_rate_5m"],
                    )

                if mat_1h:
                    refresh_jobs_1h = _match_jobs(
                        jobs,
                        proc_contains="policy_refresh_continuous_aggregate",
                        hypertable_names=[mat_1h],
                        config_contains=[],
                    )
                if not refresh_jobs_1h:
                    refresh_jobs_1h = _match_jobs(
                        jobs,
                        proc_contains="policy_refresh_continuous_aggregate",
                        hypertable_names=[],
                        config_contains=["of_gate_ok_rate_1h"],
                    )

                def _scheduled(job: Dict[str, Any]) -> Optional[bool]:
                    if "scheduled" not in job:
                        return None
                    return _bool(job.get("scheduled"))

                if ret_jobs_metrics:
                    present["retention_of_gate_metrics"] = 1
                    s = _scheduled(ret_jobs_metrics[0])
                    if s is False:
                        disabled["retention_of_gate_metrics"] = 1

                if ret_jobs_quarantine:
                    present["retention_of_gate_metrics_quarantine"] = 1
                    s = _scheduled(ret_jobs_quarantine[0])
                    if s is False:
                        disabled["retention_of_gate_metrics_quarantine"] = 1

                if refresh_jobs_5m:
                    present["refresh_of_gate_ok_rate_5m"] = 1
                    s = _scheduled(refresh_jobs_5m[0])
                    if s is False:
                        disabled["refresh_of_gate_ok_rate_5m"] = 1

                if refresh_jobs_1h:
                    present["refresh_of_gate_ok_rate_1h"] = 1
                    s = _scheduled(refresh_jobs_1h[0])
                    if s is False:
                        disabled["refresh_of_gate_ok_rate_1h"] = 1

                for p in required_policies:
                    if present[p] != 1:
                        missing_list.append(p)
                    if disabled[p] == 1:
                        disabled_list.append(p)

                payload.update({f"present_{k}": v for k, v in present.items()})
                payload.update({f"disabled_{k}": v for k, v in disabled.items()})
                payload["missing_count"] = len(missing_list)
                payload["disabled_count"] = len(disabled_list)
                payload["missing_policies"] = ",".join(missing_list)
                payload["disabled_policies"] = ",".join(disabled_list)
                payload["mat_5m"] = mat_5m
                payload["mat_1h"] = mat_1h

                ok = 1
                if expect_timescale == 1 and (len(missing_list) > 0 or len(disabled_list) > 0):
                    ok = 0
                payload["ok"] = ok
        finally:
            conn.close()
    except Exception as e:
        payload["ok"] = 0
        payload["error"] = f"db_error:{type(e).__name__}"

    # Persist in Redis
    if redis_url:
        # keep details small
        details = {
            "timescale_present": payload.get("timescale_present"),
            "missing": missing_list,
            "disabled": disabled_list,
            "mat_5m": payload.get("mat_5m", ""),
            "mat_1h": payload.get("mat_1h", ""),
        }
        payload["details_json"] = json.dumps(details, ensure_ascii=False)[:1800]
        _hset_redis(redis_url, key, payload)

    # Print for logs
    print({k: payload[k] for k in sorted(payload.keys()) if k not in ("details_json",)})

    if payload.get("ok") != 1:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
