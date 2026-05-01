#!/usr/bin/env python3
from __future__ import annotations
"""OFInputs DLQ DB drilldown (P99).

Reads from Timescale/Postgres table `of_inputs_dlq_events` (P98) and prints:
  - top reasons (dq_code / err_prefix)
  - last event age
  - optional samples for a given reason

This is intended for on-call triage + daily summaries.

Usage:
  TRADES_DB_DSN=... python -m orderflow_services.of_inputs_dlq_db_drilldown_p99 --lookback-h 24
  TRADES_DB_DSN=... python -m orderflow_services.of_inputs_dlq_db_drilldown_p99 --reason missing_lob_fields --sample 5

Notify (best-effort):
  REDIS_URL=... TELEGRAM_NOTIFY_STREAM=notify:telegram:crit \
    python -m orderflow_services.of_inputs_dlq_db_drilldown_p99 --notify

ENV (DSN):
  TRADES_DB_DSN / ARCHIVER_PG_DSN / DATABASE_URL / PG_DSN
ENV (notify):
  REDIS_URL (default redis://redis-worker-1:6379/0)
  TELEGRAM_NOTIFY_STREAM / NOTIFY_TELEGRAM_STREAM
"""

from utils.time_utils import get_ny_time_millis

import argparse
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2


def _pick_dsn() -> str:
    return (
        os.getenv("ARCHIVER_PG_DSN")
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))
        or ""
    )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_age_s(age_s: float) -> str:
    if age_s < 0:
        return "0s"
    if age_s < 60:
        return f"{int(age_s)}s"
    if age_s < 3600:
        return f"{age_s/60.0:.1f}m"
    if age_s < 86400:
        return f"{age_s/3600.0:.1f}h"
    return f"{age_s/86400.0:.1f}d"


def _connect(dsn: str):
    return psycopg2.connect(dsn)


def _has_view(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (name,))
        row = cur.fetchone()
        return bool(row and row[0])


def _query_top_reasons(conn, lookback_h: int, kind: str, top_n: int) -> List[Tuple[str, int, datetime]]:
    use_view = _has_view(conn, "public.v_of_inputs_dlq_events_reason_24h")
    with conn.cursor() as cur:
        if use_view and lookback_h == 24:
            sql = """
            SELECT reason, n_events, last_ts
            FROM v_of_inputs_dlq_events_reason_24h
            WHERE (%s = 'all' OR kind = %s)
            ORDER BY n_events DESC
            LIMIT %s
            """
            cur.execute(sql, (kind, kind, top_n))
        else:
            sql = """
            WITH parsed AS (
              SELECT
                ts,
                CASE
                  WHEN stream LIKE 'stream:dlq:%' THEN 'dlq'
                  WHEN stream LIKE 'quarantine:%' THEN 'quarantine'
                  ELSE 'other'
                END AS kind,
                COALESCE(
                  NULLIF(dq_code,''),
                  NULLIF(substring(COALESCE(err,'') from '^([^\\s:]+)'),'') 
                  'unknown'
                ) AS reason
              FROM of_inputs_dlq_events
              WHERE ts >= now() - (%s || ' hours')::interval
            )
            SELECT reason, COUNT(*)::bigint AS n_events, MAX(ts) AS last_ts
            FROM parsed
            WHERE (%s = 'all' OR kind = %s)
            GROUP BY 1
            ORDER BY n_events DESC
            LIMIT %s
            """
            cur.execute(sql, (int(lookback_h), kind, kind, top_n))
        rows = cur.fetchall() or []
    out: List[Tuple[str, int, datetime]] = []
    for r in rows:
        out.append((str(r[0]), int(r[1]), r[2]))
    return out


def _query_last_event(conn, kind: str) -> Optional[datetime]:
    with conn.cursor() as cur:
        sql = """
        SELECT MAX(ts)
        FROM of_inputs_dlq_events
        WHERE (%s = 'all'
          OR (%s='dlq' AND stream LIKE 'stream:dlq:%')
          OR (%s='quarantine' AND stream LIKE 'quarantine:%')
          OR (%s='other' AND stream NOT LIKE 'stream:dlq:%' AND stream NOT LIKE 'quarantine:%')
        )
        """
        cur.execute(sql, (kind, kind, kind, kind))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    return None


def _query_samples(conn, reason: str, kind: str, limit: int) -> List[Dict[str, Any]]:
    use_view = _has_view(conn, "public.v_of_inputs_dlq_events_parsed")
    with conn.cursor() as cur:
        if use_view:
            sql = """
            SELECT ts, kind, reason, symbol, stream, dlq_id, err, dq_code, attempt_version, published_version, missing_fields
            FROM v_of_inputs_dlq_events_parsed
            WHERE reason = %s
              AND (%s='all' OR kind=%s)
            ORDER BY ts DESC
            LIMIT %s
            """
            cur.execute(sql, (reason, kind, kind, limit))
        else:
            sql = """
            WITH parsed AS (
              SELECT
                ts,
                stream,
                dlq_id,
                err,
                dq_code,
                attempt_version,
                published_version,
                missing_fields,
                COALESCE(NULLIF(payload_json->>'symbol',''), NULLIF(payload_json->>'sym',''), NULLIF(payload_json->>'s','')) AS symbol,
                CASE
                  WHEN stream LIKE 'stream:dlq:%' THEN 'dlq'
                  WHEN stream LIKE 'quarantine:%' THEN 'quarantine'
                  ELSE 'other'
                END AS kind,
                COALESCE(
                  NULLIF(dq_code,''),
                  NULLIF(substring(COALESCE(err,'') from '^([^\\s:]+)'),'') 
                  'unknown'
                ) AS reason
              FROM of_inputs_dlq_events
            )
            SELECT ts, kind, reason, symbol, stream, dlq_id, err, dq_code, attempt_version, published_version, missing_fields
            FROM parsed
            WHERE reason=%s AND (%s='all' OR kind=%s)
            ORDER BY ts DESC
            LIMIT %s
            """
            cur.execute(sql, (reason, kind, kind, limit))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall() or []

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = {}
        for i, c in enumerate(cols):
            d[c] = r[i]
        out.append(d)
    return out


def _notify(text: str, severity: str = "crit") -> None:
    try:
        import redis  # type: ignore

        url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        r = redis.Redis.from_url(url, decode_responses=True)
        stream = (
            os.getenv("TELEGRAM_NOTIFY_STREAM")
            or os.getenv("NOTIFY_TELEGRAM_STREAM")
            or ("notify:telegram:crit" if severity == "crit" else "notify:telegram")
        )
        r.xadd(
            stream,
            {
                "message": text,
                "source": "of_inputs_dlq_db_drilldown_p99",
                "ts_ms": str(get_ny_time_millis()),
                "severity": severity,
            },
            maxlen=10000,
            approximate=True,
        )
    except Exception:
        # fail-open
        return


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", type=str, default="")
    ap.add_argument("--lookback-h", type=int, default=24)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--kind", type=str, default="all", choices=["all", "dlq", "quarantine", "other"])
    ap.add_argument("--reason", type=str, default="")
    ap.add_argument("--sample", type=int, default=0)
    ap.add_argument("--notify", action="store_true")
    args = ap.parse_args()

    dsn = args.dsn or _pick_dsn()
    if not dsn:
        raise SystemExit("missing_db_dsn: set TRADES_DB_DSN/ARCHIVER_PG_DSN")

    with _connect(dsn) as conn:
        last_ts = _query_last_event(conn, args.kind)
        if last_ts:
            age = (_now_utc() - last_ts).total_seconds()
            print(f"last_event_ts={last_ts.isoformat()} age={_fmt_age_s(age)} kind={args.kind}")
        else:
            print(f"last_event_ts=NA kind={args.kind}")

        if args.reason and args.sample > 0:
            samples = _query_samples(conn, args.reason, args.kind, args.sample)
            for s in samples:
                ts = s.get("ts")
                sym = s.get("symbol") or "na"
                stream = s.get("stream")
                dlq_id = s.get("dlq_id")
                dq = s.get("dq_code")
                err = s.get("err")
                mf = s.get("missing_fields")
                print(f"sample ts={ts} sym={sym} stream={stream} id={dlq_id} dq={dq} missing={mf} err={err}")
            return

        top_reasons = _query_top_reasons(conn, args.lookback_h, args.kind, args.top)
        lines: List[str] = []
        for reason, n, last in top_reasons:
            age = (_now_utc() - last).total_seconds() if last else 0.0
            lines.append(f"{reason}: n={n} last_age={_fmt_age_s(age)}")

        if lines:
            print("top_reasons:")
            for ln in lines:
                print("  - " + ln)

        if args.notify:
            msg = "OF_INPUTS_DLQ_DB (" + args.kind + ")\n" + "\n".join(lines[:5])
            _notify(msg, severity="crit")


if __name__ == "__main__":
    main()
