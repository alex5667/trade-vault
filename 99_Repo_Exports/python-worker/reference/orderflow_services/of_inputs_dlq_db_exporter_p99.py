#!/usr/bin/env python3
from __future__ import annotations
"""Prometheus exporter: OFInputs DLQ DB rollups (P99).

Exports *low-cardinality* gauges derived from Timescale/Postgres table `of_inputs_dlq_events`:
  - of_inputs_dlq_db_events_lookback_total{kind,reason}
  - of_inputs_dlq_db_last_event_ts_ms{kind}
  - of_inputs_dlq_db_last_event_age_sec{kind}

`reason` is bucketed by an allowlist to avoid label explosion:
  - if reason in allowlist => keep
  - else => 'other'

Run:
  TRADES_DB_DSN=... python -m orderflow_services.of_inputs_dlq_db_exporter_p99

ENV:
  TRADES_DB_DSN / ARCHIVER_PG_DSN / DATABASE_URL / PG_DSN
  OF_INPUTS_DLQ_DB_EXPORTER_PORT (default 9157)
  OF_INPUTS_DLQ_DB_EXPORTER_LOOKBACK_H (default 24)
  OF_INPUTS_DLQ_DB_REASON_ALLOWLIST (comma-separated; default set in code)
"""


import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import psycopg2
from prometheus_client import Gauge, start_http_server  # type: ignore


GAUGE_EVENTS = Gauge(
    "of_inputs_dlq_db_events_lookback_total",
    "Count of of_inputs_dlq_events in lookback window (gauge)",
    ["kind", "reason"],
)
GAUGE_LAST_TS_MS = Gauge(
    "of_inputs_dlq_db_last_event_ts_ms",
    "Last event timestamp (ms since epoch) observed in of_inputs_dlq_events",
    ["kind"],
)
GAUGE_LAST_AGE_S = Gauge(
    "of_inputs_dlq_db_last_event_age_sec",
    "Age (seconds) since last event observed in of_inputs_dlq_events",
    ["kind"],
)


DEFAULT_ALLOWLIST = [
    # data-quality / contour codes (keep stable)
    "missing_lob_fields",
    "book_state_degraded",
    "book_state_bad",
    "v3_to_v2_downgrade",
    "bad_ts_ms",
    "bad_time",
    "bad_schema_version",
    "missing_legs",
    "missing_fields",
    # common error prefixes
    "ValueError",
    "KeyError",
    "TypeError",
    "redis",
    "publish",
    "unknown",
]


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


def _to_ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


def _allowlist() -> List[str]:
    raw = os.getenv("OF_INPUTS_DLQ_DB_REASON_ALLOWLIST", "").strip()
    if not raw:
        return DEFAULT_ALLOWLIST
    out = [x.strip() for x in raw.split(",") if x.strip()]
    return out or DEFAULT_ALLOWLIST


class Exporter:
    def __init__(self) -> None:
        self.dsn = _pick_dsn()
        if not self.dsn:
            raise SystemExit("missing_db_dsn: set TRADES_DB_DSN/ARCHIVER_PG_DSN")
        self.port = int(os.getenv("OF_INPUTS_DLQ_DB_EXPORTER_PORT", "9157") or 9157)
        self.lookback_h = int(os.getenv("OF_INPUTS_DLQ_DB_EXPORTER_LOOKBACK_H", "24") or 24)

    def _conn(self):
        return psycopg2.connect(self.dsn)

    def _has_view(self, conn, name: str) -> bool:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (name,))
            row = cur.fetchone()
            return bool(row and row[0])

    def _query(self) -> Tuple[Dict[Tuple[str, str], int], Dict[str, datetime]]:
        allow = _allowlist()
        counts: Dict[Tuple[str, str], int] = {}
        last_ts_by_kind: Dict[str, datetime] = {}

        with self._conn() as conn:
            use_view = self._has_view(conn, "public.v_of_inputs_dlq_events_parsed")
            with conn.cursor() as cur:
                if use_view:
                    sql = """
                    WITH base AS (
                      SELECT
                        kind,
                        CASE WHEN reason = ANY(%s::text[]) THEN reason ELSE 'other' END AS reason2,
                        ts
                      FROM v_of_inputs_dlq_events_parsed
                      WHERE ts >= now() - (%s || ' hours')::interval
                    )
                    SELECT kind, reason2, COUNT(*)::bigint AS n, MAX(ts) AS last_ts
                    FROM base
                    GROUP BY 1,2
                    """
                    cur.execute(sql, (allow, int(self.lookback_h)))
                else:
                    sql = """
                    WITH parsed AS (
                      SELECT
                        ts
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
                    ), bucketed AS (
                      SELECT
                        kind,
                        CASE WHEN reason = ANY(%s::text[]) THEN reason ELSE 'other' END AS reason2,
                        ts
                      FROM parsed
                    )
                    SELECT kind, reason2, COUNT(*)::bigint AS n, MAX(ts) AS last_ts
                    FROM bucketed
                    GROUP BY 1,2
                    """
                    cur.execute(sql, (int(self.lookback_h), allow))

                rows = cur.fetchall() or []

        for kind, reason2, n, last_ts in rows:
            k = str(kind)
            r = str(reason2)
            counts[(k, r)] = int(n)
            if last_ts is not None:
                # last_ts in this bucket; take max across reasons
                prev = last_ts_by_kind.get(k)
                if prev is None or last_ts > prev:
                    last_ts_by_kind[k] = last_ts

        # ensure stable 0 series for allowlisted reasons
        for kind in ("dlq", "quarantine", "other"):
            for r in allow:
                counts.setdefault((kind, r), 0)
            counts.setdefault((kind, "other"), counts.get((kind, "other"), 0))

        return counts, last_ts_by_kind

    def tick(self) -> None:
        counts, last_ts_by_kind = self._query()

        for (kind, reason), n in counts.items():
            GAUGE_EVENTS.labels(kind=kind, reason=reason).set(n)

        now = _now_utc()
        for kind in ("dlq", "quarantine", "other"):
            ts = last_ts_by_kind.get(kind)
            if ts is None:
                GAUGE_LAST_TS_MS.labels(kind=kind).set(0)
                GAUGE_LAST_AGE_S.labels(kind=kind).set(0)
            else:
                GAUGE_LAST_TS_MS.labels(kind=kind).set(_to_ms(ts))
                GAUGE_LAST_AGE_S.labels(kind=kind).set(max(0.0, (now - ts).total_seconds()))


def main() -> None:
    ex = Exporter()
    start_http_server(ex.port)
    print(f"of_inputs_dlq_db_exporter_p99 serving on :{ex.port}")
    while True:
        try:
            ex.tick()
        except Exception:
            # fail-open
            pass
        time.sleep(15)


if __name__ == "__main__":
    main()
