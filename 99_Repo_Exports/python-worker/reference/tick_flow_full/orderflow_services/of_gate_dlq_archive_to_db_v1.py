from __future__ import annotations

"""OF-Gate DLQ -> PostgreSQL/Timescale archiver (P83 optional).

Goal
- Keep a durable history of DLQ entries (both 'metrics' and 'quarantine' DLQs) for postmortems.
- Does NOT delete from DLQ by default.

Design
- Reads Redis streams (XRANGE) from a checkpoint stored in Redis.
- Inserts into DB with idempotency (PRIMARY KEY (stream, dlq_id)).
- Optional delete-after-insert (dangerous; off by default).

Usage
  TRADES_DB_DSN=... REDIS_URL=... \
    python -m orderflow_services.of_gate_dlq_archive_to_db_v1 --once

  # Keep running (daemon mode)
  python -m orderflow_services.of_gate_dlq_archive_to_db_v1 --loop

  # Backfill last N entries without updating checkpoint
  python -m orderflow_services.of_gate_dlq_archive_to_db_v1 --tail 200000 --no-checkpoint
"""


import argparse
import datetime as dt
import json
import os
import time
from dataclasses import dataclass
from typing import Any

import psycopg2
from psycopg2.extras import execute_values


def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v else default


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v else default
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def pick_dsn() -> str:
    return (
        os.getenv("ARCHIVER_PG_DSN")
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))
        or ""
    )


def _decode(x: Any) -> Any:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "ignore")
    return x


def _json_loads_maybe(s: Any) -> Any:
    if s is None:
        return None
    s = _decode(s)
    if isinstance(s, (dict, list)):
        return s
    if not isinstance(s, str):
        return s
    try:
        return json.loads(s)
    except Exception:
        return s


def ts_ms_from_stream_id(stream_id: str) -> int:
    return int(str(stream_id).split("-", 1)[0])


def coalesce_ts_ms(payload: dict[str, Any], stream_id: str) -> int:
    for k in ("ts_ms", "ts_event_ms", "ts", "timestamp_ms"):
        v = payload.get(k)
        try:
            if v is not None:
                return int(v)
        except Exception:
            pass
    return ts_ms_from_stream_id(stream_id)


def _connect_redis():
    import redis  # type: ignore

    url = os.getenv("REDIS_URL") or os.getenv("REDIS_TICKS_URL") or "redis://localhost:6379/0"
    return redis.Redis.from_url(url, decode_responses=False)


@dataclass
class PgCfg:
    dsn: str


class PgWriter:
    def __init__(self, cfg: PgCfg):
        self.cfg = cfg

    def _conn(self):
        return psycopg2.connect(self.cfg.dsn)

    def ensure_tables(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS of_gate_dlq_events (
          stream TEXT NOT NULL,
          dlq_id TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          ts TIMESTAMPTZ NOT NULL,
          src_stream TEXT,
          src_stream_id TEXT,
          err TEXT,
          dq_code TEXT,
          reason_code TEXT,
          schema_version INT,
          payload_json JSONB,
          inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (stream, dlq_id)
        );
        CREATE INDEX IF NOT EXISTS of_gate_dlq_events_ts_idx ON of_gate_dlq_events (ts DESC);
        CREATE INDEX IF NOT EXISTS of_gate_dlq_events_dq_idx ON of_gate_dlq_events (dq_code, ts DESC);
        CREATE INDEX IF NOT EXISTS of_gate_dlq_events_reason_idx ON of_gate_dlq_events (reason_code, ts DESC);
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
                try:
                    cur.execute("SELECT create_hypertable('of_gate_dlq_events','ts', if_not_exists => TRUE);")
                except Exception:
                    conn.rollback()
            conn.commit()

    def insert_rows(self, rows: list[tuple[Any, ...]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO of_gate_dlq_events (
          stream, dlq_id, ts_ms, ts,
          src_stream, src_stream_id, err,
          dq_code, reason_code, schema_version,
          payload_json,
        ) VALUES %s
        ON CONFLICT (stream, dlq_id) DO NOTHING
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=5000)
            conn.commit()
        return len(rows)


def parse_dlq_fields(dlq_id: str, fields: dict[str, Any]) -> tuple[str, str, str, str, str, str, str, str | None, str | None, int | None, Any, int]:
    f = {str(_decode(k)): _decode(v) for k, v in (fields or {}).items()}
    src_stream = str(f.get("stream") or f.get("src_stream") or "")
    src_stream_id = str(f.get("stream_id") or f.get("src_stream_id") or "")
    err = str(f.get("err") or f.get("error") or "")
    payload_raw = f.get("payload")
    if payload_raw is None:
        payload_raw = f.get("data")
    payload = _json_loads_maybe(payload_raw)

    payload_dict: dict[str, Any] = payload if isinstance(payload, dict) else {}
    ts_ms = coalesce_ts_ms(payload_dict, dlq_id)

    dq_code = payload_dict.get("dq_code") or payload_dict.get("why")
    reason_code = payload_dict.get("reason_code")
    schema_version = payload_dict.get("schema_version")
    try:
        schema_version_i = int(schema_version) if schema_version is not None else None
    except Exception:
        schema_version_i = None

    return (
        src_stream,
        src_stream_id,
        err,
        str(dq_code) if dq_code is not None else None,
        str(reason_code) if reason_code is not None else None,
        schema_version_i,
        payload,
        ts_ms,
    )


def _checkpoint_key(stream: str) -> str:
    return f"cfg:of_gate_dlq_db_archive:last_id:{stream}"


def read_batch(r, stream: str, start_id: str, count: int) -> list[tuple[str, dict[str, Any]]]:
    # XRANGE is inclusive; we use start_id from checkpoint, then skip first if equals
    items = r.xrange(stream, min=start_id, max="+", count=count)
    out: list[tuple[str, dict[str, Any]]] = []
    for mid, fields in items:
        out.append((str(_decode(mid)), fields))
    return out


def run_once(args: argparse.Namespace) -> int:
    dsn = pick_dsn()
    if not dsn:
        raise SystemExit("missing_db_dsn: set TRADES_DB_DSN/ARCHIVER_PG_DSN")

    r = _connect_redis()
    pg = PgWriter(PgCfg(dsn=dsn))
    if args.auto_migrate:
        pg.ensure_tables()

    total_inserted = 0
    for stream in args.streams:
        stream = stream.strip()
        if not stream:
            continue

        if args.no_checkpoint:
            start_id = "-"
        else:
            start_id_raw = r.get(_checkpoint_key(stream))
            start_id = str(_decode(start_id_raw)) if start_id_raw else "-"

        # tail mode: start from last N entries by using XREVRANGE + reverse
        if args.tail and int(args.tail) > 0:
            tail_n = int(args.tail)
            rev = r.xrevrange(stream, max="+", min="-", count=tail_n)
            items = [(str(_decode(mid)), fields) for mid, fields in reversed(rev)]
        else:
            items = read_batch(r, stream, start_id, args.batch)

        if not items:
            continue

        rows: list[tuple[Any, ...]] = []
        last_id = None
        for dlq_id, fields in items:
            last_id = dlq_id
            src_stream, src_stream_id, err, dq_code, reason_code, schema_version_i, payload, ts_ms = parse_dlq_fields(dlq_id, fields)
            ts = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.UTC)
            payload_json = json.dumps(payload, ensure_ascii=False) if payload is not None else None
            rows.append(
                (
                    stream,
                    dlq_id,
                    int(ts_ms),
                    ts,
                    src_stream or None,
                    src_stream_id or None,
                    err or None,
                    dq_code,
                    reason_code,
                    schema_version_i,
                    payload_json,
                )
            )

        inserted = pg.insert_rows(rows)
        total_inserted += inserted

        if args.delete_after and args.yes:
            r.xdel(stream, *[dlq_id for dlq_id, _ in items])

        if (not args.no_checkpoint) and last_id is not None and (args.tail == 0):
            # checkpoint only for forward scan (not tail mode)
            r.set(_checkpoint_key(stream), last_id)

    print(json.dumps({"inserted": total_inserted, "streams": args.streams}, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="of_gate_dlq_archive_to_db_v1")
    p.add_argument(
        "--streams",
        default=env("OF_GATE_DLQ_STREAMS", "stream:dlq:of_gate_metrics,stream:dlq:of_gate_quarantine"),
        help="comma-separated DLQ streams",
    )
    p.add_argument("--batch", type=int, default=env_int("OF_GATE_DLQ_DB_ARCHIVE_BATCH", 5000))
    p.add_argument("--auto-migrate", action="store_true", default=env_bool("OF_GATE_DLQ_DB_ARCHIVE_AUTO_MIGRATE", True))
    p.add_argument("--tail", type=int, default=0, help="archive last N items only (no checkpoint)")
    p.add_argument("--no-checkpoint", action="store_true", default=False)
    p.add_argument("--delete-after", action="store_true", default=env_bool("OF_GATE_DLQ_DB_ARCHIVE_DELETE_AFTER", False))
    p.add_argument("--yes", action="store_true", default=False, help="required together with --delete-after")
    p.add_argument("--loop", action="store_true", default=False)
    p.add_argument("--interval-s", type=int, default=env_int("OF_GATE_DLQ_DB_ARCHIVE_INTERVAL_S", 60))
    p.add_argument("--once", action="store_true", default=False)
    return p


def main(argv: list[str] | None = None) -> int:
    ap = build_parser()
    args = ap.parse_args(argv)

    args.streams = [s.strip() for s in str(args.streams).split(",") if s.strip()]

    if args.delete_after and not args.yes:
        raise SystemExit("refusing: --delete-after requires --yes")

    if args.once:
        return run_once(args)

    if args.loop:
        while True:
            run_once(args)
            time.sleep(max(1, int(args.interval_s)))

    # default: run once
    return run_once(args)


if __name__ == "__main__":
    raise SystemExit(main())
