from __future__ import annotations
"""OFInputs DLQ + quarantine streams -> Postgres/Timescale archiver (P98).

Goal
- Keep a durable history of OFInputs DLQ/quarantine events for postmortems.
- Designed to sit *on top* of the existing DLQ/quarantine contour (P96/P97):
  - stream:dlq:of_inputs
  - quarantine:signals:of:inputs

Design
- Reads Redis streams (XRANGE) from a checkpoint stored in Redis.
- Inserts into DB with idempotency (PRIMARY KEY (stream, dlq_id)).
- Updates a small status hash in Redis for Prometheus exporter:
    metrics:of_inputs_dlq_db_archive
    metrics:of_inputs_quarantine_db_archive

Usage
  TRADES_DB_DSN=... REDIS_URL=... \
    python -m orderflow_services.of_inputs_dlq_archive_to_db_p98 --once

  # Continuous loop
  python -m orderflow_services.of_inputs_dlq_archive_to_db_p98 --loop

  # Backfill last N entries without updating checkpoint
  python -m orderflow_services.of_inputs_dlq_archive_to_db_p98 --tail 200000 --no-checkpoint

ENV
  REDIS_URL / REDIS_TICKS_URL
  TRADES_DB_DSN / ARCHIVER_PG_DSN / DATABASE_URL / PG_DSN
  OF_INPUTS_DLQ_DB_ARCHIVE_STREAMS (comma-separated) default:
    stream:dlq:of_inputs,quarantine:signals:of:inputs
  OF_INPUTS_DLQ_DB_ARCHIVE_BATCH (default 5000)
  OF_INPUTS_DLQ_DB_ARCHIVE_DELETE_AFTER_INSERT (default 0)
  OF_INPUTS_DLQ_DB_ARCHIVE_AUTO_MIGRATE (default 0)

Rollback
  - stop the job; no runtime impact.
  - table is append-only.
""",
from utils.time_utils import get_ny_time_millis

import argparse
import datetime as dt
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

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
        or os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or os.getenv("DATABASE_URL")
        or os.getenv("PG_DSN")
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


def coalesce_ts_ms(payload: Dict[str, Any], stream_id: str) -> int:
    for k in ("ts_ms", "tick_ts", "ts_event_ms", "ts", "timestamp_ms"):
        v = payload.get(k)
        try:
            if v is not None:
                return int(float(v))
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
        ddl = """,
        CREATE TABLE IF NOT EXISTS of_inputs_dlq_events (
          stream TEXT NOT NULL,
          dlq_id TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          ts TIMESTAMPTZ NOT NULL,
          src_stream TEXT,
          src_stream_id TEXT,
          err TEXT,
          dq_code TEXT,
          attempt_version INT,
          published_version INT,
          missing_fields TEXT,
          payload_json JSONB,
          inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (stream, dlq_id)
        );
        CREATE INDEX IF NOT EXISTS of_inputs_dlq_events_ts_idx ON of_inputs_dlq_events (ts DESC);
        CREATE INDEX IF NOT EXISTS of_inputs_dlq_events_dq_idx ON of_inputs_dlq_events (dq_code, ts DESC);
        CREATE INDEX IF NOT EXISTS of_inputs_dlq_events_src_idx ON of_inputs_dlq_events (src_stream, ts DESC);
        """,
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
                try:
                    cur.execute("SELECT create_hypertable('of_inputs_dlq_events','ts', if_not_exists => TRUE);")
                except Exception:
                    conn.rollback()
            conn.commit()

    def insert_rows(self, rows: List[Tuple[Any, ...]]) -> int:
        if not rows:
            return 0
        sql = """

        INSERT INTO of_inputs_dlq_events (
          stream, dlq_id, ts_ms, ts,
          src_stream, src_stream_id, err,
          dq_code, attempt_version, published_version, missing_fields,
          payload_json
        ) VALUES %s
        ON CONFLICT (stream, dlq_id) DO NOTHING,
        """,
        with self._conn() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=5000)
            conn.commit()
        return len(rows)


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "replace")
        return int(float(x))
    except Exception:
        return default


def _s(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "replace")
        except Exception:
            return ""
    return str(x)


def _as_payload_guess(fields: Dict[str, Any]) -> Dict[str, Any]:
    """If no explicit 'payload' field exists, treat the remaining fields as payload.""",
    drop = {
        "err",
        "error",
        "stream",
        "src_stream",
        "stream_id",
        "src_stream_id",
        "data",
        "payload",
    }
    out: Dict[str, Any] = {}
    for k, v in (fields or {}).items():
        ks = _s(k)
        if ks in drop:
            continue
        out[ks] = _decode(v)
    return out


def parse_event(stream: str, dlq_id: str, fields: Dict[str, Any]) -> Tuple[Tuple[Any, ...], str]:
    """Parse a Redis stream entry into a DB row + last_stream_id for checkpoint.""",
    f = {str(_decode(k)): _decode(v) for k, v in (fields or {}).items()}

    src_stream = str(f.get("stream") or f.get("src_stream") or "")
    src_stream_id = str(f.get("stream_id") or f.get("src_stream_id") or "")
    err = str(f.get("err") or f.get("error") or "")

    payload_raw = f.get("payload")
    if payload_raw is None:
        payload_raw = f.get("data")

    payload_any = _json_loads_maybe(payload_raw)
    if payload_any is None:
        payload = _as_payload_guess(f)
    elif isinstance(payload_any, dict):
        payload = payload_any
    else:
        payload = {"payload": payload_any}

    ts_ms = coalesce_ts_ms(payload, dlq_id)
    ts = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.timezone.utc)

    dq_code = payload.get("dq_code") or payload.get("why") or f.get("dq_code") or f.get("why")
    attempt_version = payload.get("attempt_version") or payload.get("attempt_v") or f.get("attempt_version")
    published_version = payload.get("published_version") or payload.get("published_v") or f.get("published_version")
    missing_fields = payload.get("missing_fields") or payload.get("missing") or f.get("missing_fields")

    # stringify missing_fields consistently
    mf_s = ""
    try:
        if isinstance(missing_fields, (list, tuple)):
            mf_s = ",".join([str(x) for x in missing_fields])
        elif missing_fields is None:
            mf_s = ""
        else:
            mf_s = str(missing_fields)
    except Exception:
        mf_s = ""

    row = (
        str(stream),
        str(dlq_id),
        int(ts_ms),
        ts,
        src_stream or None,
        src_stream_id or None,
        err or None,
        str(dq_code) if dq_code is not None else None,
        int(float(attempt_version)) if attempt_version is not None and str(attempt_version).strip() != "" else None,
        int(float(published_version)) if published_version is not None and str(published_version).strip() != "" else None,
        mf_s or None,
        json.dumps(payload, ensure_ascii=False),
    )
    return row, str(dlq_id)


def _checkpoint_key(stream: str) -> str:
    return f"cfg:of_inputs_dlq_db_archive:last_id:{stream}"


def _metrics_key(stream: str) -> str:
    # Deterministic mapping: dlq stream -> dlq metrics; quarantine stream -> quarantine metrics
    if "quarantine" in stream:
        return "metrics:of_inputs_quarantine_db_archive"
    return "metrics:of_inputs_dlq_db_archive"


def _write_metrics(r, key: str, last_stream_id: str, inserted_delta: int, error_delta: int) -> None:
    now_ms = get_ny_time_millis()
    try:
        pipe = r.pipeline()
        pipe.hset(key, mapping={
            "last_run_ts_ms": str(now_ms),
            "last_stream_id": str(last_stream_id),
        })
        if inserted_delta:
            pipe.hincrby(key, "inserted_total", int(inserted_delta))
        if error_delta:
            pipe.hincrby(key, "error_total", int(error_delta))
        pipe.expire(key, 14 * 24 * 3600)
        pipe.execute()
    except Exception:
        pass


def read_batch(r, stream: str, start_id: str, count: int) -> List[Tuple[str, Dict[str, Any]]]:
    items = r.xrange(stream, min=start_id, max="+", count=count)
    out: List[Tuple[str, Dict[str, Any]]] = []
    for mid, fields in items:
        out.append((str(_decode(mid)), fields))
    return out


def run_once(args: argparse.Namespace) -> int:
    dsn = pick_dsn()
    if not dsn:
        raise SystemExit("missing_db_dsn: set TRADES_DB_DSN/ARCHIVER_PG_DSN")

    r = _connect_redis()
    pg = PgWriter(PgCfg(dsn=dsn))

    auto_migrate = bool(args.auto_migrate) or env_bool("OF_INPUTS_DLQ_DB_ARCHIVE_AUTO_MIGRATE", False)
    if auto_migrate:
        pg.ensure_tables()

    delete_after = bool(args.delete_after) or env_bool("OF_INPUTS_DLQ_DB_ARCHIVE_DELETE_AFTER_INSERT", False)

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

        # tail mode: start from last N entries
        if args.tail and int(args.tail) > 0:
            try:
                items = r.xrevrange(stream, max="+", min="-", count=int(args.tail))
                ids = [str(_decode(mid)) for mid, _ in items]
                if ids:
                    start_id = ids[-1]
            except Exception:
                pass

        last_id = start_id
        inserted_total_stream = 0
        errors_total_stream = 0

        while True:
            batch = read_batch(r, stream, last_id, count=args.batch)
            if not batch:
                break

            # XRANGE is inclusive; skip the first if it's the checkpoint id
            if last_id != "-" and batch and batch[0][0] == last_id:
                batch = batch[1:]

            rows: List[Tuple[Any, ...]] = []
            last_seen = last_id
            for mid, fields in batch:
                last_seen = mid
                try:
                    row, _ = parse_event(stream, mid, fields)
                    rows.append(row)
                except Exception:
                    errors_total_stream += 1
                    continue

            try:
                inserted = pg.insert_rows(rows)
                inserted_total_stream += inserted
                total_inserted += inserted
            except Exception:
                errors_total_stream += 1
                # fail-open: continue; don't advance checkpoint on DB errors
                break

            last_id = last_seen

            if args.max_batches and args.max_batches > 0:
                args.max_batches -= 1
                if args.max_batches <= 0:
                    break

        # Update checkpoint and metrics
        if not args.no_checkpoint and last_id and last_id != start_id:
            try:
                r.set(_checkpoint_key(stream), str(last_id))
            except Exception:
                pass

        # optional delete-after-insert (dangerous): trim everything <= last_id
        if delete_after and last_id and last_id != "-":
            try:
                r.xtrim(stream, minid=last_id, approximate=False)
            except Exception:
                pass

        _write_metrics(r, _metrics_key(stream), last_id or "-", inserted_total_stream, errors_total_stream)

    return total_inserted


def main() -> None:
    default_streams = env(
        "OF_INPUTS_DLQ_DB_ARCHIVE_STREAMS",
        "stream:dlq:of_inputs,quarantine:signals:of:inputs",
    )

    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="run one pass")
    ap.add_argument("--loop", action="store_true", help="run forever")
    ap.add_argument("--batch", type=int, default=env_int("OF_INPUTS_DLQ_DB_ARCHIVE_BATCH", 5000))
    ap.add_argument("--streams", type=str, default=default_streams)
    ap.add_argument("--tail", type=int, default=0)
    ap.add_argument("--no-checkpoint", action="store_true")
    ap.add_argument("--auto-migrate", action="store_true")
    ap.add_argument("--delete-after", action="store_true")
    ap.add_argument("--sleep", type=float, default=10.0)
    ap.add_argument("--max-batches", type=int, default=0)

    args = ap.parse_args()
    args.streams = [s.strip() for s in str(args.streams).split(",") if s.strip()]

    if not args.once and not args.loop:
        args.once = True

    if args.loop:
        while True:
            try:
                n = run_once(args)
                print(f"of_inputs_dlq_archive_to_db_p98 inserted={n}")
            except Exception as e:
                print(f"of_inputs_dlq_archive_to_db_p98 error: {e}")
            time.sleep(float(args.sleep))
    else:
        n = run_once(args)
        print(f"of_inputs_dlq_archive_to_db_p98 inserted={n}")


if __name__ == "__main__":
    main()
