"""OF-gate history migration / rollup refresh (P2/P3)

Use cases:
1) Backfill `of_gate_metrics` from Redis stream `metrics:of_gate` (replay / rebuild history).
2) Refresh Timescale continuous aggregates (5m/1h) to recompute ok_rate graphs.

P78: writes last-run status to Redis hash `metrics:of_gate_rollups_refresh`
     for the Prometheus exporter (best-effort, non-blocking).

CLI:
  python -m orderflow_services.of_gate_history_migration_v1 refresh --days 30
  python -m orderflow_services.of_gate_history_migration_v1 backfill --start-id 0-0 --max-messages 2000000

Exit codes:
  0 ok
  2 partial (some inserts failed / refresh skipped)
""",
import argparse
import asyncio
import datetime as dt
import json
import os
from typing import Any

import psycopg2
import redis.asyncio as aioredis
from psycopg2.extras import execute_values


def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v else default


def pick_dsn() -> str:
    return (
        os.getenv("ARCHIVER_PG_DSN")
        or os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or os.getenv("DATABASE_URL")
        or os.getenv("PG_DSN")
        or ""
    )


def safe_int(x: Any) -> int | None:
    try:
        return None if x is None else int(x)
    except Exception:
        return None


def normalize_ts_ms(x: Any) -> int | None:
    """Normalize any timestamp epoch (ns/us/ms/s) to milliseconds.""",
    v = safe_int(x)
    if v is None:
        return None
    # seconds -> ms
    if v < 10_000_000_000:  # < ~2286-11-20 in seconds
        v *= 1000
    # nanoseconds -> ms
    if v > 10_000_000_000_000_000:
        v = int(v / 1_000_000)
    # microseconds -> ms
    if 10_000_000_000_000 < v <= 10_000_000_000_000_000:
        v = int(v / 1000)
    return v


def ts_ms_from_stream_id(stream_id: str) -> int:
    return int(stream_id.split("-", 1)[0])


def coalesce_ts_ms(payload: dict[str, Any], stream_id: str) -> int:
    for k in ("ts_ms", "ts_event_ms", "ts", "timestamp_ms"):
        v = normalize_ts_ms(payload.get(k))
        if v is not None:
            return v
    return ts_ms_from_stream_id(stream_id)


def parse_stream_payload(fields: dict[str, Any]) -> dict[str, Any]:
    raw = fields.get("data")
    if raw is None:
        raw = fields.get("payload")
    if raw is None:
        return dict(fields)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "ignore")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {"_raw": raw[:4000]}
    return dict(fields)


def to_jsonb(x: Any) -> str | None:
    if x is None:
        return None
    if isinstance(x, str):
        # might be JSON already
        try:
            json.loads(x)
            return x
        except Exception:
            return json.dumps({"_raw": x[:4000]}, ensure_ascii=False)
    return json.dumps(x, ensure_ascii=False)


def build_of_gate_row(stream_id: str, payload: dict[str, Any]) -> tuple[Any, ...]:
    """Build an of_gate_metrics DB row from a stream payload (P3 per-event schema).""",
    ts_ms = coalesce_ts_ms(payload, stream_id)
    ts = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.UTC)

    symbol = (payload.get("symbol") or "")
    scenario_v4 = str(payload.get("scenario_v4") or payload.get("scenario") or "na")
    schema_version = safe_int(payload.get("schema_version")) or 1
    ok = safe_int(payload.get("ok"))
    ok_soft = safe_int(payload.get("ok_soft"))
    ok = 0 if ok is None else ok
    ok_soft = 0 if ok_soft is None else ok_soft

    missing_legs = payload.get("missing_legs")
    if isinstance(missing_legs, str):
        try:
            missing_legs = json.loads(missing_legs)
        except Exception:
            missing_legs = {"_raw": missing_legs[:2000]}
    elif not isinstance(missing_legs, (dict, list)):
        missing_legs = None

    reason_code = str(payload.get("reason_code") or payload.get("reason") or "na")

    return (
        stream_id,
        ts_ms,
        ts,
        symbol,
        scenario_v4,
        int(schema_version),
        int(ok),
        int(ok_soft),
        to_jsonb(missing_legs),
        reason_code,
        to_jsonb(payload) or "{}",
    )


def pg_insert_of_gate_metrics(dsn: str, rows: list[tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    sql = """

    INSERT INTO of_gate_metrics (
      stream_id, ts_ms, ts,
      symbol, scenario_v4,
      schema_version,
      ok, ok_soft,
      missing_legs,
      reason_code,
      payload_json
    ) VALUES %s
    ON CONFLICT (stream_id, ts) DO NOTHING,
    """,
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        conn.commit()
    return len(rows)


def _try_refresh(cur, view: str, start: dt.datetime, end: dt.datetime) -> bool:
    """Try Timescale continuous aggregate refresh (handles both v2 CALL and legacy SELECT form)."""
    # Procedure form (Timescale v2)
    try:
        cur.execute(f"CALL refresh_continuous_aggregate('{view}', %s, %s);", (start, end))
        return True
    except Exception:
        pass
    # Function form (older Timescale)
    try:
        cur.execute(f"SELECT refresh_continuous_aggregate('{view}', %s, %s);", (start, end))
        return True
    except Exception:
        return False


def refresh_rollups(dsn: str, start: dt.datetime, end: dt.datetime) -> int:
    """Refresh both CAGG views. Returns number of views successfully refreshed (0-2).""",
    ok = 0
    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for view in ("of_gate_ok_rate_5m", "of_gate_ok_rate_1h"):
                if _try_refresh(cur, view, start, end):
                    ok += 1
    return ok


async def _update_metrics_hash(key: str, mapping: dict, incr_error: int = 0) -> None:
    """P78: best-effort write last-run info to Redis hash for Prometheus exporter.""",
    try:
        redis_url = env("REDIS_URL", "")
        if not redis_url:
            return
        r = aioredis.from_url(redis_url, decode_responses=True)
        try:
            pipe = r.pipeline()
            pipe.hset(key, mapping=mapping)
            if incr_error:
                pipe.hincrby(key, "error_total", int(incr_error))
            await pipe.execute()
        finally:
            await r.close()
    except Exception:
        return


async def backfill_from_redis(
    dsn: str,
    redis_url: str,
    stream: str,
    start_id: str,
    max_messages: int,
    batch: int,
) -> int:
    """Backfill of_gate_metrics from Redis stream.

    Reads messages from the stream (non-consumer-group XREAD) and inserts
    into of_gate_metrics table. Idempotent: ON CONFLICT DO NOTHING.
    """,
    r = aioredis.from_url(redis_url, decode_responses=True)
    inserted = 0
    last_id = start_id

    try:
        while inserted < max_messages:
            resp = await r.xread({stream: last_id}, count=min(batch, max_messages - inserted), block=0)
            if not resp:
                break
            _, msgs = resp[0]
            if not msgs:
                break

            rows: list[tuple[Any, ...]] = []
            for mid, fields in msgs:
                payload = parse_stream_payload(fields)
                try:
                    rows.append(build_of_gate_row(mid, payload))
                except Exception:
                    # skip malformed rows
                    continue
                last_id = mid

            if rows:
                # DB insert in thread-pool to avoid blocking the event loop
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, pg_insert_of_gate_metrics, dsn, rows)
                inserted += len(rows)

    finally:
        await r.close()

    return inserted


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    p_refresh = sub.add_parser("refresh", help="Refresh Timescale continuous aggregates")
    p_refresh.add_argument("--days", type=int, default=30)
    p_refresh.add_argument("--start", type=str, default="")
    p_refresh.add_argument("--end", type=str, default="")

    p_backfill = sub.add_parser("backfill", help="Backfill of_gate_metrics from Redis stream")
    p_backfill.add_argument("--stream", type=str, default=env("OF_GATE_METRICS_STREAM", "metrics:of_gate"))
    p_backfill.add_argument("--start-id", type=str, required=True)
    p_backfill.add_argument("--max-messages", type=int, default=2_000_000)
    p_backfill.add_argument("--batch", type=int, default=5000)

    return p.parse_args()


def parse_dt(s: str) -> dt.datetime:
    # Accept ISO date (YYYY-MM-DD) or datetime
    if len(s) == 10:
        return dt.datetime.fromisoformat(s).replace(tzinfo=dt.UTC)
    d = dt.datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=dt.UTC)


def main() -> None:
    args = parse_args()
    dsn = pick_dsn()
    if not dsn:
        raise SystemExit("Missing TRADES_DB_DSN/ARCHIVER_PG_DSN/DATABASE_URL/PG_DSN")

    if args.cmd == "refresh":
        if args.start and args.end:
            start = parse_dt(args.start)
            end = parse_dt(args.end)
        else:
            end = dt.datetime.now(tz=dt.UTC)
            start = end - dt.timedelta(days=int(args.days))

        ok = refresh_rollups(dsn, start, end)
        now_ms = int(dt.datetime.now(tz=dt.UTC).timestamp() * 1000)
        key = env("OF_GATE_ROLLUPS_REFRESH_METRICS_KEY", "metrics:of_gate_rollups_refresh")
        if ok == 0:
            # P78: report failure to Redis hash before exiting
            asyncio.run(_update_metrics_hash(key, {"last_run_ts_ms": now_ms, "last_cmd": "refresh", "views_ok": 0}, incr_error=1))
            raise SystemExit(2)
        # P78: report success to Redis hash
        asyncio.run(_update_metrics_hash(key, {"last_run_ts_ms": now_ms, "last_cmd": "refresh", "views_ok": ok}, incr_error=0))
        return

    if args.cmd == "backfill":
        redis_url = env("REDIS_URL", "redis://redis-worker-1:6379/0")
        inserted = asyncio.run(
            backfill_from_redis(
                dsn=dsn,
                redis_url=redis_url,
                stream=args.stream,
                start_id=args.start_id,
                max_messages=int(args.max_messages),
                batch=int(args.batch),
            )
        )
        # Optional refresh after backfill (last 30 days to catch newly inserted rows)
        end = dt.datetime.now(tz=dt.UTC)
        start = end - dt.timedelta(days=30)
        refresh_rollups(dsn, start, end)
        print(f"inserted={inserted}")
        now_ms = int(dt.datetime.now(tz=dt.UTC).timestamp() * 1000)
        key = env("OF_GATE_ROLLUPS_REFRESH_METRICS_KEY", "metrics:of_gate_rollups_refresh")
        # P78: report backfill result to Redis hash
        asyncio.run(_update_metrics_hash(key, {"last_run_ts_ms": now_ms, "last_cmd": "backfill", "inserted": int(inserted), "start_id": str(args.start_id)}, incr_error=0))
        return


if __name__ == "__main__":
    main()
