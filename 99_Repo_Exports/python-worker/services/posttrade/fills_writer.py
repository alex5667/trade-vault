from __future__ import annotations

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""fills_writer — events:trades → fills table (Phase B).

Why this service exists
-----------------------
TCA worker needs a canonical, joinable fill stream:
  sid + ts_fill_ms + px + qty + fee_bps + side + venue + symbol

In the current system, fills are embedded inside `events:trades` events
(POSITION_OPENED / POSITION_CLOSED) with heterogeneous keys.
This writer normalizes them into the `fills` table.

Important constraints
---------------------
* Fail-open: broken events must not block the stream. They go to DLQ.
* Deterministic time: ts_fill_ms must be epoch ms.
* Idempotent: upsert by (sid, ts_fill_ms, fill_role).

ENV
---
REDIS_URL=redis://redis-worker-1:6379/0
TRADE_EVENTS_STREAM=events:trades
TRADE_EVENTS_GROUP=fills_writer_v1
TRADE_EVENTS_CONSUMER=<hostname:pid>

FILLS_DLQ_STREAM=events:fills:dlq
FILLS_DB_DSN=<TRADES_DB_DSN fallback chain>

FILLS_WRITER_BATCH_SIZE=256
FILLS_WRITER_BLOCK_MS=5000
"""

import asyncio
import logging
import os
import socket
from dataclasses import dataclass
from typing import Any

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

from common.redis_errors import is_redis_busy_loading_error, is_transient_error
from services.posttrade.fill_event_contract import normalize_fill_event, validate_fill_event
from services.posttrade.redis_stream_dlq import publish_dlq

logger = logging.getLogger("fills_writer")


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _env_int(name: str, default: str) -> int:
    try:
        return int(float(_env(name, default)))
    except Exception:
        return int(float(default))


def pick_dsn() -> str:
    return (
        os.getenv("FILLS_DB_DSN")
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("ANALYTICS_DSN")
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))
        or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL"))
        or ""
    )


def _now_ms() -> int:
    return get_ny_time_millis()


def _decode_fields(fields: dict[Any, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        if isinstance(v, (bytes, bytearray)):
            out[kk] = v.decode("utf-8", "replace")
        else:
            out[kk] = v
    return out


def _event_type(ev: dict[str, Any]) -> str:
    return str(ev.get("event_type") or ev.get("type") or ev.get("event") or "").upper().strip()


def _fill_role(et: str) -> str | None:
    if et == "POSITION_OPENED":
        return "entry"
    if et == "POSITION_CLOSED":
        return "exit"
    return None


def _best_effort_fee_bps(ev: dict[str, Any]) -> float | None:
    # Prefer explicit fee_bps
    for k in ("fee_bps", "fees_bps"):
        if k in ev and ev.get(k) not in (None, ""):
            try:
                return float(ev.get(k))
            except Exception:
                pass
    # Derive from fees_usd and turnover_roundtrip (if present)
    fees_usd = ev.get("fees_usd")
    turnover = ev.get("turnover_roundtrip") or ev.get("turnover_usd")
    try:
        if fees_usd is not None and turnover is not None:
            f = float(fees_usd)
            t = float(turnover)
            if t > 0:
                return float(f / t * 10_000.0)
    except Exception:
        pass
    return None


class PgWriter:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def _connect(self):
        try:
            import psycopg  # type: ignore
            return psycopg.connect(self.dsn)
        except Exception:
            import psycopg2  # type: ignore
            return psycopg2.connect(self.dsn)

    def upsert_fills(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        conn = self._connect()
        try:
            cur = conn.cursor()
            sql = (
                "INSERT INTO fills (ts, ts_fill_ms, sid, order_id, sym, venue, side, fill_role, px, qty, fee_bps, "
                "bid_at_fill, ask_at_fill, mid_at_fill, event_type, event_id, stream_id, ts_insert_ms) "
                "VALUES (to_timestamp(%(ts_fill_ms)s/1000.0), %(ts_fill_ms)s, %(sid)s, %(order_id)s, %(sym)s, %(venue)s, %(side)s, %(fill_role)s, %(px)s, %(qty)s, %(fee_bps)s, "
                "%(bid_at_fill)s, %(ask_at_fill)s, %(mid_at_fill)s, %(event_type)s, %(event_id)s, %(stream_id)s, %(ts_insert_ms)s) "
                "ON CONFLICT (sid, ts_fill_ms, fill_role, ts) DO UPDATE SET "
                "order_id=excluded.order_id, sym=excluded.sym, venue=excluded.venue, side=excluded.side, px=excluded.px, qty=excluded.qty, fee_bps=excluded.fee_bps, "
                "bid_at_fill=excluded.bid_at_fill, ask_at_fill=excluded.ask_at_fill, mid_at_fill=excluded.mid_at_fill, "
                "event_type=excluded.event_type, event_id=excluded.event_id, stream_id=excluded.stream_id, ts_insert_ms=excluded.ts_insert_ms"
            )
            cur.executemany(sql, rows)
            conn.commit()
            return len(rows)
        finally:
            conn.close()


@dataclass
class Cfg:
    redis_url: str
    stream: str
    group: str
    consumer: str
    block_ms: int
    count: int
    dlq_stream: str
    dlq_maxlen: int
    batch_size: int

    @staticmethod
    def from_env() -> Cfg:
        host = socket.gethostname()
        return Cfg(
            redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
            stream=_env("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES),
            group=_env("TRADE_EVENTS_GROUP", "fills_writer_v1"),
            consumer=_env("TRADE_EVENTS_CONSUMER", f"{host}:{os.getpid()}"),
            block_ms=_env_int("FILLS_WRITER_BLOCK_MS", "5000"),
            count=_env_int("FILLS_WRITER_COUNT", "256"),
            dlq_stream=_env("FILLS_DLQ_STREAM", "events:fills:dlq"),
            dlq_maxlen=_env_int("FILLS_DLQ_MAXLEN", "200000"),
            batch_size=_env_int("FILLS_WRITER_BATCH_SIZE", "256"),
        )


async def _ensure_group(
    r: Any,
    *,
    stream: str,
    group: str,
    max_retries: int = 60,
    retry_delay: float = 2.0,
) -> None:
    """Create the consumer group, retrying on transient/startup errors.

    Handles three cases:
    1. BUSYGROUP – group already exists, treat as success.
    2. Redis LOADING – RDB restore in progress, keep waiting.
    3. ConnectionError / TimeoutError / OSError – Redis not yet reachable
       (Docker startup race); retry up to *max_retries* times.
    """
    attempt = 0
    while True:
        try:
            await r.xgroup_create(stream, group, id="$", mkstream=True)
            logger.info("Consumer group '%s' ready on stream '%s'", group, stream)
            return
        except Exception as e:
            err_str = str(e).upper()
            if "BUSYGROUP" in err_str:
                logger.info(
                    "Consumer group '%s' already exists on stream '%s' (OK)", group, stream
                )
                return
            if is_redis_busy_loading_error(e):
                logger.info("Redis is loading dataset, retrying xgroup_create… (attempt %d)", attempt + 1)
                await asyncio.sleep(retry_delay)
                attempt += 1
                continue
            # Connection-level errors: Redis not reachable yet (Docker race condition).
            if isinstance(e, (ConnectionError, TimeoutError, OSError)) or is_transient_error(e):
                attempt += 1
                if attempt >= max_retries:
                    logger.error(
                        "fills_writer: Redis unreachable after %d attempts — giving up.", attempt
                    )
                    raise
                logger.warning(
                    "fills_writer: Redis not reachable yet (%s), retry %d/%d in %.0fs…",
                    e, attempt, max_retries, retry_delay,
                )
                await asyncio.sleep(retry_delay)
                continue
            raise


def _to_fill_row(ev: dict[str, Any], *, stream_id: str) -> tuple[dict[str, Any] | None, str]:
    et = _event_type(ev)
    role = _fill_role(et)
    if role is None:
        return None, "skip:not_fill"

    # Map trade events to canonical fill event keys
    evt: dict[str, Any] = {
        "sid": ev.get("sid") or ev.get("signal_id"),
        "order_id": ev.get("order_id") or ev.get("exit_order_id") or ev.get("position_id") or ev.get("event_id") or "",
        "ts_fill_ms": ev.get("ts_fill_ms") or ev.get("exit_ts_ms") or ev.get("ts") or ev.get("ts_ms"),
        "px": ev.get("px") or ev.get("price"),
        "qty": ev.get("qty") or ev.get("lot"),
        "fee_bps": _best_effort_fee_bps(ev),
        "venue": ev.get("venue") or ev.get("source"),
        "symbol": ev.get("symbol"),
        "side": ev.get("side") or ev.get("direction"),
        "bid_at_fill": ev.get("bid_at_fill"),
        "ask_at_fill": ev.get("ask_at_fill"),
        "mid_at_fill": ev.get("mid_at_fill"),
    }

    norm = normalize_fill_event(evt)
    # fee_bps is required by contract; allow default 0.0 if unknown.
    if norm.get("fee_bps") is None:
        norm["fee_bps"] = float(os.getenv("FILL_FEE_BPS_DEFAULT", "0") or 0)

    ok, missing = validate_fill_event(norm)
    if not ok:
        return None, "missing:" + ",".join(missing)

    row = {
        "ts_fill_ms": int(norm["ts_fill_ms"]),
        "sid": str(norm["sid"]),
        "order_id": str(norm["order_id"]),
        "sym": str(norm["symbol"]).upper(),
        "venue": str(norm["venue"]).lower(),
        "side": str(norm["side"]).upper(),
        "fill_role": role,
        "px": float(norm["px"]),
        "qty": float(norm["qty"]),
        "fee_bps": float(norm["fee_bps"]),
        "bid_at_fill": norm.get("bid_at_fill"),
        "ask_at_fill": norm.get("ask_at_fill"),
        "mid_at_fill": norm.get("mid_at_fill"),
        "event_type": et,
        "event_id": (ev.get("event_id") or ""),
        "stream_id": stream_id,
        "ts_insert_ms": _now_ms(),
    }
    return row, ""


async def main() -> None:
    if aioredis is None:
        raise RuntimeError("redis-py is required")

    dsn = pick_dsn()
    if not dsn:
        raise RuntimeError("FILLS_DB_DSN/TRADES_DB_DSN must be set")

    cfg = Cfg.from_env()
    r = aioredis.from_url(cfg.redis_url, decode_responses=False)
    await _ensure_group(r, stream=cfg.stream, group=cfg.group)

    pg = PgWriter(dsn)
    logger.info("fills_writer started: stream=%s group=%s", cfg.stream, cfg.group)

    while True:
        try:
            res = await r.xreadgroup(
                groupname=cfg.group,
                consumername=cfg.consumer,
                streams={cfg.stream: ">"},
                count=cfg.count,
                block=cfg.block_ms,
            )
            if not res:
                continue

            rows: list[dict[str, Any]] = []
            ack_ids: list[Any] = []
            for _stream, msgs in res:
                for mid, fields in msgs:
                    mid_s = mid.decode() if isinstance(mid, (bytes, bytearray)) else str(mid)
                    ev = _decode_fields(fields)
                    row, reason = _to_fill_row(ev, stream_id=mid_s)
                    if row is None:
                        if not reason.startswith("skip:"):
                            await publish_dlq(
                                r,
                                dlq_stream=cfg.dlq_stream,
                                reason=reason,
                                error="invalid_fill_event",
                                src_stream=cfg.stream,
                                src_entry_id=mid_s,
                                payload=ev,
                                maxlen=cfg.dlq_maxlen,
                            )
                        # Always ACK so the group doesn't get stuck.
                        await r.xack(cfg.stream, cfg.group, mid)
                        continue
                    rows.append(row)
                    ack_ids.append(mid)

                    if len(rows) >= cfg.batch_size:
                        pg.upsert_fills(rows)
                        await r.xack(cfg.stream, cfg.group, *ack_ids)
                        rows.clear(); ack_ids.clear()

            if rows:
                pg.upsert_fills(rows)
                await r.xack(cfg.stream, cfg.group, *ack_ids)

        except Exception as e:
            if is_redis_busy_loading_error(e):
                logger.warning("Redis is loading dataset into memory, pausing fills_writer loop...")
            elif is_transient_error(e):
                logger.warning(f"Transient redis error in fills_writer loop: {e}")
            else:
                logger.exception("fills_writer loop error")
            await asyncio.sleep(1.0)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    asyncio.run(main())
