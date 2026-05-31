"""signal_snapshot_persister.py — durable archive for `signals:of:inputs`.

Reads `signals:of:inputs` via a consumer group, batched-inserts into
`scanner_analytics.signal_snapshots` (Timescale hypertable, 30d retention,
7d compression). Backs ML training pipelines that need > Redis stream
retention.

Design:
  • Consumer group `signal-snapshot-persister` (stable across restarts).
  • Batched INSERT (psycopg2.extras.execute_values) every BATCH_SIZE
    msgs OR FLUSH_INTERVAL_S — whichever comes first.
  • ON CONFLICT (sid, ts) DO NOTHING — idempotent on replay.
  • XACK only after successful PG commit; on failure, message stays in PEL
    and the next iteration retries via XREADGROUP.
  • Poison-message cap (5 retries) → DLQ stream + force-ACK.

Env:
  REDIS_URL                  default: redis://redis-worker-1:6379/0
  PG_DSN | ANALYTICS_DB_DSN  default: postgresql://trading:.../scanner_analytics
  SSP_STREAM                 default: signals:of:inputs
  SSP_GROUP                  default: signal-snapshot-persister
  SSP_CONSUMER               default: ssp-1
  SSP_BATCH_SIZE             default: 200
  SSP_BLOCK_MS               default: 5000
  SSP_FLUSH_INTERVAL_S       default: 5
  SSP_MAX_RETRIES            default: 5
  SSP_DLQ_STREAM             default: signal_snapshot_persister:dlq
  METRICS_PORT               default: 9876

Wiring:
  • Compose: docker-compose.signal-snapshot-persister.yml
  • Consumer: tools/train_v15_lgbm.py (Postgres-first loader)
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import signal as _signal
import sys
import time
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("signal_snapshot_persister")

# ── Config ────────────────────────────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
PG_DSN = (
    os.getenv("ANALYTICS_DB_DSN")
    or os.getenv("PG_DSN")
    or f"postgresql://trading:{os.getenv('TRADING_PASSWORD', 'trading_password')}"
       f"@scanner-postgres:5432/scanner_analytics"
)
STREAM = os.getenv("SSP_STREAM", "signals:of:inputs")
GROUP = os.getenv("SSP_GROUP", "signal-snapshot-persister")
CONSUMER = os.getenv("SSP_CONSUMER", "ssp-1")
BATCH_SIZE = int(os.getenv("SSP_BATCH_SIZE", "200"))
BLOCK_MS = int(os.getenv("SSP_BLOCK_MS", "5000"))
FLUSH_INTERVAL_S = float(os.getenv("SSP_FLUSH_INTERVAL_S", "5"))
MAX_RETRIES = int(os.getenv("SSP_MAX_RETRIES", "5"))
RETRY_TTL_SEC = 3600
DLQ_STREAM = os.getenv("SSP_DLQ_STREAM", "signal_snapshot_persister:dlq")
DLQ_MAXLEN = int(os.getenv("SSP_DLQ_MAXLEN", "10000"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9877"))

# ── Metrics ───────────────────────────────────────────────────────────────────

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    _msgs_read = Counter("ssp_msgs_read_total", "Messages read from stream")
    _msgs_inserted = Counter("ssp_msgs_inserted_total", "Rows inserted into PG")
    _msgs_conflict = Counter("ssp_msgs_conflict_total", "Conflicts on duplicate sid")
    _msgs_dlq = Counter("ssp_msgs_dlq_total", "Messages routed to DLQ", ["reason"])
    _msgs_poison = Counter("ssp_msgs_poison_total", "Messages force-ACKed after max_retries")
    _batch_flush = Counter("ssp_batch_flush_total", "Batches flushed", ["outcome"])
    _pg_errors = Counter("ssp_pg_errors_total", "PG errors", ["kind"])
    _pel_size = Gauge("ssp_pel_size", "Current PEL size for our consumer")
    _batch_size = Gauge("ssp_batch_size", "Current accumulated batch size")
    _last_ok_ts = Gauge("ssp_last_ok_ts_ms", "Timestamp of last successful flush (epoch ms)")
    _flush_latency = Histogram(
        "ssp_flush_latency_ms",
        "Flush latency in ms",
        buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000),
    )
    _payload_size = Histogram(
        "ssp_payload_size_bytes",
        "Raw payload size in bytes (pre-gzip)",
        buckets=(1000, 5000, 10000, 25000, 50000, 100000, 200000, 500000),
    )
    _METRICS_OK = True
except Exception:
    _msgs_read = _msgs_inserted = _msgs_conflict = _msgs_dlq = None  # type: ignore
    _msgs_poison = _batch_flush = _pg_errors = _pel_size = None  # type: ignore
    _batch_size = _last_ok_ts = _flush_latency = _payload_size = None  # type: ignore
    start_http_server = None  # type: ignore
    _METRICS_OK = False


def _inc(metric, *labels: str) -> None:
    if metric is None:
        return
    try:
        if labels:
            metric.labels(*labels).inc()
        else:
            metric.inc()
    except Exception:
        pass


def _set(metric, value: float) -> None:
    if metric is None:
        return
    try:
        metric.set(value)
    except Exception:
        pass


# ── Parsing ───────────────────────────────────────────────────────────────────

def _safe_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
        if f != f or f == float("inf") or f == float("-inf"):
            return None
        return f
    except (TypeError, ValueError):
        return None


def parse_entry(msg_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
    """Decode XADD-payload to row-dict for PG. Returns None when unsalvageable."""
    raw = fields.get("payload") if isinstance(fields, dict) else None
    if not raw:
        return None
    try:
        p = json.loads(raw)
    except Exception:
        return None
    inner = p.get("data", p) if isinstance(p, dict) else p
    if isinstance(inner, str):
        try:
            inner = json.loads(inner)
        except Exception:
            return None
    if not isinstance(inner, dict):
        return None

    sid = inner.get("sid") or inner.get("signal_id")
    if not sid:
        return None
    sid = str(sid)

    # ts: prefer inner.ts_ms, else stream-id ms
    ts_ms_raw = inner.get("ts_ms") or inner.get("tick_ts") or inner.get("ts_emit_ms")
    try:
        ts_ms = int(ts_ms_raw) if ts_ms_raw else int(msg_id.split("-")[0])
    except Exception:
        ts_ms = int(msg_id.split("-")[0])

    symbol = str(inner.get("symbol") or "").upper()
    direction = str(inner.get("direction") or inner.get("side") or "").upper() or None

    # kind: try sid-prefix first
    kind = None
    if ":" in sid:
        first = sid.split(":", 1)[0]
        # lowercase + (alpha or alpha_alpha), e.g. "of", "iceberg", "delta_spike"
        stripped = first.replace("_", "").replace("-", "")
        if stripped and stripped.isalpha() and first.lower() == first:
            kind = first

    ind = inner.get("indicators") or {}
    if not isinstance(ind, dict):
        ind = {}

    regime = ind.get("regime") or inner.get("regime") or None
    if regime is not None:
        regime = str(regime).lower()
        if regime in ("none", "null"):
            regime = None

    confidence = _safe_float(
        inner.get("confidence") or ind.get("confidence_v1") or ind.get("confidence")
    )
    cb = ind.get("confidence_breakdown") or {}
    if not isinstance(cb, dict):
        cb = {}
    ml_shadow = _safe_float(cb.get("ml_shadow_conf01"))
    scorer_mode = cb.get("scorer_mode")
    if scorer_mode is not None:
        scorer_mode = str(scorer_mode)

    # Compressed full envelope (gzip on raw payload bytes)
    raw_bytes = raw.encode() if isinstance(raw, str) else bytes(raw)
    try:
        payload_gz = gzip.compress(raw_bytes, compresslevel=6)
    except Exception:
        payload_gz = None
    if _payload_size is not None:
        try:
            _payload_size.observe(len(raw_bytes))
        except Exception:
            pass

    return {
        "sid": sid,
        "ts_ms": ts_ms,
        "symbol": symbol,
        "direction": direction,
        "kind": kind,
        "regime": regime,
        "confidence": confidence,
        "ml_shadow_conf01": ml_shadow,
        "scorer_mode": scorer_mode,
        "indicators": ind,
        "payload_gz": payload_gz,
        "payload_size_bytes": len(raw_bytes),
        "msg_id": msg_id,  # only for XACK after success
    }


# ── PG layer ──────────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO signal_snapshots
    (sid, ts, ts_ms, symbol, direction, kind, regime,
     confidence, ml_shadow_conf01, scorer_mode,
     indicators, payload_gz, payload_size_bytes)
VALUES %s
ON CONFLICT (sid, ts) DO NOTHING
"""


def _flush_batch(pg_conn, batch: list[dict]) -> tuple[int, int]:
    """Returns (inserted_count, conflict_count). Raises on PG error."""
    if not batch:
        return 0, 0
    from psycopg2.extras import execute_values, Json
    rows = []
    for row in batch:
        rows.append((
            row["sid"],
            # ts: TIMESTAMPTZ from ts_ms
            row["ts_ms"] / 1000.0,
            row["ts_ms"],
            row["symbol"],
            row["direction"],
            row["kind"],
            row["regime"],
            row["confidence"],
            row["ml_shadow_conf01"],
            row["scorer_mode"],
            Json(row["indicators"]),
            psycopg2_bytea(row["payload_gz"]),
            row["payload_size_bytes"],
        ))
    t0 = time.perf_counter()
    with pg_conn.cursor() as cur:
        # The ts is given as epoch seconds → convert via to_timestamp on PG side
        # by overriding the format string.
        sql = INSERT_SQL.replace(
            "VALUES %s",
            "VALUES %s",
        )
        # We pass ts as float seconds; need template that casts to timestamptz
        template = (
            "(%s, to_timestamp(%s) AT TIME ZONE 'UTC', %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s::jsonb, %s, %s)"
        )
        execute_values(cur, sql, rows, template=template, page_size=200)
        affected = cur.rowcount
    pg_conn.commit()
    elapsed_ms = (time.perf_counter() - t0) * 1000
    if _flush_latency is not None:
        try:
            _flush_latency.observe(elapsed_ms)
        except Exception:
            pass
    conflicts = len(batch) - max(0, affected)
    return max(0, affected), max(0, conflicts)


def psycopg2_bytea(b: bytes | None):
    """Helper: pass None as NULL; bytes wrapped in psycopg2 Binary."""
    if b is None:
        return None
    import psycopg2
    return psycopg2.Binary(b)


# ── Runtime ───────────────────────────────────────────────────────────────────

_running = True


def _sighandler(signum, _frame) -> None:
    global _running
    log.info("signal %d received; draining", signum)
    _running = False


def _ensure_group(r) -> None:
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        log.info("created consumer group %s on %s", GROUP, STREAM)
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug("consumer group %s already exists", GROUP)
        else:
            log.warning("xgroup_create failed: %s", e)


def _push_dlq(r, msg_id: str, fields: dict, reason: str) -> bool:
    try:
        entry = {
            "msg_id": msg_id,
            "reason": reason[:256],
            "fields_json": json.dumps(fields, ensure_ascii=False, default=str)[:4000],
            "ts_ms": str(int(time.time() * 1000)),
        }
        r.xadd(DLQ_STREAM, entry, maxlen=DLQ_MAXLEN, approximate=True)
        _inc(_msgs_dlq, reason.split(":")[0])
        return True
    except Exception as e:
        log.warning("DLQ push failed for %s: %s", msg_id, e)
        return False


def _poison_check(r, msg_id: str) -> bool:
    """True if message exceeded retry cap — caller must force-ACK."""
    key = f"ssp:retries:{msg_id}"
    try:
        count = int(r.incr(key) or 0)
        r.expire(key, RETRY_TTL_SEC)
        if count > MAX_RETRIES:
            _inc(_msgs_poison)
            log.warning("poison: msg_id=%s retries=%d > max=%d", msg_id, count, MAX_RETRIES)
            return True
    except Exception:
        pass
    return False


def run() -> int:
    if _METRICS_OK and start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
            log.info("prometheus metrics on :%d", METRICS_PORT)
        except Exception as e:
            log.warning("metrics server failed: %s", e)

    _signal.signal(_signal.SIGTERM, _sighandler)
    _signal.signal(_signal.SIGINT, _sighandler)

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2
    try:
        import psycopg2
    except ImportError:
        log.error("psycopg2 not installed")
        return 2

    r = redis.from_url(REDIS_URL, decode_responses=True)
    _ensure_group(r)

    pg = psycopg2.connect(PG_DSN)
    pg.autocommit = False
    log.info("connected to PG: %s", PG_DSN.split("@")[-1] if "@" in PG_DSN else PG_DSN)
    log.info("consuming %s as %s/%s batch=%d flush_interval=%.1fs",
             STREAM, GROUP, CONSUMER, BATCH_SIZE, FLUSH_INTERVAL_S)

    batch: list[dict] = []
    last_flush_ts = time.monotonic()

    def maybe_flush(force: bool = False) -> None:
        nonlocal batch, last_flush_ts
        if not batch:
            return
        if not force and len(batch) < BATCH_SIZE and (time.monotonic() - last_flush_ts) < FLUSH_INTERVAL_S:
            return
        if _batch_size is not None:
            _batch_size.set(len(batch))
        try:
            inserted, conflicts = _flush_batch(pg, batch)
            if _msgs_inserted is not None:
                _msgs_inserted.inc(inserted)
            if _msgs_conflict is not None:
                _msgs_conflict.inc(conflicts)
            _inc(_batch_flush, "ok")
            _set(_last_ok_ts, time.time() * 1000)
            # XACK all messages in the batch
            ids = [row["msg_id"] for row in batch]
            try:
                r.xack(STREAM, GROUP, *ids)
            except Exception as e:
                log.warning("xack failed for %d msgs: %s", len(ids), e)
            log.info("flushed batch: n=%d inserted=%d conflicts=%d", len(batch), inserted, conflicts)
        except Exception as e:
            log.error("PG flush failed; batch stays in PEL: %s", e)
            _inc(_pg_errors, "flush")
            _inc(_batch_flush, "error")
            try:
                pg.rollback()
            except Exception:
                pass
            # Don't drain batch — XREADGROUP without ACK will redeliver
        finally:
            batch = []
            last_flush_ts = time.monotonic()
            if _batch_size is not None:
                _batch_size.set(0)

    while _running:
        try:
            try:
                msgs = r.xreadgroup(
                    GROUP, CONSUMER,
                    {STREAM: ">"},
                    count=BATCH_SIZE,
                    block=BLOCK_MS,
                )
            except Exception as e:
                log.warning("XREADGROUP error: %s", e)
                time.sleep(2)
                continue

            if not msgs:
                # Idle → flush whatever is buffered
                maybe_flush(force=False)
                continue

            for _stream, entries in msgs:
                for msg_id, fields in entries:
                    _inc(_msgs_read)
                    if _poison_check(r, msg_id):
                        _push_dlq(r, msg_id, fields, "max_retries_exceeded")
                        try:
                            r.xack(STREAM, GROUP, msg_id)
                        except Exception:
                            pass
                        continue
                    parsed = parse_entry(msg_id, fields)
                    if parsed is None:
                        if _push_dlq(r, msg_id, fields, "parse_error"):
                            try:
                                r.xack(STREAM, GROUP, msg_id)
                            except Exception:
                                pass
                        continue
                    batch.append(parsed)
                    if len(batch) >= BATCH_SIZE:
                        maybe_flush(force=True)
            # Time-based flush
            maybe_flush(force=False)

            # Update PEL gauge occasionally
            try:
                pending = r.xpending(STREAM, GROUP)
                if pending and _pel_size is not None:
                    _pel_size.set(int(pending.get("pending", 0)) if isinstance(pending, dict) else 0)
            except Exception:
                pass

        except KeyboardInterrupt:
            log.info("interrupted")
            break
        except Exception as e:
            log.exception("loop error: %s", e)
            _inc(_pg_errors, "loop")
            time.sleep(2)

    # Drain
    log.info("draining final batch (n=%d)", len(batch))
    maybe_flush(force=True)
    try:
        pg.close()
    except Exception:
        pass
    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(run())
