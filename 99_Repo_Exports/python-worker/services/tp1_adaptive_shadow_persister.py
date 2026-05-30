"""tp1_adaptive_shadow_persister.py — XREADGROUP → PG ingestor for AdaptiveTP1
shadow decisions emitted by signals/level_enricher.py.

Pipeline:
  1) XREADGROUP `stream:tp1_adaptive_shadow_events` consumer group
     `tp1-adaptive-shadow-persister`.
  2) Decode flat field-pair envelope (produced by core/tp1_adaptive_metrics.
     emit_decision → build_envelope → _flatten_for_xadd).
  3) Batched INSERT into `scanner_analytics.tp1_adaptive_shadow` hypertable
     (idempotent via ON CONFLICT (ts, sid) DO NOTHING).
  4) XACK only after PG commit; poison-cap (5 retries) → DLQ.

Env:
  REDIS_URL                  default: redis://redis-worker-1:6379/0
  PG_DSN | ANALYTICS_DB_DSN  default: postgresql://trading:.../scanner_analytics
  TASP_STREAM                default: stream:tp1_adaptive_shadow_events
  TASP_GROUP                 default: tp1-adaptive-shadow-persister
  TASP_CONSUMER              default: tasp-1
  TASP_BATCH_SIZE            default: 100
  TASP_BLOCK_MS              default: 5000
  TASP_FLUSH_INTERVAL_S      default: 5
  TASP_MAX_RETRIES           default: 5
  TASP_DLQ_STREAM            default: tp1_adaptive_shadow_persister:dlq
  METRICS_PORT               default: 9886
"""

from __future__ import annotations

import logging
import os
import signal as _signal
import time
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("tp1_adaptive_shadow_persister")


# ── Config ────────────────────────────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
PG_DSN = (
    os.getenv("ANALYTICS_DB_DSN")
    or os.getenv("PG_DSN")
    or f"postgresql://trading:{os.getenv('TRADING_PASSWORD', 'trading_password')}"
       f"@scanner-postgres:5432/scanner_analytics"
)
STREAM = os.getenv("TASP_STREAM", "stream:tp1_adaptive_shadow_events")
GROUP = os.getenv("TASP_GROUP", "tp1-adaptive-shadow-persister")
CONSUMER = os.getenv("TASP_CONSUMER", "tasp-1")
BATCH_SIZE = int(os.getenv("TASP_BATCH_SIZE", "100"))
BLOCK_MS = int(os.getenv("TASP_BLOCK_MS", "5000"))
FLUSH_INTERVAL_S = float(os.getenv("TASP_FLUSH_INTERVAL_S", "5"))
MAX_RETRIES = int(os.getenv("TASP_MAX_RETRIES", "5"))
RETRY_TTL_SEC = 3600
DLQ_STREAM = os.getenv("TASP_DLQ_STREAM", "tp1_adaptive_shadow_persister:dlq")
DLQ_MAXLEN = int(os.getenv("TASP_DLQ_MAXLEN", "10000"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9886"))


# ── Metrics ───────────────────────────────────────────────────────────────────

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    _msgs_read = Counter("tasp_msgs_read_total", "Messages read from stream")
    _msgs_inserted = Counter("tasp_msgs_inserted_total", "Rows inserted into PG")
    _msgs_conflict = Counter("tasp_msgs_conflict_total", "Conflicts on (ts_ms, sid)")
    _msgs_dlq = Counter("tasp_msgs_dlq_total", "Messages routed to DLQ", ["reason"])
    _msgs_poison = Counter("tasp_msgs_poison_total", "Force-ACKed after max_retries")
    _batch_flush = Counter("tasp_batch_flush_total", "Batches flushed", ["outcome"])
    _pg_errors = Counter("tasp_pg_errors_total", "PG errors", ["kind"])
    _pel_size = Gauge("tasp_pel_size", "Current PEL size for our consumer")
    _batch_size_gauge = Gauge("tasp_batch_size", "Current accumulated batch size")
    _last_ok_ts = Gauge("tasp_last_ok_ts_ms", "Last successful flush ts (epoch ms)")
    _flush_latency = Histogram(
        "tasp_flush_latency_ms", "Flush latency in ms",
        buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000),
    )
    _METRICS_OK = True
except Exception:
    _msgs_read = _msgs_inserted = _msgs_conflict = _msgs_dlq = None  # type: ignore
    _msgs_poison = _batch_flush = _pg_errors = _pel_size = None  # type: ignore
    _batch_size_gauge = _last_ok_ts = _flush_latency = None  # type: ignore
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


_INT_FIELDS = {"ts_ms", "samples"}
_FLOAT_FIELDS = {
    "entry_price", "sl_price",
    "baseline_tp1_price", "baseline_tp1_rr",
    "adaptive_tp1_price", "adaptive_tp1_rr",
    "p_hit_baseline", "p_hit_adaptive",
    "ev_baseline_r", "ev_adaptive_r", "ev_delta_r", "cost_r",
    "spread_bps", "slippage_bps", "fee_bps",
}
_STR_FIELDS = {"sid", "symbol", "kind", "side", "regime", "reason_code", "mode"}


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


def _safe_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def parse_entry(msg_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
    """Decode flat-field XADD entry → row dict. Returns None if unsalvageable."""
    if not isinstance(fields, dict):
        return None

    ts_ms = _safe_int(fields.get("ts_ms"))
    if ts_ms is None:
        # fallback: parse from stream-id ms
        try:
            ts_ms = int(msg_id.split("-")[0])
        except Exception:
            return None

    sid = fields.get("sid")
    if not sid:
        return None
    sid = str(sid)

    row: dict[str, Any] = {
        "ts_ms": ts_ms,
        "sid": sid,
        "msg_id": msg_id,
    }
    for name in _STR_FIELDS:
        if name == "sid":
            continue
        v = fields.get(name)
        row[name] = str(v) if v not in (None, "") else None
    for name in _FLOAT_FIELDS:
        row[name] = _safe_float(fields.get(name))
    for name in _INT_FIELDS:
        if name == "ts_ms":
            continue
        row[name] = _safe_int(fields.get(name))

    # Required fields for INSERT NOT NULL columns
    if row.get("symbol") is None or row.get("kind") is None or row.get("side") is None:
        return None
    if row.get("entry_price") is None or row.get("sl_price") is None:
        return None
    if row.get("baseline_tp1_price") is None or row.get("baseline_tp1_rr") is None:
        return None
    if row.get("reason_code") is None or row.get("mode") is None:
        return None
    return row


# ── PG layer ──────────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO tp1_adaptive_shadow
    (ts_ms, sid, symbol, kind, side, regime,
     entry_price, sl_price,
     baseline_tp1_price, baseline_tp1_rr,
     adaptive_tp1_price, adaptive_tp1_rr,
     p_hit_baseline, p_hit_adaptive,
     ev_baseline_r, ev_adaptive_r, ev_delta_r, cost_r,
     spread_bps, slippage_bps, fee_bps, samples,
     reason_code, mode)
VALUES %s
ON CONFLICT (ts, sid) DO NOTHING
"""


def _flush_batch(pg_conn, batch: list[dict]) -> tuple[int, int]:
    if not batch:
        return 0, 0
    from psycopg2.extras import execute_values
    rows = []
    for r in batch:
        rows.append((
            r["ts_ms"], r["sid"], r["symbol"], r["kind"], r["side"], r.get("regime"),
            r["entry_price"], r["sl_price"],
            r["baseline_tp1_price"], r["baseline_tp1_rr"],
            r.get("adaptive_tp1_price"), r.get("adaptive_tp1_rr"),
            r.get("p_hit_baseline"), r.get("p_hit_adaptive"),
            r.get("ev_baseline_r"), r.get("ev_adaptive_r"),
            r.get("ev_delta_r"), r.get("cost_r"),
            r.get("spread_bps"), r.get("slippage_bps"), r.get("fee_bps"),
            r.get("samples"),
            r["reason_code"], r["mode"],
        ))
    t0 = time.perf_counter()
    with pg_conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows, page_size=200)
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
        if "BUSYGROUP" in str(e):
            log.debug("consumer group %s already exists", GROUP)
        else:
            log.warning("xgroup_create failed: %s", e)


def _push_dlq(r, msg_id: str, fields: dict, reason: str) -> bool:
    try:
        import json as _json
        entry = {
            "msg_id": msg_id,
            "reason": reason[:256],
            "fields_json": _json.dumps(fields, ensure_ascii=False, default=str)[:4000],
            "ts_ms": str(int(time.time() * 1000)),
        }
        r.xadd(DLQ_STREAM, entry, maxlen=DLQ_MAXLEN, approximate=True)
        _inc(_msgs_dlq, reason.split(":")[0])
        return True
    except Exception as e:
        log.warning("DLQ push failed for %s: %s", msg_id, e)
        return False


def _poison_check(r, msg_id: str) -> bool:
    key = f"tasp:retries:{msg_id}"
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
    log.info(
        "persister start: stream=%s group=%s consumer=%s batch=%d block=%dms",
        STREAM, GROUP, CONSUMER, BATCH_SIZE, BLOCK_MS,
    )

    batch: list[dict] = []
    last_flush = time.time()

    def _flush_now() -> None:
        nonlocal batch, last_flush
        if not batch:
            return
        try:
            inserted, conflicts = _flush_batch(pg, batch)
            _inc(_batch_flush, "ok")
            if _msgs_inserted is not None:
                try:
                    _msgs_inserted.inc(inserted)
                except Exception:
                    pass
            if _msgs_conflict is not None and conflicts > 0:
                try:
                    _msgs_conflict.inc(conflicts)
                except Exception:
                    pass
            # XACK after commit
            ids = [row["msg_id"] for row in batch]
            try:
                r.xack(STREAM, GROUP, *ids)
            except Exception as e:
                log.warning("XACK after commit failed: %s", e)
            _set(_last_ok_ts, time.time() * 1000)
        except Exception as e:
            _inc(_batch_flush, "error")
            _inc(_pg_errors, type(e).__name__)
            log.exception("flush failed (%d rows): %s", len(batch), e)
            try:
                pg.rollback()
            except Exception:
                pass
        finally:
            batch = []
            last_flush = time.time()
            _set(_batch_size_gauge, 0)

    while _running:
        try:
            resp = r.xreadgroup(
                GROUP, CONSUMER,
                streams={STREAM: ">"},
                count=BATCH_SIZE, block=BLOCK_MS,
            )
        except Exception as e:
            log.warning("xreadgroup failed: %s; sleep 1s", e)
            time.sleep(1.0)
            continue
        if not resp:
            if batch and (time.time() - last_flush) >= FLUSH_INTERVAL_S:
                _flush_now()
            continue
        for _stream_name, entries in resp:
            for msg_id, fields in entries:
                _inc(_msgs_read)
                row = parse_entry(msg_id, fields)
                if row is None:
                    if _poison_check(r, msg_id):
                        _push_dlq(r, msg_id, fields, "unparseable:max_retries")
                        try:
                            r.xack(STREAM, GROUP, msg_id)
                        except Exception:
                            pass
                    else:
                        _push_dlq(r, msg_id, fields, "unparseable")
                        try:
                            r.xack(STREAM, GROUP, msg_id)
                        except Exception:
                            pass
                    continue
                batch.append(row)
                _set(_batch_size_gauge, len(batch))
        if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_INTERVAL_S:
            _flush_now()

    # graceful drain
    _flush_now()
    try:
        pg.close()
    except Exception:
        pass
    log.info("exited cleanly")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
