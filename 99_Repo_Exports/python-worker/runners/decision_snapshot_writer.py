import asyncio
import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

from prometheus_client import start_http_server, Counter, Gauge, Histogram

logger = logging.getLogger("decision_snapshot_writer")

@dataclass
class Metrics:
    processed_total: Counter
    written_total: Counter
    dlq_total: Counter
    db_fail_total: Counter
    redis_lag_ms: Histogram
    pending_count: Gauge
    last_ok: Gauge
    last_batch_rows: Gauge

def build_metrics() -> Metrics:
    return Metrics(
        processed_total=Counter("decision_snapshot_processed_total", "Total snapshots processed"),
        written_total=Counter("decision_snapshot_publish_total", "Total snapshots written to DB", ["tca_ready"]),
        dlq_total=Counter("decision_snapshot_dlq_total", "Snapshots sent to DLQ", ["reason"]),
        db_fail_total=Counter("decision_snapshot_db_fail_total", "Database write failures"),
        redis_lag_ms=Histogram(
            "decision_snapshot_pg_lag_ms",
            "Lag between decision_ts_ms and DB write",
            buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000],
        ),
        pending_count=Gauge("decision_snapshot_pending_count", "Pending messages in consumer group"),
        last_ok=Gauge("decision_snapshot_last_ok", "1 if last run was successful, 0 otherwise"),
        last_batch_rows=Gauge("decision_snapshot_last_batch_rows", "Number of rows in last batch"),
    )

def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env(name, str(default))))
    except Exception:
        return int(default)

def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except Exception:
        return float(default)

def pick_dsn() -> str:
    return (
        (os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN"))
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or ""
    )

def _decode(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", "replace")
        except Exception:
            return str(v)
    return v

def _loads_json(v: Any) -> Optional[dict]:
    if v is None:
        return None
    if isinstance(v, dict):
        return v
    v = _decode(v)
    if not isinstance(v, str):
        v = str(v)
    s = v.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None

def _parse_stream_fields(fields: Dict[Any, Any]) -> Dict[str, Any]:
    if b"payload" in fields:
        return _loads_json(fields.get(b"payload")) or {}
    if "payload" in fields:
        return _loads_json(fields.get("payload")) or {}
    out: Dict[str, Any] = {}
    for k, v in fields.items():
        out[str(_decode(k))] = _decode(v)
    return out

def _to_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None

def _to_int(v: Any) -> Optional[int]:
    try:
        return int(float(v))
    except Exception:
        return None

def _normalize_row(evt: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    sid = str(evt.get("sid") or evt.get("signal_id") or "").strip()
    signal_id = str(evt.get("signal_id") or sid).strip()
    symbol = str(evt.get("symbol") or evt.get("sym") or "").strip()
    decision_ts_ms = _to_int(evt.get("decision_ts_ms") or evt.get("ts_ms") or evt.get("ts_emit_ms"))
    
    if not sid:
        return None, "missing:sid"
    if not symbol:
        return None, "missing:symbol"
    if not decision_ts_ms or decision_ts_ms <= 0:
        return None, "bad:decision_ts_ms"

    flags_raw = evt.get("book_sanity_flags") or []
    flags = [str(x) for x in flags_raw] if isinstance(flags_raw, list) else []

    row = {
        "decision_ts_ms": decision_ts_ms,
        "sid": sid,
        "signal_id": signal_id,
        "symbol": symbol,
        "decision_mid": _to_float(evt.get("decision_mid")),
        "decision_bid": _to_float(evt.get("decision_bid")),
        "decision_ask": _to_float(evt.get("decision_ask")),
        "decision_spread_bps": _to_float(evt.get("decision_spread_bps")),
        "decision_expected_slippage_bps": _to_float(evt.get("decision_expected_slippage_bps")),
        "decision_exec_risk_norm": _to_float(evt.get("decision_exec_risk_norm")),
        "book_sanity_flags": json.dumps(flags, ensure_ascii=False),
        "tca_ready": bool(evt.get("tca_ready", False)),
        "payload_jsonb": json.dumps(evt, ensure_ascii=False),
    }
    return row, ""

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

    def insert_rows(self, rows: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not rows:
            return 0, 0
        conn = self._connect()
        try:
            cur = conn.cursor()
            sql = (
                "INSERT INTO trade_decisions_tca ("
                "decision_ts_ms,sid,signal_id,symbol,decision_mid,decision_bid,decision_ask"
                "decision_spread_bps,decision_expected_slippage_bps,decision_exec_risk_norm"
                "book_sanity_flags,tca_ready,payload_jsonb) "
                "VALUES (%(decision_ts_ms)s,%(sid)s,%(signal_id)s,%(symbol)s,%(decision_mid)s,%(decision_bid)s,%(decision_ask)s"
                "%(decision_spread_bps)s,%(decision_expected_slippage_bps)s,%(decision_exec_risk_norm)s"
                "%(book_sanity_flags)s,%(tca_ready)s,%(payload_jsonb)s) "
                "ON CONFLICT (decision_ts_ms, sid) DO NOTHING"
            )
            cur.executemany(sql, rows)
            conn.commit()
            tca_ready_count = sum(1 for r in rows if r["tca_ready"])
            return len(rows), tca_ready_count
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
    metrics_port: int
    fail_sleep_sec: float

    @staticmethod
    def from_env() -> "Cfg":
        host = socket.gethostname()
        return Cfg(
            redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
            stream=_env("DECISION_SNAPSHOT_STREAM", "events:decision_snapshot"),
            group=_env("DECISION_SNAPSHOT_CG", "tca_persistence_cg"),
            consumer=_env("DECISION_SNAPSHOT_CONSUMER", f"{host}:{os.getpid()}"),
            block_ms=_env_int("DECISION_SNAPSHOT_BLOCK_MS", 5000),
            count=_env_int("DECISION_SNAPSHOT_COUNT", 500),
            dlq_stream=_env("DECISION_SNAPSHOT_DLQ_STREAM", "events:decision_snapshot:dlq"),
            dlq_maxlen=_env_int("DECISION_SNAPSHOT_DLQ_MAXLEN", 200000),
            batch_size=_env_int("DECISION_SNAPSHOT_BATCH_SIZE", 500),
            metrics_port=_env_int("DECISION_SNAPSHOT_METRICS_PORT", 9841),
            fail_sleep_sec=_env_float("DECISION_SNAPSHOT_FAIL_SLEEP_SEC", 1.0),
        )

async def _ensure_group(r: Any, *, stream: str, group: str) -> None:
    while True:
        try:
            await r.xgroup_create(stream, group, id="$", mkstream=True)
            return
        except Exception as e:
            if "BUSYGROUP" in str(e).upper():
                return
            if "LOADING" in str(e).upper():
                await asyncio.sleep(1.0)
                continue
            raise

async def main() -> None:
    if aioredis is None:
        raise RuntimeError("redis-py is required")
    dsn = pick_dsn()
    if not dsn:
        raise RuntimeError("TRADES_DB_DSN (or TIMESCALE_DSN) must be set")

    cfg = Cfg.from_env()
    metrics = build_metrics()
    start_http_server(cfg.metrics_port)

    r = aioredis.from_url(cfg.redis_url, decode_responses=False)
    await _ensure_group(r, stream=cfg.stream, group=cfg.group)
    pg = PgWriter(dsn)

    logger.info("decision_snapshot_writer started: stream=%s group=%s", cfg.stream, cfg.group)
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
                try:
                    pend = await r.xpending(cfg.stream, cfg.group)
                    pending = int(pend["pending"] if isinstance(pend, dict) else pend[0])
                    metrics.pending_count.set(pending)
                except Exception:
                    pass
                continue

            ack_ids: List[str] = []
            rows: List[Dict[str, Any]] = []
            
            for _stream, entries in res:
                for msg_id, fields in entries:
                    payload = _parse_stream_fields(fields)
                    metrics.processed_total.inc()
                    row, reason = _normalize_row(payload)
                    if row is None:
                        metrics.dlq_total.labels(reason=reason).inc()
                        ack_ids.append(msg_id)
                        continue
                    
                    now_ms = int(time.time() * 1000)
                    lag_ms = max(0, now_ms - row["decision_ts_ms"])
                    metrics.redis_lag_ms.observe(lag_ms)
                    rows.append(row)
                    ack_ids.append(msg_id)

            if rows:
                try:
                    written, tca_ready_count = pg.insert_rows(rows[: cfg.batch_size])
                    metrics.written_total.labels(tca_ready="true").inc(tca_ready_count)
                    metrics.written_total.labels(tca_ready="false").inc(written - tca_ready_count)
                    metrics.last_ok.set(1)
                    metrics.last_batch_rows.set(written)
                except Exception as e:
                    metrics.db_fail_total.inc()
                    metrics.last_ok.set(0)
                    logger.exception("decision_snapshot_writer DB failure")
                    await asyncio.sleep(cfg.fail_sleep_sec)
                    continue

            if ack_ids:
                await r.xack(cfg.stream, cfg.group, *ack_ids)
                
            try:
                pend = await r.xpending(cfg.stream, cfg.group)
                pending = int(pend["pending"] if isinstance(pend, dict) else pend[0])
            except Exception:
                pending = 0
            metrics.pending_count.set(pending)

        except Exception:
            logger.exception("decision_snapshot_writer loop failure")
            metrics.last_ok.set(0)
            await asyncio.sleep(cfg.fail_sleep_sec)

if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    raise SystemExit(asyncio.run(main()))
