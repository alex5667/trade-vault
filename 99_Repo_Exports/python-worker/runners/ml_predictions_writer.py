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

logger = logging.getLogger("ml_predictions_writer")

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
        processed_total=Counter("ml_predictions_processed_total", "Total ML predictions processed")
        written_total=Counter("ml_predictions_write_total", "Total ML predictions written to DB")
        dlq_total=Counter("ml_predictions_dlq_total", "Predictions sent to DLQ", ["reason"])
        db_fail_total=Counter("ml_predictions_db_fail_total", "Database write failures")
        redis_lag_ms=Histogram(
            "ml_predictions_pg_lag_ms"
            "Lag between prediction ts_ms and DB write"
            buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000]
        )
        pending_count=Gauge("ml_predictions_pending_count", "Pending messages in consumer group")
        last_ok=Gauge("ml_predictions_last_ok", "1 if last run was successful, 0 otherwise")
        last_batch_rows=Gauge("ml_predictions_last_batch_rows", "Number of rows in last batch")
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

def _to_bool(v: Any, default: bool = False) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "on"}:
        return True
    if s in {"0", "false", "no", "off"}:
        return False
    return default

def _normalize_row(evt: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    sid = str(evt.get("sid") or evt.get("signal_id") or "").strip()
    symbol = str(evt.get("symbol") or evt.get("sym") or "").strip()
    ts_ms = _to_int(evt.get("ts_ms") or evt.get("timestamp_ms"))
    
    if not sid:
        return None, "missing:sid"
    if not symbol:
        return None, "missing:symbol"
    if not ts_ms or ts_ms <= 0:
        return None, "bad:ts_ms"

    row = {
        "ts_ms": ts_ms
        "sid": sid
        "symbol": symbol
        "model_ver": str(evt.get("model_ver") or evt.get("model_version") or "")
        "mode": str(evt.get("mode") or "")
        "p_edge": _to_float(evt.get("p_edge"))
        "p_min": _to_float(evt.get("p_min"))
        "p_margin": _to_float(evt.get("p_margin"))
        "allow": _to_bool(evt.get("allow"))
        "bucket": str(evt.get("bucket") or "")
        "missing": _to_bool(evt.get("missing"), False)
        "latency_us": _to_int(evt.get("latency_us"))
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

    def insert_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        conn = self._connect()
        try:
            cur = conn.cursor()
            sql = (
                "INSERT INTO ml_predictions ("
                "ts_ms,sid,symbol,model_ver,mode,p_edge,p_min,p_margin"
                "allow,bucket,missing,latency_us) "
                "VALUES (%(ts_ms)s,%(sid)s,%(symbol)s,%(model_ver)s,%(mode)s,%(p_edge)s,%(p_min)s,%(p_margin)s"
                "%(allow)s,%(bucket)s,%(missing)s,%(latency_us)s) "
                "ON CONFLICT (ts_ms, sid) DO NOTHING"
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
    metrics_port: int
    fail_sleep_sec: float

    @staticmethod
    def from_env() -> "Cfg":
        host = socket.gethostname()
        return Cfg(
            redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0")
            stream=_env("ML_PREDICTIONS_STREAM", "metrics:ml_confirm")
            group=_env("ML_PREDICTIONS_CG", "ml_persistence_cg")
            consumer=_env("ML_PREDICTIONS_CONSUMER", f"{host}:{os.getpid()}")
            block_ms=_env_int("ML_PREDICTIONS_BLOCK_MS", 5000)
            count=_env_int("ML_PREDICTIONS_COUNT", 500)
            dlq_stream=_env("ML_PREDICTIONS_DLQ_STREAM", "events:ml_predictions:dlq")
            dlq_maxlen=_env_int("ML_PREDICTIONS_DLQ_MAXLEN", 200000)
            batch_size=_env_int("ML_PREDICTIONS_BATCH_SIZE", 500)
            metrics_port=_env_int("ML_PREDICTIONS_METRICS_PORT", 9842)
            fail_sleep_sec=_env_float("ML_PREDICTIONS_FAIL_SLEEP_SEC", 1.0)
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

    logger.info("ml_predictions_writer started: stream=%s group=%s", cfg.stream, cfg.group)
    while True:
        try:
            res = await r.xreadgroup(
                groupname=cfg.group
                consumername=cfg.consumer
                streams={cfg.stream: ">"}
                count=cfg.count
                block=cfg.block_ms
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
                    lag_ms = max(0, now_ms - row["ts_ms"])
                    metrics.redis_lag_ms.observe(lag_ms)
                    rows.append(row)
                    ack_ids.append(msg_id)

            if rows:
                try:
                    written = pg.insert_rows(rows[: cfg.batch_size])
                    metrics.written_total.inc(written)
                    metrics.last_ok.set(1)
                    metrics.last_batch_rows.set(written)
                except Exception as e:
                    metrics.db_fail_total.inc()
                    metrics.last_ok.set(0)
                    logger.exception("ml_predictions_writer DB failure")
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
            logger.exception("ml_predictions_writer loop failure")
            metrics.last_ok.set(0)
            await asyncio.sleep(cfg.fail_sleep_sec)

if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper()
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    raise SystemExit(asyncio.run(main()))
