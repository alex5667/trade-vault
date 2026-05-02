#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
"""Phase-0 ML runtime telemetry rollup worker.

Consumes `metrics:ml_confirm` and produces low-cardinality per-minute rollups to:
  1) Redis stream  `stream:ml:health_snapshot`
  2) Redis hash    `metrics:ml:health_snapshot:last`
  3) Timescale     `ml_model_runtime_1m` (optional, fail-open)
  4) Prometheus    exporter metrics

Implementation notes
--------------------
- Uses Redis consumer groups for durable progress.
- Quantiles are estimated from fixed latency histogram buckets (deterministic).
- ECE/Brier are left nullable in Phase-0 unless source metrics are supplied by
  external nightly jobs; this worker focuses on runtime telemetry.
- No writes into hot-path keys; only separate control-plane keys/streams.
""",
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from prometheus_client import Counter, Gauge, Histogram, start_http_server

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import execute_values, Json  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore
    execute_values = None  # type: ignore
    Json = None  # type: ignore


LATENCY_BOUNDS_MS: List[float] = [0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 250.0, 500.0, 1000.0]

STREAM = os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm")
GROUP = os.getenv("ML_HEALTH_GROUP", "cg:ml_health_rollup_v1")
CONSUMER = os.getenv("ML_HEALTH_CONSUMER", os.getenv("HOSTNAME", "ml-health-rollup-v1"))
OUT_STREAM = os.getenv("ML_HEALTH_OUT_STREAM", "stream:ml:health_snapshot")
OUT_SUMMARY_KEY = os.getenv("ML_HEALTH_OUT_SUMMARY_KEY", "metrics:ml:health_snapshot:last")
BUCKET_PREFIX = os.getenv("ML_HEALTH_BUCKET_PREFIX", "metrics:ml:bucket:")
STATE_KEY = os.getenv("ML_HEALTH_STATE_KEY", "metrics:ml:health_rollup:state")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
DB_DSN = os.getenv("TRADES_DB_DSN", "")
DB_ENABLE = os.getenv("ML_HEALTH_DB_ENABLE", "1") == "1"
PORT = int(os.getenv("ML_HEALTH_EXPORTER_PORT", "9853"))
BLOCK_MS = int(os.getenv("ML_HEALTH_BLOCK_MS", "5000"))
COUNT = int(os.getenv("ML_HEALTH_READ_COUNT", "500"))
FINALIZE_LAG_MIN = int(os.getenv("ML_HEALTH_FINALIZE_LAG_MIN", "1"))
STATS_TTL_S = int(os.getenv("ML_HEALTH_BUCKET_TTL_S", str(3 * 24 * 3600)))
LOOP_SLEEP_S = float(os.getenv("ML_HEALTH_LOOP_SLEEP_S", "10"))


UP = Gauge("ml_health_rollup_up", "1 if worker loop is healthy")
LAST_RUN_TS = Gauge("ml_health_rollup_last_run_ts_seconds", "Last successful loop timestamp")
LAST_FINALIZED_MINUTE = Gauge("ml_health_rollup_last_finalized_minute", "Last finalized minute")
QUEUE_LAG_MS = Gauge("ml_health_rollup_queue_lag_ms", "Approx queue lag in ms")
SNAPSHOTS_WRITTEN = Counter("ml_health_snapshots_written_total", "Snapshots written", ["family"])
SNAPSHOT_ROWS = Gauge("ml_health_snapshot_rows", "Last finalized rows", ["family"])
EVENTS = Counter("ml_health_events_total", "Consumed ml_confirm metrics events", ["kind", "status"])
LOOP_LAT = Histogram("ml_health_rollup_loop_seconds", "Loop duration seconds")
LAT_P95 = Gauge("ml_health_runtime_latency_p95_ms", "Finalized p95 latency", ["model_id", "symbol"])
ALLOW_RATE = Gauge("ml_health_runtime_allow_rate", "Finalized allow rate", ["model_id", "symbol"])
ERR_RATE = Gauge("ml_health_runtime_error_rate", "Finalized error rate", ["model_id", "symbol"])


@dataclass
class Cfg:
    redis_url: str
    stream: str
    group: str
    consumer: str
    out_stream: str
    out_summary_key: str
    bucket_prefix: str
    state_key: str
    block_ms: int
    count: int
    finalize_lag_min: int
    bucket_ttl_s: int
    db_dsn: str
    db_enable: bool


@dataclass
class SnapshotRow:
    ts_ms: int
    model_id: str
    symbol: str
    mode: str
    latency_p50_ms: Optional[float]
    latency_p95_ms: Optional[float]
    latency_p99_ms: Optional[float]
    allow_rate: Optional[float]
    block_rate: Optional[float]
    abstain_rate: Optional[float]
    shadow_rate: Optional[float]
    error_rate: Optional[float]
    ece: Optional[float]
    brier: Optional[float]
    psi_top_json: List[str]
    ks_top_json: List[str]
    missing_critical_rate: Optional[float]
    artifact_age_sec: Optional[float]

    def stream_payload(self) -> Dict[str, Any]:
        return {
            "schema_version": 1,
            "event": "ml_health_snapshot_1m",
            "ts_ms": self.ts_ms,
            "model_id": self.model_id,
            "symbol": self.symbol,
            "mode": self.mode,
            "latency_p50_ms": "" if self.latency_p50_ms is None else f"{self.latency_p50_ms:.6f}",
            "latency_p95_ms": "" if self.latency_p95_ms is None else f"{self.latency_p95_ms:.6f}",
            "latency_p99_ms": "" if self.latency_p99_ms is None else f"{self.latency_p99_ms:.6f}",
            "allow_rate": "" if self.allow_rate is None else f"{self.allow_rate:.6f}",
            "block_rate": "" if self.block_rate is None else f"{self.block_rate:.6f}",
            "abstain_rate": "" if self.abstain_rate is None else f"{self.abstain_rate:.6f}",
            "shadow_rate": "" if self.shadow_rate is None else f"{self.shadow_rate:.6f}",
            "error_rate": "" if self.error_rate is None else f"{self.error_rate:.6f}",
            "ece": "" if self.ece is None else f"{self.ece:.6f}",
            "brier": "" if self.brier is None else f"{self.brier:.6f}",
            "psi_top_json": json.dumps(self.psi_top_json, separators=(",", ":")),
            "ks_top_json": json.dumps(self.ks_top_json, separators=(",", ":")),
            "missing_critical_rate": "" if self.missing_critical_rate is None else f"{self.missing_critical_rate:.6f}",
            "artifact_age_sec": "" if self.artifact_age_sec is None else f"{self.artifact_age_sec:.6f}",
        }


def _now_ms() -> int:
    return get_ny_time_millis()


def _as_str(v: Any, d: str = "") -> str:
    try:
        if v is None:
            return d
        if isinstance(v, (bytes, bytearray)):
            return bytes(v).decode("utf-8", errors="ignore")
        return str(v)
    except Exception:
        return d


def _as_int(v: Any, d: int = 0) -> int:
    try:
        if v is None or isinstance(v, bool):
            return d
        return int(float(v))
    except Exception:
        return d


def _as_float(v: Any, d: float = 0.0) -> float:
    try:
        if v is None or isinstance(v, bool):
            return d
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _minute(ts_ms: int) -> int:
    return int(ts_ms // 60000)


def _lat_field(v: Any) -> float:
    return _as_float(v, 0.0)


def _bucket_key(cfg: Cfg, minute: int, model_id: str, symbol: str) -> str:
    return f"{cfg.bucket_prefix}{minute}:{model_id}:{symbol}"


def _model_id(fields: Mapping[str, Any]) -> str:
    kind = _as_str(fields.get("kind") or "unknown")
    run_id = _as_str(fields.get("model_run_id") or "na")
    return f"{kind}:{run_id}"


def _status(fields: Mapping[str, Any]) -> str:
    return _as_str(fields.get("status") or "") or ("ALLOW" if _as_int(fields.get("allow"), 0) == 1 else "DENY")


def _symbol(fields: Mapping[str, Any]) -> str:
    return _as_str(fields.get("symbol") or "*")


def _mode(fields: Mapping[str, Any]) -> str:
    return _as_str(fields.get("mode") or "UNKNOWN")


def _latency_bucket_index(lat_ms: float) -> int:
    for i, bound in enumerate(LATENCY_BOUNDS_MS):
        if lat_ms <= bound:
            return i
    return len(LATENCY_BOUNDS_MS)


def _hist_quantile(counts: Sequence[int], q: float) -> Optional[float]:
    total = sum(int(x) for x in counts)
    if total <= 0:
        return None
    target = max(1, int(math.ceil(total * q)))
    acc = 0
    for i, c in enumerate(counts):
        acc += int(c)
        if acc >= target:
            if i < len(LATENCY_BOUNDS_MS):
                return float(LATENCY_BOUNDS_MS[i])
            return float(LATENCY_BOUNDS_MS[-1])
    return float(LATENCY_BOUNDS_MS[-1])


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=REDIS_URL,
        stream=STREAM,
        group=GROUP,
        consumer=CONSUMER,
        out_stream=OUT_STREAM,
        out_summary_key=OUT_SUMMARY_KEY,
        bucket_prefix=BUCKET_PREFIX,
        state_key=STATE_KEY,
        block_ms=BLOCK_MS,
        count=COUNT,
        finalize_lag_min=FINALIZE_LAG_MIN,
        bucket_ttl_s=STATS_TTL_S,
        db_dsn=DB_DSN,
        db_enable=DB_ENABLE,
    )


def _connect_redis(url: str):
    if redis is None:
        raise RuntimeError("redis-py is required")
    return redis.Redis.from_url(url, decode_responses=True)


def ensure_group(r: Any, cfg: Cfg) -> None:
    try:
        r.xgroup_create(cfg.stream, cfg.group, id="0", mkstream=True)
    except Exception:
        pass


def _ingest_one(pipe: Any, cfg: Cfg, fields: Mapping[str, Any], stream_id: str) -> None:
    ts_ms = _as_int(fields.get("ts_ms"), 0)
    if ts_ms <= 0:
        ts_ms = _as_int(stream_id.split("-", 1)[0], 0)
    minute = _minute(ts_ms)
    model_id = _model_id(fields)
    symbol = _symbol(fields)
    key = _bucket_key(cfg, minute, model_id, symbol)
    status = _status(fields)
    lat_ms = _lat_field(fields.get("lat_ms") or fields.get("latency_ms") or 0.0)
    mode = _mode(fields)
    missing_n = _as_int(fields.get("missing_n"), 0)

    mapping: Dict[str, float] = {
        "n": 1.0,
        "lat_sum_ms": float(lat_ms),
        "missing_sum": float(max(0, missing_n)),
    }
    if status == "ALLOW":
        mapping["allow_n"] = 1.0
    elif status == "SHADOW":
        mapping["shadow_n"] = 1.0
    elif status.startswith("ABSTAIN") or _as_int(fields.get("abstain"), 0) == 1:
        mapping["abstain_n"] = 1.0
    elif status.startswith("ERR") or _as_str(fields.get("err")):
        mapping["err_n"] = 1.0
    else:
        mapping["deny_n"] = 1.0

    idx = _latency_bucket_index(lat_ms)
    mapping[f"lat_b_{idx}"] = 1.0

    pipe.hsetnx(key, "minute", minute)
    pipe.hsetnx(key, "model_id", model_id)
    pipe.hsetnx(key, "symbol", symbol)
    pipe.hsetnx(key, "mode", mode)
    for k, v in mapping.items():
        pipe.hincrbyfloat(key, k, float(v))
    pipe.expire(key, cfg.bucket_ttl_s)


# ── Finalize scan cache (avoid SCAN storm every loop) ──────────────
_finalize_cache_ts: float = 0.0
_finalize_cache_result: List[int] = []
_FINALIZE_CACHE_TTL_S = float(os.getenv("ML_HEALTH_FINALIZE_CACHE_TTL_S", "30"))


def _finalizable_minutes(r: Any, cfg: Cfg, now_minute: int) -> List[int]:
    global _finalize_cache_ts, _finalize_cache_result
    now = time.monotonic()
    if (now - _finalize_cache_ts) < _FINALIZE_CACHE_TTL_S and _finalize_cache_result:
        # Return cached result filtered by current minute
        return [m for m in _finalize_cache_result if m <= now_minute - cfg.finalize_lag_min]

    patt = f"{cfg.bucket_prefix}*"
    mins: set[int] = set()
    cursor = 0
    try:
        while True:
            # P-LATENCY-FIX: COUNT reduced from 10000 to 500 to minimize
            # per-call Redis blocking time (was 6-8ms → now ~0.3-0.5ms).
            cursor, keys = r.scan(cursor=cursor, match=patt, count=500)
            for key in keys:
                s = _as_str(key)
                try:
                    suffix = s[len(cfg.bucket_prefix):]
                    minute_str = suffix.split(":", 1)[0]
                    m = int(minute_str)
                    mins.add(m)
                except Exception:
                    continue
            if int(cursor) == 0:
                break
            time.sleep(0.010)  # yield 10ms between batches to reduce HoL blocking
    except Exception:
        return []

    _finalize_cache_result = sorted(mins)
    _finalize_cache_ts = now
    return [m for m in _finalize_cache_result if m <= now_minute - cfg.finalize_lag_min]


def _load_external_health(r: Any, model_id: str) -> Tuple[Optional[float], Optional[float], List[str], List[str], Optional[float], Optional[float]]:
    """Best-effort enrichment from existing control-plane sources.

    Returns: (ece, brier, psi_top_json, ks_top_json, missing_critical_rate, artifact_age_sec)
    """,
    ece = None
    brier = None
    psi_top: List[str] = []
    ks_top: List[str] = []
    missing_rate = None
    artifact_age = None

    # Feature drift batch summary is currently global, not per-model.
    try:
        h = r.hgetall("metrics:feature_drift_batch:last") or {}
        worst_psi = _as_float(h.get("worst_psi"), 0.0)
        worst_ks = _as_float(h.get("worst_ks_stat"), 0.0)
        if worst_psi > 0:
            psi_top = [f"worst_psi={worst_psi:.4f}"]
        if worst_ks > 0:
            ks_top = [f"worst_ks={worst_ks:.4f}"]
    except Exception:
        pass

    # Inventory summary can be used for artifact age only if we already exported model rows.
    return ece, brier, psi_top, ks_top, missing_rate, artifact_age


def _build_snapshot_from_hash(r: Any, h: Mapping[str, Any], minute: int) -> SnapshotRow:
    model_id = _as_str(h.get("model_id"), "unknown:na")
    symbol = _as_str(h.get("symbol"), "*")
    mode = _as_str(h.get("mode"), "UNKNOWN")
    n = max(0, _as_int(h.get("n"), 0))
    counts = [_as_int(h.get(f"lat_b_{i}"), 0) for i in range(len(LATENCY_BOUNDS_MS) + 1)]
    p50 = _hist_quantile(counts, 0.50)
    p95 = _hist_quantile(counts, 0.95)
    p99 = _hist_quantile(counts, 0.99)

    allow_n = _as_int(h.get("allow_n"), 0)
    shadow_n = _as_int(h.get("shadow_n"), 0)
    abstain_n = _as_int(h.get("abstain_n"), 0)
    err_n = _as_int(h.get("err_n"), 0)
    deny_n = _as_int(h.get("deny_n"), 0)
    denom = float(n) if n > 0 else 0.0
    allow_rate = (allow_n / denom) if denom else None
    block_rate = (deny_n / denom) if denom else None
    abstain_rate = (abstain_n / denom) if denom else None
    shadow_rate = (shadow_n / denom) if denom else None
    error_rate = (err_n / denom) if denom else None
    missing_rate = (_as_float(h.get("missing_sum"), 0.0) / denom) if denom else None

    ece, brier, psi_top, ks_top, external_missing_rate, artifact_age_sec = _load_external_health(r, model_id)
    if external_missing_rate is not None:
        missing_rate = external_missing_rate

    return SnapshotRow(
        ts_ms=minute * 60000,
        model_id=model_id,
        symbol=symbol,
        mode=mode,
        latency_p50_ms=p50,
        latency_p95_ms=p95,
        latency_p99_ms=p99,
        allow_rate=allow_rate,
        block_rate=block_rate,
        abstain_rate=abstain_rate,
        shadow_rate=shadow_rate,
        error_rate=error_rate,
        ece=ece,
        brier=brier,
        psi_top_json=psi_top,
        ks_top_json=ks_top,
        missing_critical_rate=missing_rate,
        artifact_age_sec=artifact_age_sec,
    )


def _write_snapshots(r: Any, cfg: Cfg, rows: Sequence[SnapshotRow]) -> None:
    if not rows:
        return
    try:
        pipe = r.pipeline()
        ts_ms = _now_ms()
        for row in rows:
            pipe.xadd(cfg.out_stream, row.stream_payload(), maxlen=50000, approximate=True)
        pipe.hset(cfg.out_summary_key, mapping={
            "updated_ts_ms": ts_ms,
            "rows": len(rows),
            "last_minute": max(row.ts_ms for row in rows) // 60000,
        })
        pipe.execute()
    except Exception:
        pass


def _write_db(cfg: Cfg, rows: Sequence[SnapshotRow]) -> None:
    if not cfg.db_enable or not cfg.db_dsn or psycopg2 is None or execute_values is None or Json is None or not rows:
        return
    sql = """

    INSERT INTO ml_model_runtime_1m (
      ts_ms, model_id, symbol, mode,
      latency_p50_ms, latency_p95_ms, latency_p99_ms,
      allow_rate, block_rate, abstain_rate, shadow_rate, error_rate,
      ece, brier, psi_top_json, ks_top_json, missing_critical_rate, artifact_age_sec
    ) VALUES %s
    ON CONFLICT (ts_ms, model_id, symbol) DO UPDATE SET
      mode = EXCLUDED.mode,
      latency_p50_ms = EXCLUDED.latency_p50_ms,
      latency_p95_ms = EXCLUDED.latency_p95_ms,
      latency_p99_ms = EXCLUDED.latency_p99_ms,
      allow_rate = EXCLUDED.allow_rate,
      block_rate = EXCLUDED.block_rate,
      abstain_rate = EXCLUDED.abstain_rate,
      shadow_rate = EXCLUDED.shadow_rate,
      error_rate = EXCLUDED.error_rate,
      ece = EXCLUDED.ece,
      brier = EXCLUDED.brier,
      psi_top_json = EXCLUDED.psi_top_json,
      ks_top_json = EXCLUDED.ks_top_json,
      missing_critical_rate = EXCLUDED.missing_critical_rate,
      artifact_age_sec = EXCLUDED.artifact_age_sec;
    """,
    values = [(
        row.ts_ms, row.model_id, row.symbol, row.mode,
        row.latency_p50_ms, row.latency_p95_ms, row.latency_p99_ms,
        row.allow_rate, row.block_rate, row.abstain_rate, row.shadow_rate, row.error_rate,
        row.ece, row.brier, Json(row.psi_top_json), Json(row.ks_top_json), row.missing_critical_rate, row.artifact_age_sec,
    ) for row in rows]
    try:
        with psycopg2.connect(cfg.db_dsn) as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, values)
            conn.commit()
    except Exception:
        pass


def _finalize_minute(r: Any, cfg: Cfg, minute: int) -> List[SnapshotRow]:
    patt = f"{cfg.bucket_prefix}{minute}:*"
    cursor = 0
    rows: List[SnapshotRow] = []
    keys: List[str] = []
    while True:
        # P-LATENCY-FIX: COUNT reduced from 10000 to 500
        cursor, batch = r.scan(cursor=cursor, match=patt, count=500)
        keys.extend(_as_str(k) for k in batch)
        if int(cursor) == 0:
            break
        time.sleep(0.010)  # yield 10ms between batches to reduce HoL blocking
    if not keys:
        return []
    pipe = r.pipeline()
    for key in keys:
        pipe.hgetall(key)
    hashes = pipe.execute() or []
    for h in hashes:
        if not isinstance(h, dict):
            continue
        rows.append(_build_snapshot_from_hash(r, h, minute))
    # cleanup after successful materialization
    try:
        if keys:
            r.delete(*keys)
    except Exception:
        pass
    return rows


def _update_gauges(rows: Sequence[SnapshotRow], last_minute: int) -> None:
    LAST_FINALIZED_MINUTE.set(float(last_minute))
    by_family: Dict[str, int] = {}
    for row in rows:
        family = row.model_id.split(":", 1)[0]
        by_family[family] = by_family.get(family, 0) + 1
        if row.latency_p95_ms is not None:
            LAT_P95.labels(row.model_id, row.symbol).set(float(row.latency_p95_ms))
        if row.allow_rate is not None:
            ALLOW_RATE.labels(row.model_id, row.symbol).set(float(row.allow_rate))
        if row.error_rate is not None:
            ERR_RATE.labels(row.model_id, row.symbol).set(float(row.error_rate))
    for family, n in by_family.items():
        SNAPSHOT_ROWS.labels(family).set(float(n))
        SNAPSHOTS_WRITTEN.labels(family).inc(float(n))


def main() -> int:
    cfg = load_cfg()
    start_http_server(PORT)
    r = _connect_redis(cfg.redis_url)
    ensure_group(r, cfg)
    while True:
        t0 = time.perf_counter()
        try:
            rows = r.xreadgroup(cfg.group, cfg.consumer, {cfg.stream: ">"}, count=cfg.count, block=cfg.block_ms) or []
            pipe = r.pipeline()
            consumed = 0
            max_ts_ms = 0
            for _stream, msgs in rows:
                for msg_id, fields in msgs:
                    consumed += 1
                    f = fields or {}
                    ts_ms = _as_int(f.get("ts_ms"), _as_int(msg_id.split("-", 1)[0], 0))
                    max_ts_ms = max(max_ts_ms, ts_ms)
                    EVENTS.labels(kind=_as_str(f.get("kind") or "unknown"), status=_status(f)).inc()
                    _ingest_one(pipe, cfg, f, msg_id)
                    pipe.xack(cfg.stream, cfg.group, msg_id)
            if consumed > 0:
                pipe.execute()
                if max_ts_ms > 0:
                    QUEUE_LAG_MS.set(float(max(0, _now_ms() - max_ts_ms)))

            now_min = _minute(_now_ms())
            all_rows: List[SnapshotRow] = []
            mins = _finalizable_minutes(r, cfg, now_min)
            for minute in mins:
                finalized = _finalize_minute(r, cfg, minute)
                if finalized:
                    _write_snapshots(r, cfg, finalized)
                    _write_db(cfg, finalized)
                    _update_gauges(finalized, minute)
                    all_rows.extend(finalized)
            LAST_RUN_TS.set(time.time())
            UP.set(1.0)
        except Exception as e:
            import traceback
            traceback.print_exc()
            UP.set(0.0)
        LOOP_LAT.observe(time.perf_counter() - t0)
        time.sleep(LOOP_SLEEP_S)


if __name__ == "__main__":
    raise SystemExit(main())
