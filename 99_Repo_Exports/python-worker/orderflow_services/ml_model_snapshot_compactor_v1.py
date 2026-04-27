#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Phase 0.2 compact unified model snapshots inside scanner_infra.

Reads control-plane truth from Timescale/Postgres tables created in Phase 0/0.1:
- ml_model_registry
- ml_model_runtime_1m

Produces low-cardinality, per-model compact snapshots to:
- Redis hash  metrics:ml:model_snapshot:<model_id>
- Redis stream stream:ml:model_snapshot
- Redis hash  metrics:ml:model_snapshot:last
- Prometheus exporter metrics

Design goals:
- scanner_infra only
- fail-open
- no hot-path writes/reads
- deterministic aggregation and reason codes for future Vertex/LLM Phase 1
"""

import json
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
    from psycopg2.extras import RealDictCursor  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore
    RealDictCursor = None  # type: ignore


REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
DB_DSN = os.getenv("TRADES_DB_DSN", "")
PORT = int(os.getenv("ML_MODEL_SNAPSHOT_EXPORTER_PORT", "9856"))
INTERVAL_S = float(os.getenv("ML_MODEL_SNAPSHOT_INTERVAL_S", "60"))
OUT_STREAM = os.getenv("ML_MODEL_SNAPSHOT_STREAM", "stream:ml:model_snapshot")
OUT_SUMMARY_KEY = os.getenv("ML_MODEL_SNAPSHOT_SUMMARY_KEY", "metrics:ml:model_snapshot:last")
OUT_KEY_PREFIX = os.getenv("ML_MODEL_SNAPSHOT_KEY_PREFIX", "metrics:ml:model_snapshot:")
LOOKBACK_MIN = int(os.getenv("ML_MODEL_SNAPSHOT_LOOKBACK_MIN", "30"))
STALE_WARN_SEC = float(os.getenv("ML_MODEL_SNAPSHOT_STALE_WARN_SEC", "300"))
STALE_CRIT_SEC = float(os.getenv("ML_MODEL_SNAPSHOT_STALE_CRIT_SEC", "900"))
ERR_WARN = float(os.getenv("ML_MODEL_SNAPSHOT_ERR_WARN", "0.01"))
ERR_CRIT = float(os.getenv("ML_MODEL_SNAPSHOT_ERR_CRIT", "0.05"))
MISS_WARN = float(os.getenv("ML_MODEL_SNAPSHOT_MISS_WARN", "0.01"))
MISS_CRIT = float(os.getenv("ML_MODEL_SNAPSHOT_MISS_CRIT", "0.05"))
LAT_P95_WARN_MS = float(os.getenv("ML_MODEL_SNAPSHOT_LAT_P95_WARN_MS", "5.0"))
LAT_P95_CRIT_MS = float(os.getenv("ML_MODEL_SNAPSHOT_LAT_P95_CRIT_MS", "10.0"))
MAXLEN = int(os.getenv("ML_MODEL_SNAPSHOT_STREAM_MAXLEN", "200000"))


UP = Gauge("ml_model_snapshot_compactor_up", "1 if compactor loop is healthy")
LAST_RUN_TS = Gauge("ml_model_snapshot_compactor_last_run_ts_seconds", "Last successful compactor loop")
MODELS_TOTAL = Gauge("ml_model_snapshot_models_total", "Last snapshot model count", ["family", "status"])
SNAPSHOTS_WRITTEN = Counter("ml_model_snapshot_writes_total", "Snapshot writes", ["family", "status"])
RUNTIME_AGE = Gauge("ml_model_snapshot_runtime_age_sec", "Runtime age seconds", ["model_id"])
LOOP_LAT = Histogram("ml_model_snapshot_compactor_loop_seconds", "Compactor loop duration seconds")
LAST_STATUS_COUNT = Gauge("ml_model_snapshot_status_count", "Last run status counts", ["status"])


@dataclass
class RegistryRow:
    model_id: str
    family: str
    kind: str
    artifact_uri: str
    schema_ver: str
    schema_hash: str
    promotion_state: str
    champion_flag: bool
    owner_service: str
    created_at_ms: int
    promoted_at_ms: int
    artifact_exists: bool = False
    artifact_age_sec: Optional[float] = None
    mode: str = ""
    fail_policy: str = ""
    cfg_source: str = ""


@dataclass
class RuntimeRow:
    ts_ms: int
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


@dataclass
class Snapshot:
    ts_ms: int
    model_id: str
    family: str
    kind: str
    promotion_state: str
    champion_flag: bool
    owner_service: str
    artifact_uri: str
    artifact_exists: bool
    artifact_age_sec: Optional[float]
    schema_ver: str
    schema_hash: str
    latest_runtime_ts_ms: Optional[int]
    runtime_age_sec: Optional[float]
    symbols_seen_n: int
    mode_last: str
    latency_p95_max_ms: Optional[float]
    latency_p99_max_ms: Optional[float]
    allow_rate_avg: Optional[float]
    block_rate_avg: Optional[float]
    abstain_rate_avg: Optional[float]
    shadow_rate_avg: Optional[float]
    error_rate_max: Optional[float]
    ece_max: Optional[float]
    brier_max: Optional[float]
    missing_critical_rate_max: Optional[float]
    psi_top_json: List[str]
    ks_top_json: List[str]
    status: str
    reason_codes_json: List[str]
    hot_symbols_json: List[str]

    def redis_hash(self) -> Dict[str, str]:
        return {
            "schema_version": "1",
            "ts_ms": str(self.ts_ms),
            "model_id": self.model_id,
            "family": self.family,
            "kind": self.kind,
            "promotion_state": self.promotion_state,
            "champion_flag": "1" if self.champion_flag else "0",
            "owner_service": self.owner_service,
            "artifact_uri": self.artifact_uri,
            "artifact_exists": "1" if self.artifact_exists else "0",
            "artifact_age_sec": _fmt_float(self.artifact_age_sec),
            "schema_ver": self.schema_ver,
            "schema_hash": self.schema_hash,
            "latest_runtime_ts_ms": "" if self.latest_runtime_ts_ms is None else str(self.latest_runtime_ts_ms),
            "runtime_age_sec": _fmt_float(self.runtime_age_sec),
            "symbols_seen_n": str(self.symbols_seen_n),
            "mode_last": self.mode_last,
            "latency_p95_max_ms": _fmt_float(self.latency_p95_max_ms),
            "latency_p99_max_ms": _fmt_float(self.latency_p99_max_ms),
            "allow_rate_avg": _fmt_float(self.allow_rate_avg),
            "block_rate_avg": _fmt_float(self.block_rate_avg),
            "abstain_rate_avg": _fmt_float(self.abstain_rate_avg),
            "shadow_rate_avg": _fmt_float(self.shadow_rate_avg),
            "error_rate_max": _fmt_float(self.error_rate_max),
            "ece_max": _fmt_float(self.ece_max),
            "brier_max": _fmt_float(self.brier_max),
            "missing_critical_rate_max": _fmt_float(self.missing_critical_rate_max),
            "psi_top_json": json.dumps(self.psi_top_json, separators=(",", ":")),
            "ks_top_json": json.dumps(self.ks_top_json, separators=(",", ":")),
            "status": self.status,
            "reason_codes_json": json.dumps(self.reason_codes_json, separators=(",", ":")),
            "hot_symbols_json": json.dumps(self.hot_symbols_json, separators=(",", ":")),
        }

    def stream_payload(self) -> Dict[str, str]:
        out = self.redis_hash().copy()
        out["event"] = "ml_model_snapshot_v1"
        return out


def _now_ms() -> int:
    return get_ny_time_millis()


def _fmt_float(v: Optional[float]) -> str:
    return "" if v is None else f"{float(v):.6f}"


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
        if v is None or v == "":
            return d
        return int(float(v))
    except Exception:
        return d


def _as_float(v: Any, d: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or v == "":
            return d
        x = float(v)
        return x
    except Exception:
        return d


def _merge_top_lists(rows: Sequence[RuntimeRow], attr: str, limit: int = 5) -> List[str]:
    seen: List[str] = []
    for row in rows:
        vals = getattr(row, attr, []) or []
        for v in vals:
            if v and v not in seen:
                seen.append(v)
            if len(seen) >= limit:
                return seen
    return seen[:limit]


def _avg(vals: Iterable[Optional[float]]) -> Optional[float]:
    xs = [float(v) for v in vals if v is not None]
    if not xs:
        return None
    return sum(xs) / float(len(xs))


def _max(vals: Iterable[Optional[float]]) -> Optional[float]:
    xs = [float(v) for v in vals if v is not None]
    if not xs:
        return None
    return max(xs)


def _hot_symbols(rows: Sequence[RuntimeRow], limit: int = 3) -> List[str]:
    scored: List[Tuple[float, str]] = []
    for row in rows:
        score = 0.0
        score += float(row.error_rate or 0.0) * 1000.0
        score += float(row.missing_critical_rate or 0.0) * 500.0
        score += float(row.latency_p95_ms or 0.0)
        score += float(row.latency_p99_ms or 0.0) * 0.1
        scored.append((score, row.symbol))
    scored.sort(reverse=True)
    out: List[str] = []
    for _, sym in scored:
        if sym not in out:
            out.append(sym)
        if len(out) >= limit:
            break
    return out


def classify_status(
    *,
    runtime_age_sec: Optional[float],
    error_rate_max: Optional[float],
    missing_critical_rate_max: Optional[float],
    latency_p95_max_ms: Optional[float],
    artifact_exists: bool,
) -> Tuple[str, List[str]]:
    reasons: List[str] = []
    status = "ok"

    if not artifact_exists:
        reasons.append("ARTIFACT_MISSING")
        status = "critical"

    if runtime_age_sec is None:
        reasons.append("NO_RUNTIME")
        status = "warning" if status == "ok" else status
    else:
        if runtime_age_sec >= STALE_CRIT_SEC:
            reasons.append("RUNTIME_STALE_CRIT")
            status = "critical"
        elif runtime_age_sec >= STALE_WARN_SEC:
            reasons.append("RUNTIME_STALE_WARN")
            status = "warning" if status != "critical" else status

    er = float(error_rate_max or 0.0)
    if er >= ERR_CRIT:
        reasons.append("ERROR_RATE_CRIT")
        status = "critical"
    elif er >= ERR_WARN:
        reasons.append("ERROR_RATE_WARN")
        status = "warning" if status != "critical" else status

    mr = float(missing_critical_rate_max or 0.0)
    if mr >= MISS_CRIT:
        reasons.append("MISSING_CRITICAL_CRIT")
        status = "critical"
    elif mr >= MISS_WARN:
        reasons.append("MISSING_CRITICAL_WARN")
        status = "warning" if status != "critical" else status

    lp = float(latency_p95_max_ms or 0.0)
    if lp >= LAT_P95_CRIT_MS:
        reasons.append("LAT_P95_CRIT")
        status = "critical"
    elif lp >= LAT_P95_WARN_MS:
        reasons.append("LAT_P95_WARN")
        status = "warning" if status != "critical" else status

    return status, reasons


def build_snapshot(reg: RegistryRow, runtime_rows: Sequence[RuntimeRow], now_ms: int) -> Snapshot:
    latest_runtime_ts = max((r.ts_ms for r in runtime_rows), default=None)
    runtime_age_sec = None if latest_runtime_ts is None else max(0.0, (now_ms - latest_runtime_ts) / 1000.0)
    status, reasons = classify_status(
        runtime_age_sec=runtime_age_sec,
        error_rate_max=_max(r.error_rate for r in runtime_rows),
        missing_critical_rate_max=_max(r.missing_critical_rate for r in runtime_rows),
        latency_p95_max_ms=_max(r.latency_p95_ms for r in runtime_rows),
        artifact_exists=bool(reg.artifact_exists),
    )
    return Snapshot(
        ts_ms=now_ms,
        model_id=reg.model_id,
        family=reg.family,
        kind=reg.kind,
        promotion_state=reg.promotion_state,
        champion_flag=reg.champion_flag,
        owner_service=reg.owner_service,
        artifact_uri=reg.artifact_uri,
        artifact_exists=reg.artifact_exists,
        artifact_age_sec=reg.artifact_age_sec,
        schema_ver=reg.schema_ver,
        schema_hash=reg.schema_hash,
        latest_runtime_ts_ms=latest_runtime_ts,
        runtime_age_sec=runtime_age_sec,
        symbols_seen_n=len(runtime_rows),
        mode_last=runtime_rows[0].mode if runtime_rows else reg.mode,
        latency_p95_max_ms=_max(r.latency_p95_ms for r in runtime_rows),
        latency_p99_max_ms=_max(r.latency_p99_ms for r in runtime_rows),
        allow_rate_avg=_avg(r.allow_rate for r in runtime_rows),
        block_rate_avg=_avg(r.block_rate for r in runtime_rows),
        abstain_rate_avg=_avg(r.abstain_rate for r in runtime_rows),
        shadow_rate_avg=_avg(r.shadow_rate for r in runtime_rows),
        error_rate_max=_max(r.error_rate for r in runtime_rows),
        ece_max=_max(r.ece for r in runtime_rows),
        brier_max=_max(r.brier for r in runtime_rows),
        missing_critical_rate_max=_max(r.missing_critical_rate for r in runtime_rows),
        psi_top_json=_merge_top_lists(runtime_rows, "psi_top_json", limit=5),
        ks_top_json=_merge_top_lists(runtime_rows, "ks_top_json", limit=5),
        status=status,
        reason_codes_json=reasons,
        hot_symbols_json=_hot_symbols(runtime_rows, limit=3),
    )


def _connect_redis() -> Any:
    if redis is None:
        raise RuntimeError("redis package not available")
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def _connect_db() -> Any:
    if psycopg2 is None:
        raise RuntimeError("psycopg2 package not available")
    return psycopg2.connect(DB_DSN)


def _fetch_registry_rows(conn: Any) -> List[RegistryRow]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              model_id,
              family,
              kind,
              artifact_uri,
              COALESCE(schema_ver, '') AS schema_ver,
              COALESCE(schema_hash, '') AS schema_hash,
              promotion_state,
              champion_flag,
              owner_service,
              created_at_ms,
              COALESCE(promoted_at_ms, 0) AS promoted_at_ms,
              COALESCE(artifact_exists, false) AS artifact_exists,
              artifact_age_sec,
              COALESCE(mode, '') AS mode,
              COALESCE(fail_policy, '') AS fail_policy,
              COALESCE(cfg_source, '') AS cfg_source
            FROM ml_model_registry
            ORDER BY family, kind, model_id
            """
        )
        rows = cur.fetchall() or []
    out: List[RegistryRow] = []
    for row in rows:
        out.append(
            RegistryRow(
                model_id=_as_str(row.get("model_id")),
                family=_as_str(row.get("family")),
                kind=_as_str(row.get("kind")),
                artifact_uri=_as_str(row.get("artifact_uri")),
                schema_ver=_as_str(row.get("schema_ver")),
                schema_hash=_as_str(row.get("schema_hash")),
                promotion_state=_as_str(row.get("promotion_state"), "unknown"),
                champion_flag=bool(row.get("champion_flag")),
                owner_service=_as_str(row.get("owner_service")),
                created_at_ms=_as_int(row.get("created_at_ms"), 0),
                promoted_at_ms=_as_int(row.get("promoted_at_ms"), 0),
                artifact_exists=bool(row.get("artifact_exists")),
                artifact_age_sec=_as_float(row.get("artifact_age_sec"), None),
                mode=_as_str(row.get("mode"), ""),
                fail_policy=_as_str(row.get("fail_policy"), ""),
                cfg_source=_as_str(row.get("cfg_source"), ""),
            )
        )
    return out


def _parse_json_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if x]
    try:
        if isinstance(v, str):
            obj = json.loads(v)
            if isinstance(obj, list):
                return [str(x) for x in obj if x]
    except Exception:
        pass
    return []


def _fetch_runtime_rows(conn: Any, model_id: str, lookback_min: int) -> List[RuntimeRow]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
              ts_ms,
              symbol,
              COALESCE(mode, '') AS mode,
              latency_p50_ms,
              latency_p95_ms,
              latency_p99_ms,
              allow_rate,
              block_rate,
              abstain_rate,
              shadow_rate,
              error_rate,
              ece,
              brier,
              psi_top_json,
              ks_top_json,
              missing_critical_rate,
              artifact_age_sec
            FROM ml_model_runtime_1m
            WHERE model_id = %s
              AND ts_ms >= ((extract(epoch from now()) * 1000)::bigint - (%s * 60 * 1000)::bigint)
            ORDER BY symbol, ts_ms DESC
            """,
            (model_id, lookback_min),
        )
        rows = cur.fetchall() or []
    out: List[RuntimeRow] = []
    for row in rows:
        out.append(
            RuntimeRow(
                ts_ms=_as_int(row.get("ts_ms"), 0),
                symbol=_as_str(row.get("symbol"), "*"),
                mode=_as_str(row.get("mode"), ""),
                latency_p50_ms=_as_float(row.get("latency_p50_ms"), None),
                latency_p95_ms=_as_float(row.get("latency_p95_ms"), None),
                latency_p99_ms=_as_float(row.get("latency_p99_ms"), None),
                allow_rate=_as_float(row.get("allow_rate"), None),
                block_rate=_as_float(row.get("block_rate"), None),
                abstain_rate=_as_float(row.get("abstain_rate"), None),
                shadow_rate=_as_float(row.get("shadow_rate"), None),
                error_rate=_as_float(row.get("error_rate"), None),
                ece=_as_float(row.get("ece"), None),
                brier=_as_float(row.get("brier"), None),
                psi_top_json=_parse_json_list(row.get("psi_top_json")),
                ks_top_json=_parse_json_list(row.get("ks_top_json")),
                missing_critical_rate=_as_float(row.get("missing_critical_rate"), None),
                artifact_age_sec=_as_float(row.get("artifact_age_sec"), None),
            )
        )
    out.sort(key=lambda r: (r.ts_ms, r.symbol), reverse=True)
    return out


def run_once(rds: Any, conn: Any) -> Dict[str, int]:
    now_ms = _now_ms()
    regs = _fetch_registry_rows(conn)
    counts: Dict[str, int] = {"ok": 0, "warning": 0, "critical": 0}
    family_status: Dict[Tuple[str, str], int] = {}
    for reg in regs:
        rt_rows = _fetch_runtime_rows(conn, reg.model_id, LOOKBACK_MIN)
        snap = build_snapshot(reg, rt_rows, now_ms=now_ms)
        key = f"{OUT_KEY_PREFIX}{reg.model_id}"
        try:
            rds.hset(key, mapping=snap.redis_hash())
            rds.xadd(OUT_STREAM, snap.stream_payload(), maxlen=MAXLEN, approximate=True)
        except Exception:
            continue
        counts[snap.status] = counts.get(snap.status, 0) + 1
        fam_key = (snap.family, snap.status)
        family_status[fam_key] = family_status.get(fam_key, 0) + 1
        SNAPSHOTS_WRITTEN.labels(family=snap.family, status=snap.status).inc()
        RUNTIME_AGE.labels(model_id=snap.model_id).set(float(snap.runtime_age_sec or 0.0))

    try:
        summary = {
            "schema_version": "1",
            "ts_ms": str(now_ms),
            "models_total": str(len(regs)),
            "ok_count": str(counts.get("ok", 0)),
            "warning_count": str(counts.get("warning", 0)),
            "critical_count": str(counts.get("critical", 0)),
        }
        rds.hset(OUT_SUMMARY_KEY, mapping=summary)
    except Exception:
        pass

    for status in ("ok", "warning", "critical"):
        LAST_STATUS_COUNT.labels(status=status).set(float(counts.get(status, 0)))
    for (family, status), n in family_status.items():
        MODELS_TOTAL.labels(family=family, status=status).set(float(n))
    return counts


def main() -> None:
    start_http_server(PORT)
    rds = None
    conn = None
    while True:
        t0 = time.perf_counter()
        try:
            UP.set(0)
            if rds is None:
                rds = _connect_redis()
            if conn is None or getattr(conn, "closed", 1):
                conn = _connect_db()
            run_once(rds, conn)
            try:
                conn.commit()
            except Exception:
                pass
            LAST_RUN_TS.set(time.time())
            UP.set(1)
        except Exception:
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
            conn = None
            UP.set(0)
        finally:
            LOOP_LAT.observe(max(0.0, time.perf_counter() - t0))
            time.sleep(INTERVAL_S)


if __name__ == "__main__":  # pragma: no cover
    main()
