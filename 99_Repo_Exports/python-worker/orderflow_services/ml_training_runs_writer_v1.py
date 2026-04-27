#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Phase 0.1 ML training-runs writer.

Normalizes existing training summary hashes/files into a single control-plane stream
and Timescale/Postgres table (`ml_training_runs`). The worker is fail-open and only
writes when source fingerprints change.
"""

import hashlib
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
    from psycopg2.extras import Json, execute_values  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore
    Json = None  # type: ignore
    execute_values = None  # type: ignore


DEFAULT_SOURCE_MAP = {
    "edge_stack_v1": [
        "metrics:edge_stack_train:last",
        "metrics:edge_stack_train_v13:last",
    ],
    "meta_lr": [
        "metrics:meta_model_lr_train:last",
        "metrics:meta_model_train:last",
        "metrics:meta_model_v9_train:last",
    ],
    "ml_scorer_v2": [
        "metrics:ml_scorer_train:last",
        "metrics:ml_scorer_v2_train:last",
    ],
    "ml_scorer_v3": [
        "metrics:ml_scorer_v3_train:last",
    ],
    "confidence_cal": [
        "metrics:confidence_calibration:last",
        "metrics:confidence_calibration_v2:last",
    ],
}


STREAM = os.getenv("ML_TRAINING_RUNS_STREAM", "stream:ml:training_runs")
SUMMARY_KEY = os.getenv("ML_TRAINING_RUNS_SUMMARY_KEY", "metrics:ml:training_runs:last")
STATE_KEY = os.getenv("ML_TRAINING_RUNS_STATE_KEY", "metrics:ml:training_runs_writer:state")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
DB_DSN = os.getenv("TRADES_DB_DSN", "")
DB_ENABLE = os.getenv("ML_TRAINING_RUNS_DB_ENABLE", "1") == "1"
PORT = int(os.getenv("ML_TRAINING_RUNS_EXPORTER_PORT", "9854"))
INTERVAL_S = float(os.getenv("ML_TRAINING_RUNS_INTERVAL_S", "60"))
SOURCE_MAP_JSON = os.getenv("ML_TRAINING_SOURCE_MAP_JSON", "")
MAXLEN = int(os.getenv("ML_TRAINING_RUNS_STREAM_MAXLEN", "100000"))


UP = Gauge("ml_training_runs_writer_up", "1 if training-runs writer loop is healthy")
LAST_RUN_TS = Gauge("ml_training_runs_writer_last_run_ts_seconds", "Last successful loop timestamp")
ROWS_WRITTEN = Counter("ml_training_runs_rows_written_total", "Rows written", ["family"])
ROWS_SKIPPED = Counter("ml_training_runs_rows_skipped_total", "Rows skipped due to unchanged fingerprint", ["family"])
SOURCES_FOUND = Gauge("ml_training_runs_sources_found", "Current source keys found", ["family"])
SOURCES_MISSING = Gauge("ml_training_runs_sources_missing", "Current source keys missing", ["family"])
LOOP_LAT = Histogram("ml_training_runs_writer_loop_seconds", "Loop duration seconds")
LAST_SUCCESS = Gauge("ml_training_runs_last_success", "1 if last run completed with at least one source scan")


@dataclass
class TrainingRunRow:
    training_run_id: str
    ts_ms: int
    family: str
    kind: str
    model_id: str
    status: str
    metrics_json: Dict[str, Any]
    artifact_uri: str
    notes_json: Dict[str, Any]

    def stream_payload(self) -> Dict[str, Any]:
        return {
            "schema_version": 1,
            "event": "ml_training_run_v1",
            "training_run_id": self.training_run_id,
            "ts_ms": int(self.ts_ms),
            "family": self.family,
            "kind": self.kind,
            "model_id": self.model_id,
            "status": self.status,
            "artifact_uri": self.artifact_uri,
            "metrics_json": json.dumps(self.metrics_json, separators=(",", ":"), ensure_ascii=False),
            "notes_json": json.dumps(self.notes_json, separators=(",", ":"), ensure_ascii=False),
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


def _load_source_map() -> Dict[str, List[str]]:
    if not SOURCE_MAP_JSON.strip():
        return {k: list(v) for k, v in DEFAULT_SOURCE_MAP.items()}
    try:
        obj = json.loads(SOURCE_MAP_JSON)
        out: Dict[str, List[str]] = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, list):
                    out[str(k)] = [_as_str(x) for x in v if _as_str(x)]
        return out or {k: list(v) for k, v in DEFAULT_SOURCE_MAP.items()}
    except Exception:
        return {k: list(v) for k, v in DEFAULT_SOURCE_MAP.items()}


def _sha1_json(obj: Mapping[str, Any]) -> str:
    blob = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


def _clean_mapping(m: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in m.items():
        ks = _as_str(k)
        vs = _as_str(v)
        if ks:
            out[ks] = vs
    return out


def _artifact_uri(m: Mapping[str, Any]) -> str:
    for key in (
        "artifact_uri",
        "model_path",
        "champion_path",
        "candidate_path",
        "report_json",
        "output_json",
        "out_path",
        "status_file",
    ):
        val = _as_str(m.get(key), "")
        if val:
            return val
    return ""


def _pick_run_id(m: Mapping[str, Any], family: str, source_key: str) -> str:
    for key in ("training_run_id", "run_id", "train_run_id", "model_ver", "version", "updated_ts_ms", "ts_ms"):
        val = _as_str(m.get(key), "")
        if val:
            return val
    return hashlib.sha1(f"{family}|{source_key}|{json.dumps(_clean_mapping(m), sort_keys=True)}".encode("utf-8")).hexdigest()[:16]


def _normalize_row(family: str, source_key: str, src: Mapping[str, Any]) -> TrainingRunRow:
    clean = _clean_mapping(src)
    ts_ms = 0
    for key in ("ts_ms", "updated_ts_ms", "train_ts_ms", "finished_ts_ms", "created_ts_ms"):
        ts_ms = _as_int(clean.get(key), 0)
        if ts_ms > 0:
            break
    if ts_ms <= 0:
        ts_ms = _now_ms()

    kind = _as_str(clean.get("kind"), family)
    run_id = _pick_run_id(clean, family, source_key)
    training_run_id = f"{family}:{run_id}"
    model_id = _as_str(clean.get("model_id"), f"{kind}:{run_id}")
    status = _as_str(clean.get("status"), "ok")
    artifact_uri = _artifact_uri(clean)
    metrics_json = dict(clean)
    notes_json: Dict[str, Any] = {
        "source_key": source_key,
        "schema_ver": _as_str(clean.get("schema_ver") or clean.get("feature_schema_ver"), ""),
        "schema_hash": _as_str(clean.get("schema_hash") or clean.get("feature_cols_hash"), ""),
        "sample_n": _as_int(clean.get("sample_n") or clean.get("n") or clean.get("joined_n"), 0),
        "pos_rate": _as_str(clean.get("pos_rate"), ""),
        "promotion_state": _as_str(clean.get("promotion_state") or clean.get("state"), ""),
    }
    return TrainingRunRow(
        training_run_id=training_run_id,
        ts_ms=ts_ms,
        family=family,
        kind=kind,
        model_id=model_id,
        status=status,
        metrics_json=metrics_json,
        artifact_uri=artifact_uri,
        notes_json=notes_json,
    )


def _db_upsert(rows: Sequence[TrainingRunRow]) -> int:
    if not rows or not DB_ENABLE or not DB_DSN or psycopg2 is None or execute_values is None:
        return 0
    conn = None
    try:
        conn = psycopg2.connect(DB_DSN)
        conn.autocommit = True
        cur = conn.cursor()
        sql = """
        INSERT INTO ml_training_runs (
          training_run_id, ts_ms, family, kind, model_id, status, metrics_json, artifact_uri, notes_json
        ) VALUES %s
        ON CONFLICT (training_run_id) DO UPDATE SET
          ts_ms = EXCLUDED.ts_ms,
          family = EXCLUDED.family,
          kind = EXCLUDED.kind,
          model_id = EXCLUDED.model_id,
          status = EXCLUDED.status,
          metrics_json = EXCLUDED.metrics_json,
          artifact_uri = EXCLUDED.artifact_uri,
          notes_json = EXCLUDED.notes_json
        """
        values = [
            (
                r.training_run_id,
                int(r.ts_ms),
                r.family,
                r.kind,
                r.model_id,
                r.status,
                Json(r.metrics_json) if Json is not None else json.dumps(r.metrics_json),
                r.artifact_uri,
                Json(r.notes_json) if Json is not None else json.dumps(r.notes_json),
            )
            for r in rows
        ]
        execute_values(cur, sql, values, page_size=min(100, len(values)))
        cur.close()
        return len(rows)
    except Exception:
        return 0
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


class Writer:
    def __init__(self, cli: Any):
        self.r = cli
        self.source_map = _load_source_map()

    def _get_state_fp(self, source_key: str) -> str:
        try:
            return _as_str(self.r.hget(STATE_KEY, source_key), "")
        except Exception:
            return ""

    def _set_state_fp(self, source_key: str, fp: str) -> None:
        try:
            self.r.hset(STATE_KEY, mapping={source_key: fp, "updated_ts_ms": str(_now_ms())})
        except Exception:
            pass

    def scan(self) -> Tuple[int, int, int]:
        rows: List[TrainingRunRow] = []
        found_total = 0
        scanned_total = 0
        for family, keys in self.source_map.items():
            found = 0
            missing = 0
            for source_key in keys:
                scanned_total += 1
                try:
                    raw = self.r.hgetall(source_key) or {}
                except Exception:
                    raw = {}
                if not raw:
                    missing += 1
                    continue
                found += 1
                found_total += 1
                row = _normalize_row(family, source_key, raw)
                fp_obj = {
                    "training_run_id": row.training_run_id,
                    "status": row.status,
                    "artifact_uri": row.artifact_uri,
                    "metrics_json": row.metrics_json,
                    "notes_json": row.notes_json,
                }
                fp = _sha1_json(fp_obj)
                prev = self._get_state_fp(source_key)
                if fp == prev:
                    ROWS_SKIPPED.labels(family=family).inc()
                    continue
                rows.append(row)
                self._set_state_fp(source_key, fp)
            SOURCES_FOUND.labels(family=family).set(float(found))
            SOURCES_MISSING.labels(family=family).set(float(missing))

        if rows:
            _db_upsert(rows)
            for row in rows:
                try:
                    self.r.xadd(STREAM, row.stream_payload(), maxlen=MAXLEN, approximate=True)
                except Exception:
                    pass
                try:
                    self.r.hset(
                        SUMMARY_KEY,
                        mapping={
                            "updated_ts_ms": str(_now_ms()),
                            "last_training_run_id": row.training_run_id,
                            "last_family": row.family,
                            "last_kind": row.kind,
                            "last_status": row.status,
                            "last_artifact_uri": row.artifact_uri,
                        },
                    )
                except Exception:
                    pass
                ROWS_WRITTEN.labels(family=row.family).inc()
        LAST_SUCCESS.set(1.0 if scanned_total > 0 else 0.0)
        return len(rows), found_total, scanned_total


def main() -> int:
    if redis is None:
        raise RuntimeError("redis-py is required")
    cli = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    start_http_server(PORT)
    writer = Writer(cli)
    while True:
        t0 = time.perf_counter()
        try:
            UP.set(1)
            writer.scan()
            LAST_RUN_TS.set(time.time())
        except Exception:
            UP.set(0)
        LOOP_LAT.observe(max(0.0, time.perf_counter() - t0))
        time.sleep(INTERVAL_S)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
