#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Phase-0 ML inventory exporter.

Purpose
-------
Builds a low-cardinality inventory of all ML/runtime model families known to the
trade stack and publishes it to:
  1) Redis stream  : stream:ml:model_inventory
  2) Redis hash    : metrics:ml:model_inventory:last
  3) Postgres/Timescale table `ml_model_registry` (optional, fail-open)
  4) Prometheus    : inventory health + model gauges

Design constraints
------------------
- advisory / control-plane only; never blocks trading runtime
- deterministic and low-cardinality payloads
- reads only already existing sources: Redis cfg keys, env, filesystem artifacts
- fail-open on Redis/DB/filesystem errors
"""

import hashlib
import json
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from prometheus_client import Gauge, start_http_server

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import execute_values  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore
    execute_values = None  # type: ignore


DEFAULT_REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
DEFAULT_DB_DSN = os.getenv("TRADES_DB_DSN", "")

STREAM = os.getenv("ML_INVENTORY_STREAM", "stream:ml:model_inventory")
SUMMARY_KEY = os.getenv("ML_INVENTORY_SUMMARY_KEY", "metrics:ml:model_inventory:last")
EXPORTER_PORT = int(os.getenv("ML_INVENTORY_EXPORTER_PORT", "9852"))
INTERVAL_S = float(os.getenv("ML_INVENTORY_EXPORTER_INTERVAL_S", "60"))
STREAM_MAXLEN = int(os.getenv("ML_INVENTORY_STREAM_MAXLEN", "20000"))
DB_ENABLE = os.getenv("ML_INVENTORY_DB_ENABLE", "1") == "1"
HOSTNAME = os.getenv("HOSTNAME", "ml-inventory-exporter")


UP = Gauge("ml_inventory_exporter_up", "1 if last inventory scan succeeded")
LAST_RUN_TS = Gauge("ml_inventory_last_run_ts_seconds", "Last successful inventory scan timestamp")
LAST_DURATION = Gauge("ml_inventory_last_duration_seconds", "Duration of last inventory scan")
MODELS_TOTAL = Gauge("ml_inventory_models_total", "Number of discovered models", ["family", "promotion_state"])
MODEL_PRESENT = Gauge("ml_inventory_model_present", "1 if a model record is present", ["family", "kind", "promotion_state", "champion_flag"])
MODEL_ARTIFACT_AGE = Gauge("ml_inventory_model_artifact_age_seconds", "Artifact age in seconds", ["family", "kind", "promotion_state"])
MODEL_ARTIFACT_EXISTS = Gauge("ml_inventory_model_artifact_exists", "1 if artifact exists", ["family", "kind", "promotion_state"])


@dataclass
class Cfg:
    redis_url: str
    db_dsn: str
    stream: str
    summary_key: str
    interval_s: float
    stream_maxlen: int
    db_enable: bool
    port: int
    owner_service: str


@dataclass
class ModelRecord:
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
    mode: str
    fail_policy: str
    cfg_source: str
    model_run_id: str
    artifact_exists: int
    artifact_age_sec: float

    def stream_payload(self, ts_ms: int) -> Dict[str, Any]:
        return {
            "schema_version": 1,
            "event": "ml_model_inventory",
            "event_time_ms": ts_ms,
            "producer": HOSTNAME,
            **asdict(self),
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


def _load_json(s: Any) -> Dict[str, Any]:
    try:
        if s is None:
            return {}
        if isinstance(s, dict):
            return s
        obj = json.loads(_as_str(s))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _sha16(parts: Sequence[str]) -> str:
    h = hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()
    return h[:16]


def _mtime_age(path: str) -> Tuple[int, float]:
    p = Path(path)
    if not path:
        return 0, 0.0
    try:
        st = p.stat()
        age = max(0.0, time.time() - st.st_mtime)
        return 1, float(age)
    except Exception:
        return 0, 0.0


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=DEFAULT_REDIS_URL,
        db_dsn=DEFAULT_DB_DSN,
        stream=STREAM,
        summary_key=SUMMARY_KEY,
        interval_s=INTERVAL_S,
        stream_maxlen=STREAM_MAXLEN,
        db_enable=DB_ENABLE,
        port=EXPORTER_PORT,
        owner_service=os.getenv("ML_INVENTORY_OWNER_SERVICE", "ml_control_plane_phase0"),
    )


def _connect_redis(url: str):
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _hgetall(r: Any, key: str) -> Dict[str, Any]:
    if r is None:
        return {}
    try:
        return r.hgetall(key) or {}
    except Exception:
        return {}


def _get(r: Any, key: str) -> str:
    if r is None:
        return ""
    try:
        v = r.get(key)
        return _as_str(v)
    except Exception:
        return ""


def _discover_ml_confirm_records(r: Any, owner_service: str) -> List[ModelRecord]:
    out: List[ModelRecord] = []
    ts_ms = _now_ms()

    json_keys: List[Tuple[str, str]] = [
        (os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion"), "champion"),
        (os.getenv("ML_CFG_CHALLENGER_KEY", "cfg:ml_confirm:challenger"), "challenger"),
        ("cfg:ml_confirm:edge_stack_v1:champion", "champion"),
        ("cfg:ml_confirm:edge_stack_v1:challenger", "challenger"),
        ("cfg:ml_confirm:edge_stack_v1:candidate", "candidate"),
        ("cfg:ml_confirm:edge_stack_v1:candidate_v13", "candidate"),
    ]
    seen: set[str] = set()

    for key, state in json_keys:
        cfg = _load_json(_get(r, key))
        if not cfg:
            continue
        kind = _as_str(cfg.get("kind") or cfg.get("model_kind") or "unknown")
        model_path = _as_str(cfg.get("model_path") or cfg.get("path") or "")
        model_ver = _as_str(cfg.get("model_ver") or cfg.get("run_id") or "")
        schema_ver = _as_str(cfg.get("feature_schema_ver") or cfg.get("schema_ver") or cfg.get("feature_set") or "")
        schema_hash = _as_str(cfg.get("feature_cols_hash") or cfg.get("schema_hash") or "")
        mode = _as_str(cfg.get("mode") or os.getenv("ML_CONFIRM_MODE", "OFF"))
        fail_policy = _as_str(cfg.get("fail_policy") or os.getenv("ML_CONFIRM_FAIL_POLICY", "OPEN"))
        champion_flag = state == "champion"
        model_id = f"ml_confirm:{kind}:{model_ver or _sha16([key, model_path, state])}"
        if model_id in seen:
            continue
        seen.add(model_id)
        exists, age = _mtime_age(model_path)
        out.append(ModelRecord(
            model_id=model_id,
            family="ml_confirm",
            kind=kind,
            artifact_uri=model_path,
            schema_ver=schema_ver,
            schema_hash=schema_hash,
            promotion_state=state,
            champion_flag=champion_flag,
            owner_service=owner_service,
            created_at_ms=ts_ms,
            promoted_at_ms=ts_ms if champion_flag else 0,
            mode=mode,
            fail_policy=fail_policy,
            cfg_source=key,
            model_run_id=model_ver,
            artifact_exists=exists,
            artifact_age_sec=age,
        ))

    # Legacy hash fallback
    h = _hgetall(r, os.getenv("ML_CFG_HASH_KEY", "cfg:ml_confirm"))
    if h:
        kind = _as_str(h.get("kind") or h.get("model_kind") or "unknown")
        model_path = _as_str(h.get("model_path") or "")
        model_ver = _as_str(h.get("model_ver") or "")
        state = "legacy"
        model_id = f"ml_confirm:{kind}:{model_ver or _sha16([state, model_path])}"
        if model_id not in seen:
            exists, age = _mtime_age(model_path)
            out.append(ModelRecord(
                model_id=model_id,
                family="ml_confirm",
                kind=kind,
                artifact_uri=model_path,
                schema_ver=_as_str(h.get("feature_schema_ver") or h.get("schema_ver") or ""),
                schema_hash=_as_str(h.get("feature_cols_hash") or h.get("schema_hash") or ""),
                promotion_state=state,
                champion_flag=False,
                owner_service=owner_service,
                created_at_ms=ts_ms,
                promoted_at_ms=0,
                mode=_as_str(h.get("mode") or os.getenv("ML_CONFIRM_MODE", "OFF")),
                fail_policy=_as_str(h.get("fail_policy") or os.getenv("ML_CONFIRM_FAIL_POLICY", "OPEN")),
                cfg_source=os.getenv("ML_CFG_HASH_KEY", "cfg:ml_confirm"),
                model_run_id=model_ver,
                artifact_exists=exists,
                artifact_age_sec=age,
            ))
    return out


def _discover_meta_lr_records(owner_service: str) -> List[ModelRecord]:
    ts_ms = _now_ms()
    paths: List[Tuple[str, str, bool]] = [
        (os.getenv("META_MODEL_CHAMPION_PATH", os.getenv("META_MODEL_PATH", "/var/lib/trade/models/meta_model_v7_champion.json")), "champion", True),
        (os.getenv("META_MODEL_CHALLENGER_PATH", "/var/lib/trade/models/meta_model_v7_challenger.json"), "challenger", False),
    ]
    out: List[ModelRecord] = []
    for path, state, is_champion in paths:
        if not path:
            continue
        exists, age = _mtime_age(path)
        model_id = f"meta_lr:{state}:{_sha16([path, state])}"
        out.append(ModelRecord(
            model_id=model_id,
            family="meta_lr",
            kind="meta_lr",
            artifact_uri=path,
            schema_ver=os.getenv("META_FEATURE_SCHEMA_VER", ""),
            schema_hash="",
            promotion_state=state,
            champion_flag=is_champion,
            owner_service=owner_service,
            created_at_ms=ts_ms,
            promoted_at_ms=ts_ms if is_champion else 0,
            mode=os.getenv("ML_CONFIRM_MODE", "OFF"),
            fail_policy=os.getenv("ML_CONFIRM_FAIL_POLICY", "OPEN"),
            cfg_source="env:META_MODEL_*",
            model_run_id="",
            artifact_exists=exists,
            artifact_age_sec=age,
        ))
    return out


def _discover_ml_scorer_records(owner_service: str) -> List[ModelRecord]:
    ts_ms = _now_ms()
    candidates = [
        (os.getenv("ML_SCORER_V2_MODEL_PATH", "/var/lib/trade/ml_models/scorer_v2/scorer_v2.joblib"), "ml_scorer_v2", "champion", True),
        (os.getenv("ML_SCORER_V3_MODEL_PATH", "/var/lib/trade/ml_models/scorer_v3/scorer_v3.joblib"), "ml_scorer_v3", "challenger", False),
    ]
    out: List[ModelRecord] = []
    for path, kind, state, is_champion in candidates:
        if not path:
            continue
        exists, age = _mtime_age(path)
        out.append(ModelRecord(
            model_id=f"{kind}:{_sha16([path, state])}",
            family="ml_scorer",
            kind=kind,
            artifact_uri=path,
            schema_ver="23f_core",
            schema_hash="",
            promotion_state=state,
            champion_flag=is_champion,
            owner_service=owner_service,
            created_at_ms=ts_ms,
            promoted_at_ms=ts_ms if is_champion else 0,
            mode="OPTIONAL",
            fail_policy="OPEN",
            cfg_source=f"env:{kind.upper()}_MODEL_PATH",
            model_run_id="",
            artifact_exists=exists,
            artifact_age_sec=age,
        ))
    return out


def discover_inventory(cfg: Cfg) -> List[ModelRecord]:
    r = _connect_redis(cfg.redis_url)
    out: List[ModelRecord] = []
    out.extend(_discover_ml_confirm_records(r, cfg.owner_service))
    out.extend(_discover_meta_lr_records(cfg.owner_service))
    out.extend(_discover_ml_scorer_records(cfg.owner_service))
    # deterministic ordering for testing / replayability
    out.sort(key=lambda x: (x.family, x.kind, x.promotion_state, x.artifact_uri))
    return out


def _publish_inventory(r: Any, cfg: Cfg, rows: Sequence[ModelRecord], ts_ms: int) -> None:
    if r is None:
        return
    try:
        pipe = r.pipeline()
        family_counts: Dict[Tuple[str, str], int] = {}
        for row in rows:
            pipe.xadd(cfg.stream, row.stream_payload(ts_ms), maxlen=cfg.stream_maxlen, approximate=True)
            k = (row.family, row.promotion_state)
            family_counts[k] = family_counts.get(k, 0) + 1
        summary = {
            "updated_ts_ms": ts_ms,
            "models_total": len(rows),
            "families_json": json.dumps({f"{fam}:{state}": n for (fam, state), n in sorted(family_counts.items())}, separators=(",", ":")),
            "owner_service": cfg.owner_service,
        }
        pipe.hset(cfg.summary_key, mapping=summary)
        pipe.execute()
    except Exception:
        pass


def _write_db(cfg: Cfg, rows: Sequence[ModelRecord]) -> None:
    if not cfg.db_enable or not cfg.db_dsn or psycopg2 is None or execute_values is None or not rows:
        return
    sql = """
    INSERT INTO ml_model_registry (
      model_id, family, kind, artifact_uri, schema_ver, schema_hash,
      promotion_state, champion_flag, owner_service, created_at_ms, promoted_at_ms,
      artifact_exists, artifact_age_sec, mode, fail_policy, cfg_source
    ) VALUES %s
    ON CONFLICT (model_id) DO UPDATE SET
      family = EXCLUDED.family,
      kind = EXCLUDED.kind,
      artifact_uri = EXCLUDED.artifact_uri,
      schema_ver = EXCLUDED.schema_ver,
      schema_hash = EXCLUDED.schema_hash,
      promotion_state = EXCLUDED.promotion_state,
      champion_flag = EXCLUDED.champion_flag,
      owner_service = EXCLUDED.owner_service,
      artifact_exists = EXCLUDED.artifact_exists,
      artifact_age_sec = EXCLUDED.artifact_age_sec,
      mode = EXCLUDED.mode,
      fail_policy = EXCLUDED.fail_policy,
      cfg_source = EXCLUDED.cfg_source,
      promoted_at_ms = GREATEST(ml_model_registry.promoted_at_ms, EXCLUDED.promoted_at_ms);
    """
    values = [(
        r.model_id, r.family, r.kind, r.artifact_uri, r.schema_ver, r.schema_hash,
        r.promotion_state, r.champion_flag, r.owner_service, r.created_at_ms, r.promoted_at_ms,
        bool(r.artifact_exists), r.artifact_age_sec, r.mode, r.fail_policy, r.cfg_source
    ) for r in rows]
    try:
        with psycopg2.connect(cfg.db_dsn) as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, values)
            conn.commit()
    except Exception:
        pass


def _update_metrics(rows: Sequence[ModelRecord], ts_s: float, duration_s: float) -> None:
    LAST_RUN_TS.set(ts_s)
    LAST_DURATION.set(duration_s)

    # reset gauges by setting current rows; unlabeled old series are tolerated for phase-0
    counts: Dict[Tuple[str, str], int] = {}
    for row in rows:
        counts[(row.family, row.promotion_state)] = counts.get((row.family, row.promotion_state), 0) + 1
        MODEL_PRESENT.labels(
            family=row.family,
            kind=row.kind,
            promotion_state=row.promotion_state,
            champion_flag="1" if row.champion_flag else "0",
        ).set(1.0)
        MODEL_ARTIFACT_EXISTS.labels(row.family, row.kind, row.promotion_state).set(float(row.artifact_exists))
        MODEL_ARTIFACT_AGE.labels(row.family, row.kind, row.promotion_state).set(float(row.artifact_age_sec))
    for (family, state), n in counts.items():
        MODELS_TOTAL.labels(family=family, promotion_state=state).set(float(n))


def main() -> int:
    cfg = load_cfg()
    start_http_server(cfg.port)
    while True:
        t0 = time.time()
        try:
            rows = discover_inventory(cfg)
            ts_ms = _now_ms()
            r = _connect_redis(cfg.redis_url)
            _publish_inventory(r, cfg, rows, ts_ms)
            _write_db(cfg, rows)
            _update_metrics(rows, ts_ms / 1000.0, time.time() - t0)
            UP.set(1.0)
        except Exception:
            UP.set(0.0)
        time.sleep(cfg.interval_s)


if __name__ == "__main__":
    raise SystemExit(main())
