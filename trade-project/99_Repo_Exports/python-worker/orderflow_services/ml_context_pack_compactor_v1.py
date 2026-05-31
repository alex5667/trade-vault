from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from core.redis_stream_consumer import SyncRedisStreamHelper
from utils.time_utils import get_ny_time_millis
import contextlib

try:
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore


RUNS = Counter("ml_context_pack_compactor_runs_total", "Compactor runs", ["status"])
PACKS = Counter("ml_context_pack_compactor_packs_total", "Compacted packs built", ["family"])
INPUT_BYTES = Histogram("ml_context_pack_input_bytes", "Input request payload bytes")
OUTPUT_BYTES = Histogram("ml_context_pack_output_bytes", "Compacted request payload bytes")
LAST_RUN = Gauge("ml_context_pack_compactor_last_run_ts_seconds", "Last run ts")
QUEUE_LAG_MS = Gauge("ml_context_pack_compactor_queue_lag_ms", "Queue lag")


def _now_ms() -> int:
    return get_ny_time_millis()


def _sha16(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def compact_request(req: dict[str, Any]) -> dict[str, Any]:
    """Keep only stable, high-signal fields for deterministic triage.""",
    payload = req.get("payload") if isinstance(req.get("payload"), dict) else req
    snapshot = payload.get("model_snapshot") or {}
    training = payload.get("training") or {}
    out = {
        "schema_version": 1,
        "request_id": str(req.get("request_id") or req.get("id") or _sha16(req)),
        "ts_ms": int(req.get("ts_ms") or _now_ms()),
        "task_type": (req.get("task_type") or "root_cause_degradation"),
        "priority": (req.get("priority") or "normal"),
        "scope": {
            "model_id": snapshot.get("model_id"),
            "family": snapshot.get("family"),
            "kind": snapshot.get("kind"),
            "symbols": snapshot.get("hot_symbols_json") or [],
        },
        "context": {
            "status": snapshot.get("status"),
            "reason_codes": snapshot.get("reason_codes_json") or [],
            "latency_p95_max_ms": snapshot.get("latency_p95_max_ms"),
            "error_rate_max": snapshot.get("error_rate_max"),
            "missing_critical_rate_max": snapshot.get("missing_critical_rate_max"),
            "ece_max": snapshot.get("ece_max"),
            "brier_max": snapshot.get("brier_max"),
            "psi_top_json": snapshot.get("psi_top_json") or [],
            "ks_top_json": snapshot.get("ks_top_json") or [],
            "promotion_state": snapshot.get("promotion_state"),
            "champion_flag": snapshot.get("champion_flag"),
            "schema_ver": snapshot.get("schema_ver"),
            "training": {
                "run_id": training.get("run_id"),
                "sample_n": training.get("sample_n"),
                "pos_rate": training.get("pos_rate"),
                "metrics_json": training.get("metrics_json") or {},
                "promotion_state": training.get("promotion_state"),
            }
        }
    }
    out["compact_hash"] = _sha16(out)
    out["prompt_version"] = os.getenv("ML_TRIAGE_PROMPT_VERSION", "ml_triage_v1")
    out["policy_version"] = os.getenv("ML_TRIAGE_POLICY_VERSION", "policy_v1")
    return out


def main() -> None:
    if redis is None:
        raise RuntimeError("redis package is required")
    start_http_server(int(os.getenv("ML_CONTEXT_PACK_COMPACTOR_PORT", "9859")))
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    in_stream = os.getenv("ML_ANALYSIS_REQUESTS_STREAM", "stream:ml:analysis_requests")
    out_stream = os.getenv("ML_ANALYSIS_REQUESTS_COMPACT_STREAM", "stream:ml:analysis_requests_compact")
    group = os.getenv("ML_CONTEXT_PACK_GROUP", "cg:ml_context_pack_compactor")
    consumer = os.getenv("ML_CONTEXT_PACK_CONSUMER", "ml-context-pack-1")

    helper = SyncRedisStreamHelper(client=r, group=group, consumer=consumer)
    helper.ensure_groups([in_stream], start_id="0")

    pel_start_id = "0-0"
    while True:
        pel_start_id, pending_msgs = helper.claim_pending(
            in_stream, min_idle_ms=5000, count=64, start_id=pel_start_id
        )
        pending_formatted = [(m.msg_id, m.fields) for m in pending_msgs]

        if pending_formatted:
            rows = [[in_stream, pending_formatted]]
        else:
            rows = helper.read({in_stream: ">"}, count=64, block=5000)

        if not rows:
            LAST_RUN.set(time.time())
            continue
        for _, msgs in rows:
            for msg_id, fields in msgs:
                try:
                    req = json.loads(fields.get("payload", "{}")) if "payload" in fields else fields
                    raw = json.dumps(req, ensure_ascii=False)
                    INPUT_BYTES.observe(len(raw.encode("utf-8")))
                    compact = compact_request(req)
                    family = str((compact.get("scope") or {}).get("family") or "unknown")
                    packed = json.dumps(compact, ensure_ascii=False)
                    OUTPUT_BYTES.observe(len(packed.encode("utf-8")))
                    r.xadd(out_stream, {"payload": packed}, maxlen=int(os.getenv("ML_ANALYSIS_REQUESTS_COMPACT_MAXLEN", "200000")), approximate=True)
                    PACKS.labels(family=family).inc()
                    RUNS.labels(status="ok").inc()
                    try:
                        q_lag = max(0, _now_ms() - int(compact.get("ts_ms") or _now_ms()))
                        QUEUE_LAG_MS.set(q_lag)
                    except Exception:
                        pass
                except Exception:
                    RUNS.labels(status="err").inc()
                finally:
                    with contextlib.suppress(Exception):
                        helper.ack(in_stream, msg_id)
                    LAST_RUN.set(time.time())


if __name__ == "__main__":  # pragma: no cover
    main()

