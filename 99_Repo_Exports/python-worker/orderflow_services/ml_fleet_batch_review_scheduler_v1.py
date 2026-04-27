from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
import uuid
from typing import Any, Dict, Iterable, List

from prometheus_client import Counter, Gauge, Histogram, start_http_server

try:
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore


RUNS = Counter("ml_batch_review_scheduler_runs_total", "Batch review scheduler runs", ["status"])
BATCHES = Counter("ml_batch_review_batches_total", "Batch review batches emitted", ["family"])
ITEMS = Counter("ml_batch_review_items_total", "Batch review items emitted", ["family"])
LAST_RUN = Gauge("ml_batch_review_scheduler_last_run_ts_seconds", "Last run ts")
SELECTED = Gauge("ml_batch_review_scheduler_selected_items", "Selected suspicious items count")
BUILD_LAT = Histogram("ml_batch_review_scheduler_build_seconds", "Batch build latency")


def _loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def snapshot_to_batch_item(model_id: str, h: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "model_id": model_id,
        "family": h.get("family", "unknown"),
        "kind": h.get("kind", "unknown"),
        "status": h.get("status", "unknown"),
        "promotion_state": h.get("promotion_state", "unknown"),
        "champion_flag": str(h.get("champion_flag", "0")) in {"1", "true", "True"},
        "latency_p95_max_ms": float(h.get("latency_p95_max_ms", 0.0) or 0.0),
        "error_rate_max": float(h.get("error_rate_max", 0.0) or 0.0),
        "missing_critical_rate_max": float(h.get("missing_critical_rate_max", 0.0) or 0.0),
        "ece_max": float(h.get("ece_max", 0.0) or 0.0),
        "brier_max": float(h.get("brier_max", 0.0) or 0.0),
        "reason_codes": _loads(h.get("reason_codes_json"), []),
        "hot_symbols": _loads(h.get("hot_symbols_json"), []),
        "schema_ver": h.get("schema_ver", ""),
    }


def select_suspicious_snapshots(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    include_ok = str(os.getenv("ML_BATCH_REVIEW_INCLUDE_OK", "0") or "0") == "1"
    for h in rows:
        status = str(h.get("status") or "unknown").lower()
        if include_ok or status in {"warning", "critical"}:
            out.append(h)
    return out


def group_batch_items(items: List[Dict[str, Any]], max_items: int) -> List[List[Dict[str, Any]]]:
    max_items = max(1, int(max_items))
    return [items[i:i + max_items] for i in range(0, len(items), max_items)]


def main() -> None:
    if redis is None:
        raise RuntimeError("redis package is required")
    start_http_server(int(os.getenv("ML_BATCH_REVIEW_SCHEDULER_PORT", "9861")))
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    out_stream = os.getenv("ML_ANALYSIS_BATCH_REQUESTS_STREAM", "stream:ml:analysis_batch_requests")
    every_sec = int(os.getenv("ML_BATCH_REVIEW_EVERY_SEC", "3600") or 3600)
    max_items = int(os.getenv("ML_BATCH_REVIEW_MAX_ITEMS", "10") or 10)
    while True:
        t0 = time.perf_counter()
        try:
            keys = [k for k in r.scan_iter(match="metrics:ml:model_snapshot:*") if not k.endswith(":last")]
            rows = [r.hgetall(k) for k in keys]
            suspicious = select_suspicious_snapshots(rows)
            SELECTED.set(len(suspicious))
            if suspicious:
                items = [snapshot_to_batch_item(str(h.get("model_id") or "unknown"), h) for h in suspicious]
                for chunk in group_batch_items(items, max_items=max_items):
                    family = str(chunk[0].get("family") or "unknown") if chunk else "unknown"
                    ts_ms = get_ny_time_millis()
                    batch_scope = {
                        "families": sorted(list({str(x.get("family") or "unknown") for x in chunk})),
                        "model_ids": [str(x.get("model_id") or "unknown") for x in chunk],
                        "item_count": len(chunk),
                    }
                    payload = {
                        "schema_version": 1,
                        "batch_id": uuid.uuid4().hex,
                        "ts_ms": ts_ms,
                        "task_type": "fleet_batch_triage",
                        "priority": "low",
                        "prompt_version": str(os.getenv("ML_TRIAGE_PROMPT_VERSION", "ml_triage_v1")),
                        "policy_version": str(os.getenv("ML_TRIAGE_POLICY_VERSION", "policy_v1")),
                        "batch_scope_json": batch_scope,
                        "items_json": chunk,
                    }
                    r.xadd(out_stream, {
                        "schema_version": 1,
                        "batch_id": payload["batch_id"],
                        "ts_ms": ts_ms,
                        "task_type": payload["task_type"],
                        "priority": payload["priority"],
                        "family": family,
                        "item_count": len(chunk),
                        "payload": json.dumps(payload, ensure_ascii=False),
                    }, maxlen=int(os.getenv("ML_ANALYSIS_BATCH_REQUESTS_MAXLEN", "50000") or 50000), approximate=True)
                    BATCHES.labels(family=family).inc()
                    ITEMS.labels(family=family).inc(len(chunk))
            RUNS.labels(status="ok").inc()
            LAST_RUN.set(time.time())
        except Exception:
            RUNS.labels(status="err").inc()
        BUILD_LAT.observe(max(0.0, time.perf_counter() - t0))
        time.sleep(max(5, every_sec))


if __name__ == "__main__":
    main()
