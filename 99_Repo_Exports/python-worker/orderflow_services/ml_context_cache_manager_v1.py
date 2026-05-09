from __future__ import annotations

import json
import os
import time
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from utils.time_utils import get_ny_time_millis

try:
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from core.redis_stream_consumer import SyncRedisStreamHelper
from orderflow_services.context_cache_registry_v1 import ContextCacheRegistryV1, build_cache_observation

RUNS = Counter("ml_context_cache_manager_runs_total", "Context cache manager runs", ["status"])
ENTRIES = Counter("ml_context_cache_entries_total", "Context cache entries observed", ["eligible"])
LAST_RUN = Gauge("ml_context_cache_manager_last_run_ts_seconds", "Last run ts")
LAST_HITS = Gauge("ml_context_cache_last_hits", "Last observed hits")
LAST_PAYLOAD_BYTES = Gauge("ml_context_cache_last_payload_bytes", "Last observed payload bytes")
LOOP_LAT = Histogram("ml_context_cache_manager_loop_seconds", "Loop latency")


def _s(x: Any) -> str:
    if isinstance(x, bytes):
        return x.decode("utf-8", "replace")
    return str(x)


def main() -> None:
    if redis is None:
        raise RuntimeError("redis package is required")
    start_http_server(int(os.getenv("ML_CONTEXT_CACHE_MANAGER_PORT", "9862")))
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=False)
    registry = ContextCacheRegistryV1(redis_url)
    in_stream = os.getenv("ML_ANALYSIS_BATCH_REQUESTS_STREAM", "stream:ml:analysis_batch_requests")
    group = os.getenv("ML_CONTEXT_CACHE_MANAGER_GROUP", "cg:ml_context_cache_manager_v1")
    consumer = os.getenv("HOSTNAME", "ml-context-cache-manager-v1")
    last_hash = os.getenv("ML_CONTEXT_CACHE_LAST_HASH", "metrics:ml:context_cache:last")

    helper = SyncRedisStreamHelper(client=r, group=group, consumer=consumer)
    helper.ensure_groups([in_stream], start_id="0")

    pel_start_id = "0-0"
    while True:
        t0 = time.perf_counter()

        pel_start_id, pending_msgs = helper.claim_pending(
            in_stream, min_idle_ms=5000, count=32, start_id=pel_start_id
        )
        pending_formatted = [(m.msg_id, m.fields) for m in pending_msgs]
        if pending_formatted:
            rows = [[in_stream, pending_formatted]]
        else:
            rows = helper.read({in_stream: ">"}, count=32, block=5000)

        if not rows:
            LAST_RUN.set(time.time())
            continue
        for _, msgs in rows:
            for msg_id, fields in msgs:
                try:
                    data = {_s(k): _s(v) for k, v in fields.items()}
                    payload = json.loads(data.get("payload") or "{}")
                    obs = build_cache_observation(payload)
                    entry = registry.observe(
                        compact_hash=str(obs.get("compact_hash") or payload.get("batch_id") or ""),
                        prompt_version=(obs.get("prompt_version") or "unknown"),
                        policy_version=(obs.get("policy_version") or "unknown"),
                        payload_bytes=int(obs.get("payload_bytes") or 0),
                        ts_ms=int(payload.get("ts_ms") or get_ny_time_millis()),
                    )
                    r.hset(last_hash, mapping={
                        "compact_hash": entry.compact_hash,
                        "prompt_version": entry.prompt_version,
                        "policy_version": entry.policy_version,
                        "hits": entry.hits,
                        "payload_bytes": entry.payload_bytes,
                        "eligible": 1 if entry.eligible else 0,
                        "cache_ref": entry.cache_ref,
                        "last_seen_ms": entry.last_seen_ms,
                    })
                    ENTRIES.labels(eligible="1" if entry.eligible else "0").inc()
                    LAST_HITS.set(entry.hits)
                    LAST_PAYLOAD_BYTES.set(entry.payload_bytes)
                    RUNS.labels(status="ok").inc()
                    helper.ack(in_stream, msg_id)
                except Exception:
                    RUNS.labels(status="err").inc()
                    helper.ack(in_stream, msg_id)
        LAST_RUN.set(time.time())
        LOOP_LAT.observe(max(0.0, time.perf_counter() - t0))


if __name__ == "__main__":
    main()
