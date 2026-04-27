from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Incremental Redis-side rollup for orchestration composite preflight events.

Design intent:
- stop re-scanning the full Redis Stream window for every exporter run
- maintain bounded hourly/daily bucket counters in Redis
- keep dimensions low-cardinality and deterministic for SLO/weekly trend metrics

The module reads incremental events from the ops stream, updates Redis hash buckets,
and persists a cursor. Buckets are safe to read cheaply later by a textfile exporter.
"""

import json
import os
import re
import time
from typing import Any, Iterable, Mapping, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None


STREAM_KEY_DEFAULT = "ops:orchestration:preflight:v1"
CURSOR_KEY_DEFAULT = "metrics:orchestration:preflight:history_rollup:last_id"
STATE_KEY_DEFAULT = "metrics:orchestration:preflight:history_rollup:last"
HOURLY_PREFIX_DEFAULT = "metrics:orchestration:preflight:history:h"
DAILY_PREFIX_DEFAULT = "metrics:orchestration:preflight:history:d"

ALLOWED_SOURCES = {"deploy_lint", "latency_contract", "strategy_research_stats", "research_guard", "unknown"}
ALLOWED_STATUSES = {"ok", "invalid", "block", "unknown"}


def _get_redis() -> Any:
    if redis is None:
        return None
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    try:
        return redis.Redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_text(value: Any, default: str = "") -> str:
    return str(value or default).strip()


def normalize_source(value: Any) -> str:
    raw = _safe_text(value, "unknown").lower().replace("-", "_").replace(" ", "_")
    if raw in ALLOWED_SOURCES:
        return raw
    if raw.startswith("deploy"):
        return "deploy_lint"
    if raw.startswith("latency"):
        return "latency_contract"
    if raw.startswith("research"):
        return "strategy_research_stats" if "strategy_research_stats" in lower or raw.startswith("cfg:strategy_research_stats") else "research_guard"
    return "unknown"


def normalize_status(value: Any) -> str:
    raw = _safe_text(value, "unknown").lower().replace("-", "_")
    if raw in ALLOWED_STATUSES:
        return raw
    if raw.startswith("block"):
        return "block"
    if raw.startswith("invalid"):
        return "invalid"
    if raw.startswith("ok"):
        return "ok"
    return "unknown"


def normalize_reason_code(value: Any, source: str) -> str:
    raw = _safe_text(value, "").lower()
    raw = re.sub(r"[^a-z0-9:_-]+", "_", raw).strip("_:")
    if not raw:
        return f"{source}:none"
    if raw.startswith(("deploy_lint:", "latency_contract:", "strategy_research_stats:", "research_guard:")):
        prefix, rest = raw.split(":", 1)
        source = normalize_source(prefix)
        raw = rest

    source = normalize_source(source)

    def _family(candidates: Iterable[str], fallback: str = "other") -> str:
        for item in candidates:
            if item and item in raw:
                return item
        return fallback

    if source == "deploy_lint":
        fam = _family(("missing_env", "compose", "wrapper", "unit", "env_file", "stale", "invalid", "block"))
    elif source == "latency_contract":
        fam = _family(("slo", "lag", "timeout", "stale", "missing_state", "invalid", "block"))
    elif source in ("research_guard", "strategy_research_stats"):
        fam = _family(("pbo_high", "psr_low", "dsr_low", "report_stale", "missing_state", "invalid", "block"))
    else:
        fam = _family(("stale", "invalid", "block"))
    return f"{source}:{fam}"


def _extract_event_ts_ms(stream_id: str, payload: Mapping[str, Any]) -> int:
    raw = _safe_text(payload.get("ts_ms"))
    if raw.isdigit():
        return int(raw)
    try:
        return int(str(stream_id).split("-", 1)[0])
    except Exception:
        return _now_ms()


def _hour_bucket_start_ms(ts_ms: int) -> int:
    return (int(ts_ms) // 3_600_000) * 3_600_000


def _day_bucket_start_ms(ts_ms: int) -> int:
    return (int(ts_ms) // 86_400_000) * 86_400_000


def _bucket_key(prefix: str, bucket_start_ms: int) -> str:
    return f"{prefix}:{int(bucket_start_ms)}"


def encode_field(*, purpose: str, selected_source: str, decision_status: str, selected_reason_code: str) -> str:
    return json.dumps(
        [purpose, selected_source, decision_status, selected_reason_code],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def decode_field(value: str) -> Tuple[str, str, str, str]:
    arr = json.loads(value)
    if not isinstance(arr, list) or len(arr) != 4:
        raise ValueError("invalid field")
    return (str(arr[0]), str(arr[1]), str(arr[2]), str(arr[3]))


def _event_dims(payload: Mapping[str, Any]) -> Tuple[str, str, str, str]:
    source = normalize_source(payload.get("selected_source") or payload.get("source"))
    status = normalize_status(payload.get("decision_status") or payload.get("status"))
    purpose = _safe_text(payload.get("purpose"), "unknown") or "unknown"
    reason = normalize_reason_code(payload.get("selected_reason_code") or payload.get("reason_code") or payload.get("selected_reason"), source)
    return purpose, source, status, reason


def _bootstrap_cursor(r: Any, stream_key: str, cursor_key: str, state_key: str, skip_existing: bool) -> str:
    existing = _safe_text(r.get(cursor_key))
    if existing:
        return existing

    cursor = "0-0"
    skipped = 0
    if skip_existing:
        latest = r.xrevrange(stream_key, max="+", min="-", count=1)
        if latest:
            cursor = str(latest[0][0])
        skipped = 1
    r.set(cursor_key, cursor)
    try:
        r.hset(
            state_key,
            mapping={
                "last_stream_id": cursor,
                "bootstrap_skipped_existing": str(skipped),
                "last_rollup_ts_ms": str(_now_ms()),
            },
        )
    except Exception:
        pass
    return cursor


def rollup_incremental(
    r: Any,
    *,
    stream_key: str,
    cursor_key: str,
    state_key: str,
    hourly_prefix: str,
    daily_prefix: str,
    batch_size: int = 500,
    hourly_ttl_hours: int = 24 * 45,
    daily_ttl_days: int = 120,
    bootstrap_skip_existing: bool = True,
) -> dict[str, int]:
    cursor = _bootstrap_cursor(r, stream_key, cursor_key, state_key, bootstrap_skip_existing)
    processed_total = 0
    batches = 0
    last_stream_id = cursor
    last_event_ts_ms = 0
    hourly_ttl_s = max(1, int(hourly_ttl_hours) * 3600)
    daily_ttl_s = max(1, int(daily_ttl_days) * 86400)
    prev_processed_total = int(float(_safe_text(r.hget(state_key, "processed_events_total"), "0") or "0"))

    while True:
        rows = r.xread({stream_key: last_stream_id}, count=max(1, int(batch_size)), block=1)
        if not rows:
            break
        stream_rows = rows[0][1] if rows and rows[0] else []
        if not stream_rows:
            break
        batches += 1
        batch_processed = 0
        pipe = r.pipeline(transaction=False)
        for stream_id, payload in stream_rows:
            payload = payload or {}
            ts_ms = _extract_event_ts_ms(str(stream_id), payload)
            purpose, source, status, reason = _event_dims(payload)
            field = encode_field(
                purpose=purpose,
                selected_source=source,
                decision_status=status,
                selected_reason_code=reason,
            )
            hour_key = _bucket_key(hourly_prefix, _hour_bucket_start_ms(ts_ms))
            day_key = _bucket_key(daily_prefix, _day_bucket_start_ms(ts_ms))
            pipe.hincrby(hour_key, field, 1)
            pipe.hincrby(day_key, field, 1)
            pipe.expire(hour_key, hourly_ttl_s)
            pipe.expire(day_key, daily_ttl_s)
            last_stream_id = str(stream_id)
            last_event_ts_ms = ts_ms
            processed_total += 1
            batch_processed += 1

        pipe.set(cursor_key, last_stream_id)
        pipe.hset(
            state_key,
            mapping={
                "last_stream_id": last_stream_id,
                "last_rollup_ts_ms": str(_now_ms()),
                "last_event_ts_ms": str(last_event_ts_ms),
                "processed_events_total": str(prev_processed_total + processed_total),
                "processed_events_last_run": str(batch_processed),
                "batches_last_run": str(batches),
                "bootstrap_skipped_existing": _safe_text(r.hget(state_key, "bootstrap_skipped_existing"), "0"),
            },
        )
        pipe.execute()
        if len(stream_rows) < int(batch_size):
            break

    return {
        "processed": processed_total,
        "batches": batches,
        "last_stream_id": int(last_stream_id.split("-", 1)[0]) if last_stream_id and last_stream_id != "0-0" else 0,
        "last_event_ts_ms": last_event_ts_ms,
    }


def main() -> int:
    r = _get_redis()
    if r is None:
        return 1
    res = rollup_incremental(
        r,
        stream_key=os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_STREAM", STREAM_KEY_DEFAULT),
        cursor_key=os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CURSOR_KEY", CURSOR_KEY_DEFAULT),
        state_key=os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_STATE_KEY", STATE_KEY_DEFAULT),
        hourly_prefix=os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_HOURLY_PREFIX", HOURLY_PREFIX_DEFAULT),
        daily_prefix=os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_DAILY_PREFIX", DAILY_PREFIX_DEFAULT),
        batch_size=int(os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_BATCH_SIZE", "500")),
        hourly_ttl_hours=int(os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_HOURLY_RETENTION_HOURS", str(24 * 45))),
        daily_ttl_days=int(os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_DAILY_RETENTION_DAYS", "120")),
        bootstrap_skip_existing=os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_BOOTSTRAP_SKIP_EXISTING", "1").lower() in ("1", "true", "yes", "on"),
    )
    print(json.dumps(res, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
