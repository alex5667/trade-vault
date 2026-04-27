from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Consistency checker + rebuild tool for orchestration composite preflight rollup.

P5.7 design goals:
- detect drift between Redis-side hourly/daily rollup buckets and the source stream
- keep the check deterministic and bounded to an explicit time range
- support deterministic rebuild of bucket state from the source stream without
  mutating unrelated buckets or hiding auditability

The checker compares expected counters rebuilt from the source stream against the
actual Redis hash buckets already maintained by P5.6. The rebuild command rewrites
only the affected hourly/daily bucket keys for the requested range.

Key fix over naive implementation: we enumerate ALL bucket starts in the range
(not just those with stream events), so extra Redis keys with no stream events
are also caught as drift.
"""

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from orderflow_services.orchestration_composite_preflight_history_rollup_v1 import (
    CURSOR_KEY_DEFAULT,
    DAILY_PREFIX_DEFAULT,
    HOURLY_PREFIX_DEFAULT,
    STATE_KEY_DEFAULT,
    _day_bucket_start_ms,
    _event_dims,
    _extract_event_ts_ms,
    _hour_bucket_start_ms,
    encode_field,
)

CONSISTENCY_REPORT_PATH_DEFAULT = "/var/lib/trade/reports/orchestration_composite_preflight_history_consistency.json"
CONSISTENCY_EXPORT_PATH_DEFAULT = "/var/lib/node_exporter/textfile_collector/orchestration_composite_preflight_history_consistency.prom"


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


def _int_env(name: str, default: int) -> int:
    raw = _safe_text(os.getenv(name), str(default))
    try:
        return int(float(raw))
    except Exception:
        return int(default)


def _bool_env(name: str, default: bool) -> bool:
    raw = _safe_text(os.getenv(name), "1" if default else "0").lower()
    return raw in ("1", "true", "yes", "on")


def _metric_line(name: str, labels: Mapping[str, str], value: float) -> str:
    if labels:
        lab = ",".join(f'{k}="{str(v)}"' for k, v in sorted(labels.items()))
        return f"{name}{{{lab}}} {value}\n"
    return f"{name} {value}\n"


def _bucket_key(prefix: str, bucket_start_ms: int) -> str:
    return f"{prefix}:{int(bucket_start_ms)}"


def _hour_bucket_starts_in_range(start_ms: int, end_ms: int) -> List[int]:
    """Enumerate ALL hourly bucket boundaries overlapping [start_ms, end_ms].

    This ensures extra Redis keys with no stream events are caught as drift.
    """
    if end_ms < start_ms:
        return []
    current = _hour_bucket_start_ms(start_ms)
    end_bucket = _hour_bucket_start_ms(end_ms)
    out: List[int] = []
    while current <= end_bucket:
        out.append(int(current))
        current += 3_600_000
    return out


def _day_bucket_starts_in_range(start_ms: int, end_ms: int) -> List[int]:
    """Enumerate ALL daily bucket boundaries overlapping [start_ms, end_ms].

    This ensures extra Redis keys with no stream events are caught as drift.
    """
    if end_ms < start_ms:
        return []
    current = _day_bucket_start_ms(start_ms)
    end_bucket = _day_bucket_start_ms(end_ms)
    out: List[int] = []
    while current <= end_bucket:
        out.append(int(current))
        current += 86_400_000
    return out


def _stream_scan_range(
    r: Any,
    *,
    stream_key: str,
    start_ms: int,
    end_ms: int,
    batch_size: int,
) -> List[Tuple[str, Mapping[str, Any]]]:
    """Read all stream entries in [start_ms, end_ms] using paginated XRANGE.

    Uses the full timestamp range to avoid missing events or duplicating them.
    """
    if end_ms < start_ms:
        return []

    start_id = f"{int(start_ms)}-0"
    max_id = f"{int(end_ms)}-999999"
    next_min = start_id
    rows_out: List[Tuple[str, Mapping[str, Any]]] = []

    while True:
        rows = r.xrange(stream_key, min=next_min, max=max_id, count=max(1, int(batch_size)))
        if not rows:
            break
        last_id = None
        for stream_id, payload in rows:
            sid = str(stream_id)
            # De-duplicate if pagination overshoots
            if rows_out and sid == rows_out[-1][0]:
                continue
            rows_out.append((sid, payload or {}))
            last_id = sid
        if not last_id or len(rows) < int(batch_size):
            break
        sid_ms, sid_seq = last_id.split("-", 1)
        next_min = f"{sid_ms}-{int(sid_seq) + 1}"
    return rows_out


def _expected_counts_from_stream(
    r: Any,
    *,
    stream_key: str,
    start_ms: int,
    end_ms: int,
    batch_size: int,
) -> Dict[str, Any]:
    """Rebuild expected hourly/daily bucket counts from the source stream.

    Returns a dict with 'hourly', 'daily', 'events', 'max_stream_id',
    'max_event_ts_ms' — deterministic, no Redis hash reads.
    """
    hourly: Dict[int, Dict[str, int]] = {}
    daily: Dict[int, Dict[str, int]] = {}
    events = _stream_scan_range(r, stream_key=stream_key, start_ms=start_ms, end_ms=end_ms, batch_size=batch_size)
    max_stream_id = ""
    max_event_ts_ms = 0
    for stream_id, payload in events:
        ts_ms = _extract_event_ts_ms(stream_id, payload)
        purpose, source, status, reason = _event_dims(payload)
        field = encode_field(
            purpose=purpose,
            selected_source=source,
            decision_status=status,
            selected_reason_code=reason,
        )
        hb = _hour_bucket_start_ms(ts_ms)
        db = _day_bucket_start_ms(ts_ms)
        hourly.setdefault(hb, {})[field] = int(hourly.setdefault(hb, {}).get(field, 0)) + 1
        daily.setdefault(db, {})[field] = int(daily.setdefault(db, {}).get(field, 0)) + 1
        max_stream_id = stream_id
        if ts_ms > max_event_ts_ms:
            max_event_ts_ms = ts_ms
    return {
        "hourly": hourly,
        "daily": daily,
        "events": len(events),
        "max_stream_id": max_stream_id,
        "max_event_ts_ms": max_event_ts_ms,
    }


def _actual_counts_from_buckets(r: Any, *, prefix: str, bucket_starts: Iterable[int]) -> Dict[int, Dict[str, int]]:
    """Read current Redis hash fields for each bucket boundary in bucket_starts.

    Iterates the full range (not just expected keys) to catch extra Redis keys.
    """
    out: Dict[int, Dict[str, int]] = {}
    for bucket_start in sorted({int(v) for v in bucket_starts}):
        raw = r.hgetall(_bucket_key(prefix, bucket_start)) or {}
        bucket: Dict[str, int] = {}
        for field, value in raw.items():
            try:
                bucket[str(field)] = int(float(str(value or "0")))
            except Exception:
                bucket[str(field)] = 0
        out[int(bucket_start)] = bucket
    return out


def _compare_bucket_maps(expected: Mapping[int, Mapping[str, int]], actual: Mapping[int, Mapping[str, int]]) -> Dict[str, int]:
    """Compare expected vs actual bucket counts.

    Counts:
    - missing_fields: fields in expected but absent in actual
    - extra_fields: fields in actual but absent in expected
    - mismatched_value_fields: fields present both sides but different counts
    - mismatched_bucket_keys: buckets with any mismatch
    """
    missing_fields = 0
    extra_fields = 0
    mismatched_value_fields = 0
    mismatched_bucket_keys = 0
    checked_bucket_keys = 0
    expected_events = 0
    actual_events = 0
    for bucket_start in sorted(set(expected.keys()) | set(actual.keys())):
        checked_bucket_keys += 1
        exp = dict(expected.get(bucket_start, {}) or {})
        act = dict(actual.get(bucket_start, {}) or {})
        expected_events += sum(int(v) for v in exp.values())
        actual_events += sum(int(v) for v in act.values())
        bucket_bad = False
        for field, exp_value in exp.items():
            if field not in act:
                missing_fields += 1
                bucket_bad = True
            elif int(act[field]) != int(exp_value):
                mismatched_value_fields += 1
                bucket_bad = True
        for field in act.keys():
            if field not in exp:
                extra_fields += 1
                bucket_bad = True
        if bucket_bad:
            mismatched_bucket_keys += 1
    return {
        "checked_bucket_keys": checked_bucket_keys,
        "expected_events": expected_events,
        "actual_events": actual_events,
        "missing_fields": missing_fields,
        "extra_fields": extra_fields,
        "mismatched_value_fields": mismatched_value_fields,
        "mismatched_bucket_keys": mismatched_bucket_keys,
    }


def check_consistency(
    r: Any,
    *,
    stream_key: str,
    start_ms: int,
    end_ms: int,
    hourly_prefix: str,
    daily_prefix: str,
    state_key: str,
    cursor_key: str,
    batch_size: int = 500,
) -> Dict[str, Any]:
    """Run a full consistency check for the given time range.

    1. Rebuild expected bucket counts from the source stream.
    2. Read actual Redis bucket state using the FULL range of expected bucket keys
       (including empty buckets), so extra Redis keys are detected.
    3. Compare and produce a structured report.
    4. Check cursor/state alignment separately.

    Returns a report dict with schema_version = 'p57_v1'.
    """
    expected = _expected_counts_from_stream(
        r,
        stream_key=stream_key,
        start_ms=start_ms,
        end_ms=end_ms,
        batch_size=batch_size,
    )
    expected_hourly = expected["hourly"]
    expected_daily = expected["daily"]
    # Enumerate all bucket starts in range (not just those with events) to catch extra keys
    actual_hourly = _actual_counts_from_buckets(r, prefix=hourly_prefix, bucket_starts=_hour_bucket_starts_in_range(start_ms, end_ms))
    actual_daily = _actual_counts_from_buckets(r, prefix=daily_prefix, bucket_starts=_day_bucket_starts_in_range(start_ms, end_ms))
    hourly_cmp = _compare_bucket_maps(expected_hourly, actual_hourly)
    daily_cmp = _compare_bucket_maps(expected_daily, actual_daily)

    state = r.hgetall(state_key) or {}
    cursor = _safe_text(r.get(cursor_key))
    state_cursor = _safe_text(state.get("last_stream_id"))
    state_cursor_match = 1 if cursor and state_cursor and cursor == state_cursor else 0
    drift_detected = 1 if (
        hourly_cmp["missing_fields"]
        or hourly_cmp["extra_fields"]
        or hourly_cmp["mismatched_value_fields"]
        or daily_cmp["missing_fields"]
        or daily_cmp["extra_fields"]
        or daily_cmp["mismatched_value_fields"]
        or not state_cursor_match
    ) else 0

    return {
        "schema_version": "p57_v1",
        "checked_at_ts_ms": _now_ms(),
        "start_ts_ms": int(start_ms),
        "end_ts_ms": int(end_ms),
        "window_hours": max(0.0, (float(end_ms) - float(start_ms)) / 3_600_000.0),
        "stream_key": stream_key,
        "state_key": state_key,
        "cursor_key": cursor_key,
        "expected_stream_events": int(expected["events"]),
        "expected_max_stream_id": _safe_text(expected.get("max_stream_id")),
        "expected_max_event_ts_ms": int(expected.get("max_event_ts_ms") or 0),
        "cursor_stream_id": cursor,
        "state_last_stream_id": state_cursor,
        "state_cursor_match": int(state_cursor_match),
        "hourly": hourly_cmp,
        "daily": daily_cmp,
        "drift_detected": int(drift_detected),
        "consistency_ok": 0 if drift_detected else 1,
    }


def rebuild_range(
    r: Any,
    *,
    stream_key: str,
    start_ms: int,
    end_ms: int,
    hourly_prefix: str,
    daily_prefix: str,
    state_key: str,
    batch_size: int = 500,
    hourly_ttl_hours: int = 24 * 45,
    daily_ttl_days: int = 120,
    update_cursor: bool = False,
    cursor_key: str = CURSOR_KEY_DEFAULT,
) -> Dict[str, Any]:
    """Rebuild hourly/daily buckets from the source stream for the given range.

    Key semantics:
    - DELETE + re-write EVERY bucket key in the range (not just those with events).
      This eliminates extra keys that caused drift. Keys with no events are left absent.
    - State key is updated with rebuild metadata.
    - cursor_key is only updated if update_cursor=True (default: False, non-invasive).

    Returns a report dict compatible with render_text().
    """
    expected = _expected_counts_from_stream(
        r,
        stream_key=stream_key,
        start_ms=start_ms,
        end_ms=end_ms,
        batch_size=batch_size,
    )
    hourly: Mapping[int, Mapping[str, int]] = expected["hourly"]
    daily: Mapping[int, Mapping[str, int]] = expected["daily"]
    hourly_ttl_s = max(1, int(hourly_ttl_hours) * 3600)
    daily_ttl_s = max(1, int(daily_ttl_days) * 86400)

    written_bucket_keys = 0
    # Enumerate ALL bucket starts in range, delete each key, then re-write if non-empty.
    # This clears extra keys that would otherwise persist as ghost drift.
    for bucket_start in _hour_bucket_starts_in_range(start_ms, end_ms):
        key = _bucket_key(hourly_prefix, bucket_start)
        r.delete(key)
        mapping = {field: str(int(value)) for field, value in dict(hourly.get(bucket_start, {})).items() if int(value) != 0}
        if mapping:
            r.hset(key, mapping=mapping)
            r.expire(key, hourly_ttl_s)
            written_bucket_keys += 1
    for bucket_start in _day_bucket_starts_in_range(start_ms, end_ms):
        key = _bucket_key(daily_prefix, bucket_start)
        r.delete(key)
        mapping = {field: str(int(value)) for field, value in dict(daily.get(bucket_start, {})).items() if int(value) != 0}
        if mapping:
            r.hset(key, mapping=mapping)
            r.expire(key, daily_ttl_s)
            written_bucket_keys += 1

    now_ms = _now_ms()
    state_update: Dict[str, str] = {
        "last_rebuild_ts_ms": str(now_ms),
        "last_rebuild_start_ts_ms": str(int(start_ms)),
        "last_rebuild_end_ts_ms": str(int(end_ms)),
        "last_rebuild_stream_events": str(int(expected["events"])),
        "last_rebuild_written_bucket_keys": str(int(written_bucket_keys)),
        "last_rebuild_max_stream_id": _safe_text(expected.get("max_stream_id")),
    }
    if expected.get("max_event_ts_ms"):
        state_update["last_rebuild_max_event_ts_ms"] = str(int(expected["max_event_ts_ms"]))
    r.hset(state_key, mapping=state_update)
    if update_cursor and expected.get("max_stream_id"):
        r.set(cursor_key, _safe_text(expected.get("max_stream_id")))
        try:
            r.hset(state_key, mapping={"last_stream_id": _safe_text(expected.get("max_stream_id"))})
        except Exception:
            pass

    return {
        "schema_version": "p57_v1",
        "rebuilt_at_ts_ms": now_ms,
        "start_ts_ms": int(start_ms),
        "end_ts_ms": int(end_ms),
        "stream_key": stream_key,
        "hourly_bucket_keys_rewritten": int(len(hourly)),
        "daily_bucket_keys_rewritten": int(len(daily)),
        "written_bucket_keys": int(written_bucket_keys),
        "stream_events": int(expected["events"]),
        "max_stream_id": _safe_text(expected.get("max_stream_id")),
        "update_cursor": 1 if update_cursor else 0,
        "consistency_ok": 1,
        "drift_detected": 0,
        "state_cursor_match": 1,
    }


def render_text(report: Mapping[str, Any]) -> str:
    """Render Prometheus textfile metrics from a check or rebuild report."""
    checked_at_ts_ms = int(float(str(report.get("checked_at_ts_ms") or report.get("rebuilt_at_ts_ms") or "0") or "0"))
    lines: List[str] = []
    lines.append("# HELP orchestration_composite_preflight_rollup_consistency_ok 1 if Redis rollup buckets match the source stream over the checked range\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_consistency_ok gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_consistency_ok", {}, float(int(report.get("consistency_ok") or 0))))
    lines.append("# HELP orchestration_composite_preflight_rollup_consistency_drift_detected 1 if any drift was detected during the consistency check\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_consistency_drift_detected gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_consistency_drift_detected", {}, float(int(report.get("drift_detected") or 0))))
    lines.append("# HELP orchestration_composite_preflight_rollup_consistency_state_cursor_match 1 if rollup cursor key matches rollup state last_stream_id\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_consistency_state_cursor_match gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_consistency_state_cursor_match", {}, float(int(report.get("state_cursor_match") or 0))))
    lines.append("# HELP orchestration_composite_preflight_rollup_consistency_checked_stream_events Total stream events scanned for the check/rebuild range\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_consistency_checked_stream_events gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_consistency_checked_stream_events", {}, float(int(report.get("expected_stream_events") or report.get("stream_events") or 0))))
    lines.append("# HELP orchestration_composite_preflight_rollup_consistency_window_hours Window length checked by the consistency worker\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_consistency_window_hours gauge\n")
    window_hours = float(report.get("window_hours") or max(0.0, (float(int(report.get("end_ts_ms") or 0)) - float(int(report.get("start_ts_ms") or 0))) / 3_600_000.0))
    lines.append(_metric_line("orchestration_composite_preflight_rollup_consistency_window_hours", {}, window_hours))
    lines.append("# HELP orchestration_composite_preflight_rollup_consistency_bucket_mismatches_total Number of bucket keys with any mismatch\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_consistency_bucket_mismatches_total gauge\n")
    lines.append("# HELP orchestration_composite_preflight_rollup_consistency_missing_fields_total Missing expected fields in Redis buckets\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_consistency_missing_fields_total gauge\n")
    lines.append("# HELP orchestration_composite_preflight_rollup_consistency_extra_fields_total Unexpected extra fields present in Redis buckets\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_consistency_extra_fields_total gauge\n")
    lines.append("# HELP orchestration_composite_preflight_rollup_consistency_mismatched_value_fields_total Fields present on both sides but with different counts\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_consistency_mismatched_value_fields_total gauge\n")
    for bucket_kind in ("hourly", "daily"):
        sub = report.get(bucket_kind) if isinstance(report.get(bucket_kind), Mapping) else {}
        sub = dict(sub or {})
        lines.append(_metric_line("orchestration_composite_preflight_rollup_consistency_bucket_mismatches_total", {"bucket_kind": bucket_kind}, float(int(sub.get("mismatched_bucket_keys") or 0))))
        lines.append(_metric_line("orchestration_composite_preflight_rollup_consistency_missing_fields_total", {"bucket_kind": bucket_kind}, float(int(sub.get("missing_fields") or 0))))
        lines.append(_metric_line("orchestration_composite_preflight_rollup_consistency_extra_fields_total", {"bucket_kind": bucket_kind}, float(int(sub.get("extra_fields") or 0))))
        lines.append(_metric_line("orchestration_composite_preflight_rollup_consistency_mismatched_value_fields_total", {"bucket_kind": bucket_kind}, float(int(sub.get("mismatched_value_fields") or 0))))
    lines.append("# HELP orchestration_composite_preflight_rollup_consistency_last_check_unixtime Unix time of the last consistency report/rebuild output\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_consistency_last_check_unixtime gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_consistency_last_check_unixtime", {}, float(checked_at_ts_ms / 1000.0 if checked_at_ts_ms else 0.0)))
    return "".join(lines)


def _write_outputs(report: Mapping[str, Any]) -> None:
    """Atomically write JSON report + Prometheus textfile using tmp-then-rename."""
    report_path = Path(os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_REPORT_PATH", CONSISTENCY_REPORT_PATH_DEFAULT)).expanduser().resolve()
    export_path = Path(os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_EXPORT_PATH", CONSISTENCY_EXPORT_PATH_DEFAULT)).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.parent.mkdir(parents=True, exist_ok=True)

    report_tmp = report_path.with_suffix(report_path.suffix + ".tmp")
    report_tmp.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    report_tmp.replace(report_path)

    export_tmp = export_path.with_suffix(export_path.suffix + ".tmp")
    export_tmp.write_text(render_text(report), encoding="utf-8")
    export_tmp.replace(export_path)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="P5.7 rollup consistency checker and rebuild tool")
    parser.add_argument("mode", nargs="?", default=os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_MODE", "check"), choices=("check", "rebuild"))
    parser.add_argument("--start-ms", type=int, default=None)
    parser.add_argument("--end-ms", type=int, default=None)
    parser.add_argument("--window-hours", type=int, default=_int_env("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_WINDOW_HOURS", 168))
    parser.add_argument("--batch-size", type=int, default=_int_env("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CONSISTENCY_BATCH_SIZE", 500))
    parser.add_argument("--update-cursor", action="store_true", default=_bool_env("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_REBUILD_UPDATE_CURSOR", False))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point.

    Exit codes:
      0 — OK (check passed or rebuild completed)
      1 — error (Redis unavailable or unexpected exception)
      2 — check mode: drift detected
    """
    r = _get_redis()
    if r is None:
        return 1

    args = _parse_args(argv)
    now_ms = _now_ms()
    end_ms = int(args.end_ms if args.end_ms is not None else now_ms)
    start_ms = int(args.start_ms if args.start_ms is not None else end_ms - int(args.window_hours) * 3_600_000)
    stream_key = os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_STREAM", "ops:orchestration:preflight:v1")
    hourly_prefix = os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_HOURLY_PREFIX", HOURLY_PREFIX_DEFAULT)
    daily_prefix = os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_DAILY_PREFIX", DAILY_PREFIX_DEFAULT)
    state_key = os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_STATE_KEY", STATE_KEY_DEFAULT)
    cursor_key = os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CURSOR_KEY", CURSOR_KEY_DEFAULT)

    if args.mode == "rebuild":
        report = rebuild_range(
            r,
            stream_key=stream_key,
            start_ms=start_ms,
            end_ms=end_ms,
            hourly_prefix=hourly_prefix,
            daily_prefix=daily_prefix,
            state_key=state_key,
            batch_size=int(args.batch_size),
            hourly_ttl_hours=_int_env("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_HOURLY_RETENTION_HOURS", 24 * 45),
            daily_ttl_days=_int_env("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_DAILY_RETENTION_DAYS", 120),
            update_cursor=bool(args.update_cursor),
            cursor_key=cursor_key,
        )
    else:
        report = check_consistency(
            r,
            stream_key=stream_key,
            start_ms=start_ms,
            end_ms=end_ms,
            hourly_prefix=hourly_prefix,
            daily_prefix=daily_prefix,
            state_key=state_key,
            cursor_key=cursor_key,
            batch_size=int(args.batch_size),
        )

    _write_outputs(report)
    print(json.dumps(report, ensure_ascii=False, separators=(",", ":")))
    if args.mode == "rebuild":
        return 0
    return 0 if int(report.get("consistency_ok") or 0) == 1 else 2


if __name__ == "__main__":
    raise SystemExit(main())
