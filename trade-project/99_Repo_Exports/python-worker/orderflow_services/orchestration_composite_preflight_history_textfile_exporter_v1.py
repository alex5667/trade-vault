from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Textfile exporter for orchestration composite preflight rollup buckets.

Reads incremental Redis hourly/daily bucket hashes produced by
orchestration_composite_preflight_history_rollup_v1 and writes bounded
Prometheus metrics for 24h / 7d / 30d windows.
""",
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from orderflow_services.orchestration_composite_preflight_history_rollup_v1 import (
    CURSOR_KEY_DEFAULT,
    DAILY_PREFIX_DEFAULT,
    HOURLY_PREFIX_DEFAULT,
    STATE_KEY_DEFAULT,
    decode_field,
)


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


def _metric_line(name: str, labels: Mapping[str, str], value: float) -> str:
    if labels:
        lab = ",".join(f'{k}="{str(v)}"' for k, v in sorted(labels.items()))
        return f"{name}{{{lab}}} {value}\n"
    return f"{name} {value}\n"


def _bucket_key(prefix: str, bucket_start_ms: int) -> str:
    return f"{prefix}:{int(bucket_start_ms)}"


def _hour_buckets(now_ms: int, hours: int) -> list[int]:
    base = (now_ms // 3_600_000) * 3_600_000
    return [base - i * 3_600_000 for i in range(max(0, int(hours)))]


def _day_buckets(now_ms: int, days: int) -> list[int]:
    base = (now_ms // 86_400_000) * 86_400_000
    return [base - i * 86_400_000 for i in range(max(0, int(days)))]


def _read_bucket_hashes(r: Any, prefix: str, bucket_starts_ms: Iterable[int]) -> list[Mapping[str, str]]:
    pipe = r.pipeline(transaction=False)
    for bucket_start in bucket_starts_ms:
        pipe.hgetall(_bucket_key(prefix, bucket_start))
    rows = pipe.execute()
    return list(rows or [])


def aggregate_windows(r: Any, *, now_ms: int, hourly_prefix: str, daily_prefix: str) -> dict[tuple[str, str, str, str, str], int]:
    result: dict[tuple[str, str, str, str, str], int] = {}
    window_specs = {
        "24h": (hourly_prefix, _hour_buckets(now_ms, 24)),
        "7d": (daily_prefix, _day_buckets(now_ms, 7)),
        "30d": (daily_prefix, _day_buckets(now_ms, 30)),
    }
    for window, (prefix, buckets) in window_specs.items():
        for row in _read_bucket_hashes(r, prefix, buckets):
            for field, raw_value in (row or {}).items():
                try:
                    purpose, source, status, reason = decode_field(field)
                    value = int(float((raw_value or "0")))
                except Exception:
                    continue
                key = (window, purpose, source, status, reason)
                result[key] = result.get(key, 0) + value
    return result


def render_text(r: Any, *, now_ms: int | None = None) -> str:
    now_ms = int(now_ms or _now_ms())
    hourly_prefix = os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_HOURLY_PREFIX", HOURLY_PREFIX_DEFAULT)
    daily_prefix = os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_DAILY_PREFIX", DAILY_PREFIX_DEFAULT)
    state_key = os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_STATE_KEY", STATE_KEY_DEFAULT)
    cursor_key = os.getenv("ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_CURSOR_KEY", CURSOR_KEY_DEFAULT)

    agg = aggregate_windows(r, now_ms=now_ms, hourly_prefix=hourly_prefix, daily_prefix=daily_prefix)
    lines: list[str] = []
    lines.append("# HELP orchestration_composite_preflight_rollup_events_total Incremental orchestration preflight events aggregated from Redis buckets\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_events_total gauge\n")
    for (window, purpose, source, status, reason), value in sorted(agg.items()):
        lines.append(
            _metric_line(
                "orchestration_composite_preflight_rollup_events_total",
                {
                    "window": window,
                    "purpose": purpose,
                    "selected_source": source,
                    "decision_status": status,
                    "selected_reason_code": reason,
                },
                float(value),
            )
        )

    totals: dict[tuple[str, str], int] = {}
    blocks: dict[tuple[str, str], int] = {}
    invalids: dict[tuple[str, str], int] = {}
    for (window, purpose, _source, status, _reason), value in agg.items():
        key = (window, purpose)
        totals[key] = totals.get(key, 0) + value
        if status == "block":
            blocks[key] = blocks.get(key, 0) + value
        if status == "invalid":
            invalids[key] = invalids.get(key, 0) + value

    lines.append("# HELP orchestration_composite_preflight_rollup_total Total aggregated orchestration preflight events by window and purpose\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_total gauge\n")
    for (window, purpose), value in sorted(totals.items()):
        lines.append(_metric_line("orchestration_composite_preflight_rollup_total", {"window": window, "purpose": purpose}, float(value)))

    lines.append("# HELP orchestration_composite_preflight_rollup_block_ratio Block ratio by purpose over window\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_block_ratio gauge\n")
    lines.append("# HELP orchestration_composite_preflight_rollup_invalid_ratio Invalid ratio by purpose over window\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_invalid_ratio gauge\n")
    for (window, purpose), total in sorted(totals.items()):
        if total <= 0:
            continue
        lines.append(
            _metric_line(
                "orchestration_composite_preflight_rollup_block_ratio",
                {"window": window, "purpose": purpose},
                float(blocks.get((window, purpose), 0)) / float(total),
            )
        )
        lines.append(
            _metric_line(
                "orchestration_composite_preflight_rollup_invalid_ratio",
                {"window": window, "purpose": purpose},
                float(invalids.get((window, purpose), 0)) / float(total),
            )
        )

    state = r.hgetall(state_key) or {}
    cursor = (r.get(cursor_key) or "")
    last_rollup_ts_ms = int(float((state.get("last_rollup_ts_ms") or "0") or "0"))
    last_event_ts_ms = int(float((state.get("last_event_ts_ms") or "0") or "0"))
    lag_s = max(0.0, (float(now_ms) - float(last_rollup_ts_ms)) / 1000.0) if last_rollup_ts_ms else 0.0

    lines.append("# HELP orchestration_composite_preflight_rollup_state_present 1 if rollup state exists\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_state_present gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_state_present", {}, 1.0 if state else 0.0))
    lines.append("# HELP orchestration_composite_preflight_rollup_cursor_present 1 if stream cursor exists\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_cursor_present gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_cursor_present", {}, 1.0 if cursor else 0.0))
    lines.append("# HELP orchestration_composite_preflight_rollup_lag_seconds Age of the latest incremental rollup run\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_lag_seconds gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_lag_seconds", {}, lag_s))
    lines.append("# HELP orchestration_composite_preflight_rollup_last_rollup_unixtime Unix time of last incremental rollup\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_last_rollup_unixtime gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_last_rollup_unixtime", {}, float(last_rollup_ts_ms / 1000.0 if last_rollup_ts_ms else 0.0)))
    lines.append("# HELP orchestration_composite_preflight_rollup_last_event_unixtime Unix time of last rolled-up event\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_last_event_unixtime gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_last_event_unixtime", {}, float(last_event_ts_ms / 1000.0 if last_event_ts_ms else 0.0)))
    lines.append("# HELP orchestration_composite_preflight_rollup_last_export_unixtime Unix time of last textfile export\n")
    lines.append("# TYPE orchestration_composite_preflight_rollup_last_export_unixtime gauge\n")
    lines.append(_metric_line("orchestration_composite_preflight_rollup_last_export_unixtime", {}, float(now_ms / 1000.0)))
    return "".join(lines)


def main() -> int:
    r = _get_redis()
    if r is None:
        return 1
    out_path = Path(
        os.getenv(
            "ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORT_PATH",
            "/var/lib/node_exporter/textfile_collector/orchestration_composite_preflight_history_rollup.prom",
        )
    ).expanduser().resolve()
    text = render_text(r)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
