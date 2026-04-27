from __future__ import annotations

import os
import time
from prometheus_client import Gauge, start_http_server

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _to_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _to_int(v, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _redis_client():
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(_env("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    except Exception:
        return None


def _read_hash(client, key: str):
    if client is None or not key:
        return {}
    try:
        data = client.hgetall(key)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


UP = Gauge("ofc_ctx_rollout_exporter_up", "1 if rollout exporter loop is alive")
REDIS_OK = Gauge("ofc_ctx_rollout_exporter_redis_ok", "1 if rollout exporter could read Redis")
CURRENT_MODE = Gauge("ofc_ctx_rollout_current_mode", "Current rollout mode one-hot", ["mode"])
DESIRED_MODE = Gauge("ofc_ctx_rollout_desired_mode", "Desired rollout mode one-hot", ["mode"])
BLOCKED = Gauge("ofc_ctx_rollout_blocked", "1 if rollout change is blocked")
LAST_TS = Gauge("ofc_ctx_rollout_last_ts_ms", "Latest rollout summary timestamp")
OBS = Gauge("ofc_ctx_rollout_observations", "Observation count used by rollout controller")
SHADOW_DISAGREE = Gauge("ofc_ctx_rollout_shadow_disagree_rate", "Shadow disagreement rate")
FAIL_OPEN = Gauge("ofc_ctx_rollout_fail_open_rate", "Fail-open rate")
FALLBACK_RATE = Gauge("ofc_ctx_rollout_fallback_rate", "Fallback rate")
BUNDLE_AGE = Gauge("ofc_ctx_rollout_bundle_age_seconds", "Bundle age seconds")
WRITER_LAG = Gauge("ofc_ctx_rollout_writer_lag_seconds", "Writer lag seconds")
ROLLBACK_REQ = Gauge("ofc_ctx_rollout_rollback_requested", "1 if rollback requested")
CANARY_COUNT = Gauge("ofc_ctx_rollout_canary_symbols_count", "Canary symbol count")
RUNTIME_SUMMARY_AGE = Gauge("ofc_ctx_rollout_runtime_summary_age_seconds", "Runtime summary age seconds")
RUNTIME_CHILD_PID = Gauge("ofc_ctx_rollout_runtime_child_pid", "Runtime child pid")
RUNTIME_CHILD_UPTIME = Gauge("ofc_ctx_rollout_runtime_child_uptime_seconds", "Runtime child uptime seconds")
RUNTIME_RESTART_COUNT = Gauge("ofc_ctx_rollout_runtime_restart_count", "Runtime restart count")
RUNTIME_DEFER_ACTIVE = Gauge("ofc_ctx_rollout_runtime_defer_active", "Runtime defer active flag")
RUNTIME_COOLDOWN_REMAINING = Gauge("ofc_ctx_rollout_runtime_cooldown_remaining_seconds", "Runtime cooldown remaining seconds")
RUNTIME_INFO = Gauge("ofc_ctx_rollout_runtime_info", "Runtime overlay/restart labels", ["active_overlay_fingerprint", "last_restart_reason_kind"])


MODES = ("off", "shadow", "tighten_only", "replace_score_veto")


def _publish_mode(gauge, selected: str) -> None:
    selected = str(selected or "").strip().lower()
    for m in MODES:
        gauge.labels(mode=m).set(1.0 if m == selected else 0.0)


def main() -> None:  # pragma: no cover
    port = int(_env("OFC_CTX_ROLLOUT_EXPORTER_PORT", "9848") or 9848)
    interval_s = float(_env("OFC_CTX_ROLLOUT_EXPORTER_INTERVAL_S", "15") or 15)
    key = _env("OFC_CTX_ROLLOUT_SUMMARY_KEY", "metrics:ofc_contextual_rollout:last")
    runtime_key = _env("OFC_CTX_RUNTIME_SUMMARY_KEY", "metrics:ofc_contextual_runtime:last")
    start_http_server(port)
    while True:
        UP.set(1.0)
        client = _redis_client()
        if client is None:
            REDIS_OK.set(0.0)
            time.sleep(interval_s)
            continue
        summary = _read_hash(client, key)
        runtime = _read_hash(client, runtime_key)
        REDIS_OK.set(1.0 if (summary or runtime) else 0.0)
        _publish_mode(CURRENT_MODE, summary.get("current_mode", "shadow"))
        _publish_mode(DESIRED_MODE, summary.get("desired_mode", "shadow"))
        BLOCKED.set(_to_int(summary.get("blocked", 0), 0))
        LAST_TS.set(_to_float(summary.get("ts_ms", 0.0), 0.0))
        OBS.set(_to_float(summary.get("observations", 0.0), 0.0))
        SHADOW_DISAGREE.set(_to_float(summary.get("shadow_disagree_rate", 0.0), 0.0))
        FAIL_OPEN.set(_to_float(summary.get("fail_open_rate", 0.0), 0.0))
        FALLBACK_RATE.set(_to_float(summary.get("fallback_rate", 0.0), 0.0))
        BUNDLE_AGE.set(_to_float(summary.get("bundle_age_seconds", 0.0), 0.0))
        WRITER_LAG.set(_to_float(summary.get("writer_lag_seconds", 0.0), 0.0))
        ROLLBACK_REQ.set(_to_int(summary.get("rollback_requested", 0), 0))
        CANARY_COUNT.set(_to_float(summary.get("canary_symbols_count", 0.0), 0.0))
        runtime_src = runtime or summary
        RUNTIME_SUMMARY_AGE.set(_to_float(runtime_src.get("state_age_seconds", runtime_src.get("runtime_summary_age_seconds", 0.0)), 0.0))
        RUNTIME_CHILD_PID.set(_to_float(runtime_src.get("child_pid", runtime_src.get("runtime_child_pid", 0.0)), 0.0))
        RUNTIME_CHILD_UPTIME.set(_to_float(runtime_src.get("child_uptime_seconds", runtime_src.get("runtime_child_uptime_seconds", 0.0)), 0.0))
        RUNTIME_RESTART_COUNT.set(_to_float(runtime_src.get("restart_count", runtime_src.get("runtime_restart_count", 0.0)), 0.0))
        RUNTIME_DEFER_ACTIVE.set(_to_float(runtime_src.get("defer_active", runtime_src.get("runtime_defer_active", 0.0)), 0.0))
        RUNTIME_COOLDOWN_REMAINING.set(_to_float(runtime_src.get("cooldown_remaining_seconds", runtime_src.get("runtime_cooldown_remaining_seconds", 0.0)), 0.0))
        fp = str(runtime_src.get("active_overlay_fingerprint", runtime_src.get("runtime_active_overlay_fingerprint", "")) or "")[:128]
        rk = str(runtime_src.get("last_restart_reason_kind", runtime_src.get("runtime_last_restart_reason_kind", "unknown")) or "unknown")[:64]
        RUNTIME_INFO.labels(active_overlay_fingerprint=fp, last_restart_reason_kind=rk).set(1.0)
        time.sleep(interval_s)


if __name__ == "__main__":  # pragma: no cover
    main()
