from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from prometheus_client import Gauge, start_http_server

from utils.time_utils import get_ny_time_millis

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger(__name__)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default




def _redis_client(redis_url: str):
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(redis_url, decode_responses=True)
    except Exception:
        return None


def _read_hash(client: Any, key: str) -> dict[str, Any]:
    if client is None or not key:
        return {}
    try:
        data = client.hgetall(key)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _read_json(path: str) -> dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _now_ms() -> int:
    return get_ny_time_millis()


def _age_seconds(ts_ms: Any) -> float:
    ts = _to_int(ts_ms, 0)
    if ts <= 0:
        return 0.0
    return max(0.0, (_now_ms() - ts) / 1000.0)


def _uptime_seconds(start_ts_ms: Any) -> float:
    ts = _to_int(start_ts_ms, 0)
    if ts <= 0:
        return 0.0
    return max(0.0, (_now_ms() - ts) / 1000.0)


def _cooldown_remaining_seconds(cooldown_until_ts_ms: Any) -> float:
    ts = _to_int(cooldown_until_ts_ms, 0)
    if ts <= 0:
        return 0.0
    return max(0.0, (ts - _now_ms()) / 1000.0)


def _str(v: Any) -> str:
    return (v or "").strip()


UP = Gauge('ofc_ctx_runtime_reloader_exporter_up', '1 if runtime reloader exporter loop is running')
STATE_PRESENT = Gauge('ofc_ctx_runtime_reloader_state_present', '1 if runtime reloader state file exists and parses')
STATE_AGE_SECONDS = Gauge('ofc_ctx_runtime_reloader_state_age_seconds', 'Age of runtime reloader state update')
CHILD_PID = Gauge('ofc_ctx_runtime_reloader_child_pid', 'Current runtime child pid')
CHILD_UPTIME_SECONDS = Gauge('ofc_ctx_runtime_reloader_child_uptime_seconds', 'Current runtime child uptime in seconds')
RESTART_COUNT = Gauge('ofc_ctx_runtime_reloader_restart_count', 'Runtime reloader restart count since start')
LAST_EXIT_CODE = Gauge('ofc_ctx_runtime_reloader_last_child_exit_code', 'Last child exit code observed by reloader')
ROLLBACK_FLAG_PRESENT = Gauge('ofc_ctx_runtime_reloader_rollback_flag_present', '1 if rollback flag exists in active state')
OVERLAY_DIRTY = Gauge('ofc_ctx_runtime_reloader_overlay_dirty', '1 if desired overlay differs from active child overlay')
DEFER_ACTIVE = Gauge('ofc_ctx_runtime_reloader_defer_active', '1 if restart is currently deferred')
COOLDOWN_REMAINING_SECONDS = Gauge('ofc_ctx_runtime_reloader_cooldown_remaining_seconds', 'Cooldown remaining before reloader may restart child')
DEFER_UNTIL_SECONDS = Gauge('ofc_ctx_runtime_reloader_defer_until_seconds', 'Seconds until defer state expires')
INFO = Gauge('ofc_ctx_runtime_reloader_info', 'One-hot info for runtime reloader state', ['active_overlay_fingerprint', 'last_restart_reason_kind', 'defer_reason'])
SOURCE = Gauge('ofc_ctx_runtime_reloader_summary_source', 'One-hot source of runtime summary', ['source'])

_LAST_INFO: tuple[str, str, str] | None = None


def _resolve_data(state_path: str, redis_url: str = '', summary_key: str = '') -> dict[str, Any]:
    if summary_key:
        client = _redis_client(redis_url)
        summary = _read_hash(client, summary_key)
        if summary:
            summary['summary_source'] = 'redis'
            return summary
    data = _read_json(state_path)
    if data:
        data['summary_source'] = 'json'
    return data


def export_once(state_path: str, redis_url: str = '', summary_key: str = '') -> dict[str, Any]:
    global _LAST_INFO
    data = _read_json(state_path)
    UP.set(1.0)
    STATE_PRESENT.set(1.0 if data else 0.0)
    if not data:
        for s in ('redis', 'json'):
            SOURCE.labels(source=s).set(0.0)
        STATE_AGE_SECONDS.set(0.0)
        CHILD_PID.set(0.0)
        CHILD_UPTIME_SECONDS.set(0.0)
        RESTART_COUNT.set(0.0)
        LAST_EXIT_CODE.set(0.0)
        ROLLBACK_FLAG_PRESENT.set(0.0)
        OVERLAY_DIRTY.set(0.0)
        DEFER_ACTIVE.set(0.0)
        COOLDOWN_REMAINING_SECONDS.set(0.0)
        DEFER_UNTIL_SECONDS.set(0.0)
        if _LAST_INFO is not None:
            INFO.labels(*_LAST_INFO).set(0.0)
            _LAST_INFO = None
        return {}

    source = _str(data.get('summary_source', 'json')) or 'json'
    for s in ('redis', 'json'):
        SOURCE.labels(source=s).set(1.0 if s == source else 0.0)
    if source == 'redis':
        STATE_AGE_SECONDS.set(_to_float(data.get('state_age_seconds', 0.0), 0.0))
        CHILD_PID.set(float(_to_int(data.get('child_pid', 0), 0)))
        CHILD_UPTIME_SECONDS.set(_to_float(data.get('child_uptime_seconds', 0.0), 0.0))
    else:
        STATE_AGE_SECONDS.set(_age_seconds(data.get('ts_ms', 0)))
        CHILD_PID.set(float(_to_int(data.get('child_pid', 0), 0)))
        CHILD_UPTIME_SECONDS.set(_uptime_seconds(data.get('child_start_ts_ms', 0)))
    RESTART_COUNT.set(float(_to_int(data.get('restart_count', 0), 0)))
    LAST_EXIT_CODE.set(float(_to_int(data.get('last_child_exit_code', 0), 0)))
    ROLLBACK_FLAG_PRESENT.set(float(_to_int(data.get('rollback_exists', 0), 0)))
    OVERLAY_DIRTY.set(float(_to_int(data.get('overlay_dirty', 0), 0)))
    DEFER_ACTIVE.set(float(_to_int(data.get('defer_active', 0), 0)))
    if source == 'redis':
        COOLDOWN_REMAINING_SECONDS.set(_to_float(data.get('cooldown_remaining_seconds', 0.0), 0.0))
        DEFER_UNTIL_SECONDS.set(_to_float(data.get('defer_remaining_seconds', 0.0), 0.0))
    else:
        COOLDOWN_REMAINING_SECONDS.set(_cooldown_remaining_seconds(data.get('cooldown_until_ts_ms', 0)))
        DEFER_UNTIL_SECONDS.set(_cooldown_remaining_seconds(data.get('defer_until_ts_ms', 0)))

    labels = (
        _str(data.get('active_overlay_fingerprint', 'unknown'))[:64],
        _str(data.get('last_restart_reason_kind', 'unknown'))[:64],
        _str(data.get('defer_reason', ''))[:64],
    )
    if _LAST_INFO is not None and labels != _LAST_INFO:
        INFO.labels(*_LAST_INFO).set(0.0)
    INFO.labels(*labels).set(1.0)
    _LAST_INFO = labels
    return data


def main() -> None:
    port = _to_int(_env('OFC_CTX_RUNTIME_HEALTH_EXPORTER_PORT', '9850'), 9850)
    interval_s = max(1.0, _to_float(_env('OFC_CTX_RUNTIME_HEALTH_EXPORTER_INTERVAL_S', '15'), 15.0))
    state_path = _env('OFC_CTX_RUNTIME_RELOADER_STATE_PATH', '/var/lib/trade/ofc_contextual_runtime_reloader_state.json')
    redis_url = _env('REDIS_URL', 'redis://redis-worker-1:6379/0')
    summary_key = _env('OFC_CTX_RUNTIME_SUMMARY_KEY', 'metrics:ofc_contextual_runtime:last')
    start_http_server(port)
    logger.info('ofc contextual runtime health exporter listening on %s', port)
    while True:
        export_once(state_path, redis_url=redis_url, summary_key=summary_key)
        time.sleep(interval_s)


if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    main()
