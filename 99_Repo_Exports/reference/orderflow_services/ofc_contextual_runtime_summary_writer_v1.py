from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_json(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _redis_client(redis_url: str):
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(redis_url, decode_responses=True)
    except Exception:
        return None


def _write_hash(client: Any, key: str, mapping: Dict[str, Any]) -> bool:
    if client is None or not key:
        return False
    payload: Dict[str, str] = {}
    for k, v in mapping.items():
        if isinstance(v, (dict, list)):
            payload[str(k)] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        else:
            payload[str(k)] = str(v)
    try:
        client.hset(key, mapping=payload)
        return True
    except Exception:
        return False


def _escape_label(v: Any) -> str:
    return str(v or "").replace("\\", r"\\").replace('"', r'\"').replace("\n", r"\n")


def _write_textfile(path: str, summary: Dict[str, Any]) -> bool:
    if not path:
        return False
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lines = [
        "# HELP ofc_ctx_runtime_summary_up 1 if runtime summary writer produced metrics",
        "# TYPE ofc_ctx_runtime_summary_up gauge",
        "ofc_ctx_runtime_summary_up 1",
        "# HELP ofc_ctx_runtime_summary_state_present 1 if runtime state exists and parses",
        "# TYPE ofc_ctx_runtime_summary_state_present gauge",
        f"ofc_ctx_runtime_summary_state_present {1 if summary else 0}",
        "# HELP ofc_ctx_runtime_summary_age_seconds Age of runtime summary state",
        "# TYPE ofc_ctx_runtime_summary_age_seconds gauge",
        f"ofc_ctx_runtime_summary_age_seconds {float(summary.get('state_age_seconds', 0.0) or 0.0):.6f}",
        "# HELP ofc_ctx_runtime_summary_child_pid Current child pid",
        "# TYPE ofc_ctx_runtime_summary_child_pid gauge",
        f"ofc_ctx_runtime_summary_child_pid {float(summary.get('child_pid', 0) or 0):.0f}",
        "# HELP ofc_ctx_runtime_summary_child_uptime_seconds Current child uptime",
        "# TYPE ofc_ctx_runtime_summary_child_uptime_seconds gauge",
        f"ofc_ctx_runtime_summary_child_uptime_seconds {float(summary.get('child_uptime_seconds', 0.0) or 0.0):.6f}",
        "# HELP ofc_ctx_runtime_summary_restart_count Runtime restart count",
        "# TYPE ofc_ctx_runtime_summary_restart_count gauge",
        f"ofc_ctx_runtime_summary_restart_count {float(summary.get('restart_count', 0) or 0):.0f}",
        "# HELP ofc_ctx_runtime_summary_cooldown_remaining_seconds Cooldown remaining",
        "# TYPE ofc_ctx_runtime_summary_cooldown_remaining_seconds gauge",
        f"ofc_ctx_runtime_summary_cooldown_remaining_seconds {float(summary.get('cooldown_remaining_seconds', 0.0) or 0.0):.6f}",
        "# HELP ofc_ctx_runtime_summary_defer_remaining_seconds Defer remaining",
        "# TYPE ofc_ctx_runtime_summary_defer_remaining_seconds gauge",
        f"ofc_ctx_runtime_summary_defer_remaining_seconds {float(summary.get('defer_remaining_seconds', 0.0) or 0.0):.6f}",
        "# HELP ofc_ctx_runtime_summary_overlay_dirty Overlay dirty flag",
        "# TYPE ofc_ctx_runtime_summary_overlay_dirty gauge",
        f"ofc_ctx_runtime_summary_overlay_dirty {float(summary.get('overlay_dirty', 0) or 0):.0f}",
        "# HELP ofc_ctx_runtime_summary_defer_active Defer active flag",
        "# TYPE ofc_ctx_runtime_summary_defer_active gauge",
        f"ofc_ctx_runtime_summary_defer_active {float(summary.get('defer_active', 0) or 0):.0f}",
        "# HELP ofc_ctx_runtime_summary_rollback_flag_present Rollback flag present",
        "# TYPE ofc_ctx_runtime_summary_rollback_flag_present gauge",
        f"ofc_ctx_runtime_summary_rollback_flag_present {float(summary.get('rollback_exists', 0) or 0):.0f}",
        "# HELP ofc_ctx_runtime_summary_info Runtime summary info labels",
        "# TYPE ofc_ctx_runtime_summary_info gauge",
        'ofc_ctx_runtime_summary_info{active_overlay_fingerprint="%s",last_restart_reason_kind="%s",defer_reason="%s"} 1'
        % (
            _escape_label(summary.get("active_overlay_fingerprint", "unknown")[:128]),
            _escape_label(summary.get("last_restart_reason_kind", "unknown")[:64]),
            _escape_label(summary.get("defer_reason", "")[:64]),
        ),
    ]
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    os.replace(tmp, path)
    return True


def build_summary(state: Dict[str, Any], *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    now_ms = int(now_ms if now_ms is not None else _now_ms())
    if not state:
        return {
            "ts_ms": now_ms,
            "state_present": 0,
            "state_age_seconds": 0.0,
            "child_pid": 0,
            "child_uptime_seconds": 0.0,
            "restart_count": 0,
            "last_restart_reason": "",
            "last_restart_reason_kind": "unknown",
            "cooldown_remaining_seconds": 0.0,
            "defer_active": 0,
            "defer_reason": "",
            "defer_remaining_seconds": 0.0,
            "overlay_dirty": 0,
            "rollback_exists": 0,
            "active_overlay_fingerprint": "",
            "desired_overlay_fingerprint": "",
            "last_child_exit_code": 0,
        }
    ts_ms = _to_int(state.get("ts_ms", 0), 0)
    child_start_ts_ms = _to_int(state.get("child_start_ts_ms", 0), 0)
    cooldown_until_ts_ms = _to_int(state.get("cooldown_until_ts_ms", 0), 0)
    defer_until_ts_ms = _to_int(state.get("defer_until_ts_ms", 0), 0)
    return {
        "ts_ms": now_ms,
        "state_present": 1,
        "state_ts_ms": ts_ms,
        "state_age_seconds": max(0.0, (now_ms - ts_ms) / 1000.0) if ts_ms > 0 else 0.0,
        "child_pid": _to_int(state.get("child_pid", 0), 0),
        "child_uptime_seconds": max(0.0, (now_ms - child_start_ts_ms) / 1000.0) if child_start_ts_ms > 0 else 0.0,
        "restart_count": _to_int(state.get("restart_count", 0), 0),
        "last_restart_reason": str(state.get("last_restart_reason", "") or ""),
        "last_restart_reason_kind": str(state.get("last_restart_reason_kind", "unknown") or "unknown"),
        "cooldown_remaining_seconds": max(0.0, (cooldown_until_ts_ms - now_ms) / 1000.0) if cooldown_until_ts_ms > 0 else 0.0,
        "defer_active": _to_int(state.get("defer_active", 0), 0),
        "defer_reason": str(state.get("defer_reason", "") or ""),
        "defer_remaining_seconds": max(0.0, (defer_until_ts_ms - now_ms) / 1000.0) if defer_until_ts_ms > 0 else 0.0,
        "overlay_dirty": _to_int(state.get("overlay_dirty", 0), 0),
        "rollback_exists": _to_int(state.get("rollback_exists", 0), 0),
        "active_overlay_fingerprint": str(state.get("active_overlay_fingerprint", "") or ""),
        "desired_overlay_fingerprint": str(state.get("desired_overlay_fingerprint", "") or ""),
        "last_child_exit_code": _to_int(state.get("last_child_exit_code", 0), 0),
    }


def write_summary_once(
    *,
    state_path: str,
    redis_url: str = "",
    summary_key: str = "",
    textfile_path: str = "",
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    state = _read_json(state_path)
    summary = build_summary(state, now_ms=now_ms)
    summary["redis_write_ok"] = 0
    summary["textfile_write_ok"] = 0
    if summary_key:
        client = _redis_client(redis_url)
        summary["redis_write_ok"] = 1 if _write_hash(client, summary_key, summary) else 0
    if textfile_path:
        summary["textfile_write_ok"] = 1 if _write_textfile(textfile_path, summary) else 0
    return summary


def main() -> None:  # pragma: no cover
    state_path = _env("OFC_CTX_RUNTIME_RELOADER_STATE_PATH", "/var/lib/trade/ofc_contextual_runtime_reloader_state.json")
    redis_url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    summary_key = _env("OFC_CTX_RUNTIME_SUMMARY_KEY", "metrics:ofc_contextual_runtime:last")
    textfile_path = _env("OFC_CTX_RUNTIME_SUMMARY_TEXTFILE_PATH", "/var/lib/node_exporter/textfile_collector/ofc_contextual_runtime.prom")
    interval_s = max(1.0, _to_float(_env("OFC_CTX_RUNTIME_SUMMARY_INTERVAL_S", "15"), 15.0))
    while True:
        write_summary_once(
            state_path=state_path,
            redis_url=redis_url,
            summary_key=summary_key,
            textfile_path=textfile_path,
        )
        time.sleep(interval_s)


if __name__ == "__main__":  # pragma: no cover
    main()
