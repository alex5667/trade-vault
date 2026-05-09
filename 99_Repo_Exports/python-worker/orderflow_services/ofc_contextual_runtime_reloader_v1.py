from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Runtime reloader for OFC contextual overlay / rollback integration.

Purpose
-------
Keep the main OFC runtime process running under a tiny supervisor which:
- loads the contextual rollout overlay env file produced by Patch D,
- watches overlay / rollback flag changes,
- restarts the child process automatically with fresh env,
- avoids manual restart of the *service configuration* when rollout mode changes.

This file is intentionally stdlib-only.
""",
import argparse
import hashlib
import json
import os
import signal
import subprocess
import time
from collections.abc import Iterable
import contextlib


def _now_ms() -> int:
    return get_ny_time_millis()


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _to_int(v: object, default: int) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def load_env_file(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path or not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip()
            if not k:
                continue
            if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
                v = v[1:-1]
            out[k] = v
    return out


def merge_child_env(base_env: dict[str, str], overlay_env: dict[str, str]) -> dict[str, str]:
    env = dict(base_env)
    for k, v in overlay_env.items():
        if k:
            env[str(k)] = str(v)
    return env


def fingerprint_overlay(overlay_env: dict[str, str], rollback_exists: bool) -> str:
    payload = {
        "overlay": {k: overlay_env[k] for k in sorted(overlay_env.keys())},
        "rollback_exists": bool(rollback_exists),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _atomic_write_json(path: str, payload: dict[str, object]) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, sort_keys=True, indent=2)
        fh.write("\n")
    os.replace(tmp, path)


def _terminate_child(proc: subprocess.Popen, grace_sec: int) -> int:
    if proc.poll() is not None:
        return int(proc.returncode or 0)
    with contextlib.suppress(Exception):
        proc.terminate()
    deadline = time.time() + max(1, grace_sec)
    while time.time() < deadline:
        rc = proc.poll()
        if rc is not None:
            return int(rc)
        time.sleep(0.2)
    with contextlib.suppress(Exception):
        proc.kill()
    try:
        return int(proc.wait(timeout=5))
    except Exception:
        return 137


def _spawn_child(argv: list[str], env: dict[str, str]) -> subprocess.Popen:
    return subprocess.Popen(argv, env=env)


def _split_cmd(args: argparse.Namespace) -> list[str]:
    cmd = list(args.cmd or [])
    if not cmd:
        raise SystemExit("runtime child command is required after '--'")
    return cmd


def _load_overlay_state(path: str, rollback_flag_path: str) -> tuple[dict[str, str], bool, str]:
    overlay = load_env_file(path)
    rb_exists = bool(rollback_flag_path and os.path.exists(rollback_flag_path))
    fp = fingerprint_overlay(overlay, rb_exists)
    return overlay, rb_exists, fp


def _reason_kind(reason: str) -> str:
    s = (reason or "").strip().lower()
    if not s:
        return "unknown"
    if s == "initial":
        return "initial"
    if s == "cooldown":
        return "cooldown"
    if s.startswith("child_exit"):
        return "child_exit"
    if s.startswith("overlay_changed"):
        return "overlay_changed"
    if s.startswith("signal"):
        return "signal"
    return "other"


def run_supervisor(args: argparse.Namespace) -> int:
    child_argv = _split_cmd(args)
    overlay_path = str(args.overlay_env_file)
    rollback_flag_path = str(args.rollback_flag_path or "")
    poll_sec = max(1, _to_int(args.poll_sec, 5))
    cooldown_sec = max(1, _to_int(args.cooldown_sec, 15))
    grace_sec = max(1, _to_int(args.grace_sec, 20))
    state_path = str(args.state_path or "")

    base_env = dict(os.environ)
    desired_overlay_env, desired_rollback_exists, desired_fingerprint = _load_overlay_state(overlay_path, rollback_flag_path)
    active_overlay_env = dict(desired_overlay_env)
    active_rollback_exists = bool(desired_rollback_exists)
    active_fingerprint = str(desired_fingerprint)

    child_env = merge_child_env(base_env, active_overlay_env)
    child_env["OFC_CTX_RUNTIME_RELOADER_ACTIVE"] = "1"
    child_env["OFC_CTX_RUNTIME_OVERLAY_FINGERPRINT"] = active_fingerprint
    child = _spawn_child(child_argv, child_env)

    child_start_ms = _now_ms()
    last_restart_ms = int(child_start_ms)
    cooldown_until_ts_ms = int(last_restart_ms + cooldown_sec * 1000)
    restart_count = 0
    last_restart_reason = "initial"
    last_restart_detail = "initial"
    last_child_exit_code: int | None = None
    defer_active = False
    defer_reason = ""
    defer_until_ts_ms = 0

    def _write_state(event: str, reason: str, child_pid: int, child_rc: int | None) -> None:
        nonlocal last_child_exit_code
        if child_rc is not None:
            last_child_exit_code = int(child_rc)
        overlay_dirty = int(active_fingerprint != desired_fingerprint)
        payload = {
            "ts_ms": _now_ms(),
            "event": str(event),
            "reason": reason,
            "overlay_env_file": overlay_path,
            "rollback_flag_path": rollback_flag_path,
            "active_overlay_fingerprint": str(active_fingerprint),
            "desired_overlay_fingerprint": str(desired_fingerprint),
            "rollback_exists": int(active_rollback_exists),
            "desired_rollback_exists": int(desired_rollback_exists),
            "overlay_dirty": int(overlay_dirty),
            "child_pid": int(child_pid),
            "child_rc": None if child_rc is None else int(child_rc),
            "last_child_exit_code": None if last_child_exit_code is None else int(last_child_exit_code),
            "child_start_ts_ms": int(child_start_ms),
            "last_restart_ts_ms": int(last_restart_ms),
            "restart_count": int(restart_count),
            "last_restart_reason": str(last_restart_reason),
            "last_restart_reason_kind": _reason_kind(last_restart_reason),
            "last_restart_detail": str(last_restart_detail),
            "cooldown_sec": int(cooldown_sec),
            "cooldown_until_ts_ms": int(cooldown_until_ts_ms),
            "defer_active": int(defer_active),
            "defer_reason": str(defer_reason),
            "defer_until_ts_ms": int(defer_until_ts_ms),
            "child_argv": list(child_argv),
        }
        _atomic_write_json(state_path, payload)

    _write_state("start", "initial", int(child.pid), None)

    def _graceful_stop(signum: int, _frame: object) -> None:
        _write_state("signal", f"signal:{signum}", int(child.pid), child.poll())
        _terminate_child(child, grace_sec)
        raise SystemExit(128 + int(signum))

    signal.signal(signal.SIGTERM, _graceful_stop)
    signal.signal(signal.SIGINT, _graceful_stop)

    while True:
        time.sleep(poll_sec)
        rc = child.poll()
        new_overlay_env, new_rollback_exists, new_fp = _load_overlay_state(overlay_path, rollback_flag_path)
        desired_overlay_env = new_overlay_env
        desired_rollback_exists = bool(new_rollback_exists)
        desired_fingerprint = str(new_fp)
        changed = bool(desired_fingerprint != active_fingerprint)

        if rc is not None:
            restart_count += 1
            active_overlay_env = dict(desired_overlay_env)
            active_rollback_exists = bool(desired_rollback_exists)
            active_fingerprint = str(desired_fingerprint)
            child_env = merge_child_env(base_env, active_overlay_env)
            child_env["OFC_CTX_RUNTIME_RELOADER_ACTIVE"] = "1"
            child_env["OFC_CTX_RUNTIME_OVERLAY_FINGERPRINT"] = active_fingerprint
            child = _spawn_child(child_argv, child_env)
            child_start_ms = _now_ms()
            last_restart_ms = int(child_start_ms)
            cooldown_until_ts_ms = int(last_restart_ms + cooldown_sec * 1000)
            last_restart_reason = f"child_exit:{rc}"
            last_restart_detail = f"child_exit:{rc}"
            defer_active = False
            defer_reason = ""
            defer_until_ts_ms = 0
            _write_state("restart", last_restart_reason, int(child.pid), int(rc))
            continue

        if not changed:
            _write_state("steady", "steady", int(child.pid), None)
            continue

        now_ms = _now_ms()
        if now_ms < cooldown_until_ts_ms:
            defer_active = True
            defer_reason = "cooldown"
            defer_until_ts_ms = int(cooldown_until_ts_ms)
            _write_state("defer", "cooldown", int(child.pid), None)
            continue

        old_pid = int(child.pid)
        old_rc = _terminate_child(child, grace_sec)
        restart_count += 1
        active_overlay_env = dict(desired_overlay_env)
        active_rollback_exists = bool(desired_rollback_exists)
        active_fingerprint = str(desired_fingerprint)
        child_env = merge_child_env(base_env, active_overlay_env)
        child_env["OFC_CTX_RUNTIME_RELOADER_ACTIVE"] = "1"
        child_env["OFC_CTX_RUNTIME_OVERLAY_FINGERPRINT"] = active_fingerprint
        child = _spawn_child(child_argv, child_env)
        child_start_ms = _now_ms()
        last_restart_ms = int(child_start_ms)
        cooldown_until_ts_ms = int(last_restart_ms + cooldown_sec * 1000)
        last_restart_reason = "overlay_changed"
        last_restart_detail = f"overlay_changed:old_pid={old_pid}"
        defer_active = False
        defer_reason = ""
        defer_until_ts_ms = 0
        _write_state("restart", last_restart_reason, int(child.pid), int(old_rc))

def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Supervise OFC runtime and auto-restart on overlay/rollback changes")
    ap.add_argument("--overlay-env-file", default=_env("OFC_CTX_RUNTIME_OVERLAY_ENV_FILE", "/var/lib/trade/ofc_contextual_rollout.env"))
    ap.add_argument("--rollback-flag-path", default=_env("OFC_CTX_ROLLBACK_FLAG_PATH", "/var/lib/trade/ofc_contextual.rollback"))
    ap.add_argument("--poll-sec", type=int, default=_to_int(_env("OFC_CTX_RUNTIME_POLL_SEC", "5"), 5))
    ap.add_argument("--cooldown-sec", type=int, default=_to_int(_env("OFC_CTX_RUNTIME_RESTART_COOLDOWN_SEC", "15"), 15))
    ap.add_argument("--grace-sec", type=int, default=_to_int(_env("OFC_CTX_RUNTIME_TERM_GRACE_SEC", "20"), 20))
    ap.add_argument("--state-path", default=_env("OFC_CTX_RUNTIME_RELOADER_STATE_PATH", "/var/lib/trade/ofc_contextual_runtime_reloader_state.json"))
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    return ap


def main(argv: Iterable[str] | None = None) -> int:
    ap = build_arg_parser()
    ns = ap.parse_args(list(argv) if argv is not None else None)
    cmd = list(ns.cmd or [])
    if cmd and cmd[0] == "--":
        ns.cmd = cmd[1:]
    return run_supervisor(ns)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
