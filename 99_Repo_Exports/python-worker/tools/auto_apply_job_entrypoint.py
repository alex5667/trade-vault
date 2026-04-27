'''
Auto-apply job entrypoint (orchestration-friendly).

Goal:
  - Enforce tick-gate block (Step 25/26) BEFORE running auto-apply.
  - Log outcome to Redis stream (best-effort) for audit/reporting.

Designed to be used from systemd timer / cron / docker-compose as a single command:
  python -m tools.auto_apply_job_entrypoint

Environment variables:
  AUTO_APPLY_CMD                     Required. Shell command for the actual apply runner.
                                    Example: "python -m tools.nightly_meta_enforce_ramp_bundle"
  AUTO_APPLY_WORKDIR                 Optional. Working directory for the command.
  AUTO_APPLY_TIMEOUT_S               Optional. Default 600.

  REDIS_URL                          Optional. If present and redis library is available,
                                    logs to ops stream.
  AUTO_APPLY_OPS_STREAM              Optional. Default "ops:auto_apply_runs".

  AUTO_APPLY_BLOCK_PREFIX            Optional. Default:
                                    "cfg:suggestions:entry_policy:auto_apply_block"
  AUTO_APPLY_FAIL_MODE               Optional. "fail_open" or "fail_closed". Default "fail_open".
                                    Controls behavior when block check fails (e.g. Redis down).

Exit codes:
  0   apply succeeded
  10  apply executed but failed
  20  skipped due to tick-gate block
  21  skipped due to block-check error (fail-closed)
  30  misconfiguration
'''

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import shlex
import subprocess
import sys
import time
from typing import Any, Dict, Optional, Tuple


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default


def _now_ms() -> int:
    return get_ny_time_millis()


def _loads_maybe_json(s: Any) -> Any:
    if s is None:
        return None
    if isinstance(s, (dict, list)):
        return s
    try:
        return json.loads(s)
    except Exception:
        return None


def _redis_client(redis_url: str):
    # Best-effort import to avoid hard dependency in environments where redis-py isn't installed.
    try:
        import redis  # type: ignore
    except Exception:
        return None
    try:
        return redis.Redis.from_url(redis_url, decode_responses=True)
    except Exception:
        return None


def _ops_publish(redis_url: Optional[str], stream: str, event: Dict[str, Any]) -> None:
    if not redis_url:
        return
    r = _redis_client(redis_url)
    if not r:
        return
    try:
        payload = {k: ("" if v is None else str(v)) for k, v in event.items()}
        r.xadd(stream, payload, maxlen=50000, approximate=True)
    except Exception:
        return


def _check_block() -> Tuple[bool, Dict[str, Any], Optional[str]]:
    '''
    Returns (blocked, meta, err).
    Uses services.orderflow.auto_apply_guard if available; falls back to raw Redis key check.
    '''
    prefix = _env("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block") or ""
    block_key = f"{prefix}:tick_gate"
    meta_key = f"{prefix}:tick_gate:meta"
    ts_key = f"{prefix}:tick_gate:ts_ms"

    fail_mode = (_env("AUTO_APPLY_FAIL_MODE", "fail_open") or "fail_open").lower()
    redis_url = _env("REDIS_URL") or _env("CRYPTO_NOTIFY_REDIS_URL") or _env("REDIS_MAIN_URL")

    # Prefer the canonical guard (Step 26), but do not make it a hard dependency.
    try:
        from services.orderflow.auto_apply_guard import assert_auto_apply_not_blocked  # type: ignore

        try:
            assert_auto_apply_not_blocked()
            return False, {}, None
        except SystemExit as e:
            # Guard uses 20 for blocked.
            code = int(getattr(e, "code", 1) or 1)
            if code == 20:
                # Try to fetch meta for better logging (best-effort).
                meta: Dict[str, Any] = {}
                if redis_url:
                    r = _redis_client(redis_url)
                    if r:
                        try:
                            meta_raw = r.get(meta_key)
                            ts_raw = r.get(ts_key)
                            meta = _loads_maybe_json(meta_raw) or {}
                            if ts_raw:
                                meta["blocked_ts_ms"] = int(ts_raw)
                        except Exception:
                            pass
                return True, meta, None
            # Unknown exit code: treat as error in block check.
            if fail_mode == "fail_closed":
                return True, {"reason": "block_check_error", "code": code}, "guard_error"
            return False, {"reason": "block_check_error", "code": code}, "guard_error"
    except Exception:
        pass

    # Fallback: raw Redis key check
    if not redis_url:
        if fail_mode == "fail_closed":
            return True, {"reason": "no_redis_url"}, "no_redis_url"
        return False, {"reason": "no_redis_url"}, "no_redis_url"

    r = _redis_client(redis_url)
    if not r:
        if fail_mode == "fail_closed":
            return True, {"reason": "redis_unavailable"}, "redis_unavailable"
        return False, {"reason": "redis_unavailable"}, "redis_unavailable"

    try:
        blocked = str(r.get(block_key) or "").strip() == "1"
        meta_raw = r.get(meta_key)
        ts_raw = r.get(ts_key)
        meta = _loads_maybe_json(meta_raw) or {}
        if ts_raw:
            try:
                meta["blocked_ts_ms"] = int(ts_raw)
            except Exception:
                pass
        return blocked, meta, None
    except Exception as e:
        if fail_mode == "fail_closed":
            return True, {"reason": "redis_error", "err": str(e)}, "redis_error"
        return False, {"reason": "redis_error", "err": str(e)}, "redis_error"


def _run_apply_command(cmd: str, workdir: Optional[str], timeout_s: int) -> Tuple[int, float, str]:
    start = time.perf_counter()
    try:
        # shell=False for safety; allow quoted args in AUTO_APPLY_CMD.
        argv = shlex.split(cmd)
        proc = subprocess.run(
            argv,
            cwd=workdir or None,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_s,
            text=True,
            check=False,
        )
        dur = time.perf_counter() - start
        out = (proc.stdout or "")[-20000:]  # cap
        return int(proc.returncode), dur, out
    except subprocess.TimeoutExpired as e:
        dur = time.perf_counter() - start
        out = ((e.stdout or "") + "\n[TIMEOUT]").strip()
        return 124, dur, out[-20000:]
    except Exception as e:
        dur = time.perf_counter() - start
        return 127, dur, f"[EXCEPTION] {type(e).__name__}: {e}"


def main(argv: Optional[list[str]] = None) -> int:
    _ = argv  # currently unused; reserved for future flags

    cmd = _env("AUTO_APPLY_CMD")
    if not cmd:
        sys.stderr.write("AUTO_APPLY_CMD is required\n")
        return 30

    workdir = _env("AUTO_APPLY_WORKDIR")
    timeout_s = int(float(_env("AUTO_APPLY_TIMEOUT_S", "600") or "600"))

    redis_url = _env("REDIS_URL") or _env("CRYPTO_NOTIFY_REDIS_URL") or _env("REDIS_MAIN_URL")
    ops_stream = _env("AUTO_APPLY_OPS_STREAM", "ops:auto_apply_runs") or "ops:auto_apply_runs"

    blocked, meta, err = _check_block()
    if blocked:
        # Distinguish "blocked by tick gate" vs "fail-closed due to error"
        code = 20 if err is None else 21
        event = {
            "ts_ms": _now_ms(),
            "status": "skipped",
            "exit_code": code,
            "reason": (meta.get("pinned_reason") if isinstance(meta, dict) else None)
            or (meta.get("reason") if isinstance(meta, dict) else None)
            or "tick_gate_block",
            "err": err or "",
            "cmd": cmd,
        }
        # Attach a small meta blob (stringified)
        try:
            event["meta"] = json.dumps(meta, ensure_ascii=False)[:4000]
        except Exception:
            event["meta"] = ""
        _ops_publish(redis_url, ops_stream, event)
        # Print to stdout for cron logs
        sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
        return code

    rc, dur_s, out = _run_apply_command(cmd, workdir, timeout_s)
    status = "ok" if rc == 0 else "fail"
    exit_code = 0 if rc == 0 else 10

    event = {
        "ts_ms": _now_ms(),
        "status": status,
        "exit_code": exit_code,
        "runner_rc": rc,
        "dur_ms": int(dur_s * 1000.0),
        "cmd": cmd,
    }
    # Keep last chunk of output for audit (best-effort)
    if out:
        event["stdout_tail"] = out[-4000:]
    _ops_publish(redis_url, ops_stream, event)

    # Also print for logs
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
