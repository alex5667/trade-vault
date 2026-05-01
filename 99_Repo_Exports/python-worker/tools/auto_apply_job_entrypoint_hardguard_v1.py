from __future__ import annotations
"""Auto-Apply Job Entrypoint (Hard-Guard).

This module is intended to be used by the timer container/service that runs the
auto-apply command periodically.

Hard-guard behavior
  - Before executing AUTO_APPLY_CMD, scan AUTO_APPLY_BLOCK_PREFIX:* keys.
  - If any key indicates a block -> SKIP execution (decision=SKIPPED_FROZEN).
  - Always emit an audit record to AUTO_APPLY_OPS_STREAM (best-effort).
  - Optionally emit an SRE notify event to AUTO_APPLY_NOTIFY_STREAM.

Why a separate entrypoint?
  - Allows shipping a deterministic, fail-safe guard without having to rely on
    the actual apply runner internals.

Usage (docker-compose/systemd)
  python -m tools.auto_apply_job_entrypoint_hardguard_v1

Environment
  Required:
    REDIS_URL
    AUTO_APPLY_CMD

  Guard:
    AUTO_APPLY_BLOCK_PREFIX=cfg:suggestions:entry_policy:auto_apply_block
    AUTO_APPLY_FAIL_MODE=fail_open|fail_closed     (default fail_open)
    AUTO_APPLY_BLOCK_EXISTENCE_BLOCKS=1|0        (default 1)
    AUTO_APPLY_BLOCK_IGNORE_KEYS_REGEX=...
    AUTO_APPLY_BLOCK_IGNORE_REASONS_REGEX=...
    AUTO_APPLY_BLOCK_IGNORE_KEY_SUFFIXES=...

  Audit:
    AUTO_APPLY_OPS_STREAM=ops:auto_apply_runs
    AUTO_APPLY_OPS_STREAM_MAXLEN=20000
    AUTO_APPLY_RUN_ID_TAG=auto_apply_job_entrypoint_hardguard_v1

  Guard Metrics:
    AUTO_APPLY_GUARD_METRICS_ENABLE=1
    AUTO_APPLY_GUARD_METRICS_STREAM=metrics:auto_apply_guard
    AUTO_APPLY_GUARD_METRICS_WIN1M_PREFIX=metrics:auto_apply_guard:win1m

  Exec:
    AUTO_APPLY_WORKDIR=/app
    AUTO_APPLY_TIMEOUT_S=600
    AUTO_APPLY_STDOUT_LIMIT=4000
    AUTO_APPLY_STDERR_LIMIT=4000

  SRE notify (best-effort):
    AUTO_APPLY_NOTIFY_ON_SKIP=1
    AUTO_APPLY_NOTIFY_STREAM=notify:telegram
    AUTO_APPLY_NOTIFY_LEVEL=warn|crit              (default warn)
    AUTO_APPLY_NOTIFY_MIRROR_BASE=1                (mirror to notify:telegram)

Exit codes
  - 0: command executed successfully OR skipped due to block (default)
  - 1: command executed but failed OR unexpected crash
  - custom for skipped can be set via AUTO_APPLY_SKIP_EXIT_CODE
"""

from utils.time_utils import get_ny_time_millis

import json
import os
import re
import shlex
import subprocess
import sys
import time
import traceback
import uuid
from typing import Any, Dict, List, Optional, Tuple, Pattern

import redis  # type: ignore


def _now_ms() -> int:
    return get_ny_time_millis()


def _b2s(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8", "replace")
        except Exception:
            return repr(x)
    return str(x)


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "")
    if not v:
        return int(default)
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if not v:
        return bool(default)
    v = v.strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _split_csv(v: str) -> List[str]:
    if not v:
        return []
    out: List[str] = []
    for s in v.split(","):
        s = (s or "").strip()
        if s:
            out.append(s)
    return out


def _compile_regex(pat: str) -> Optional[Pattern]:
    pat = (pat or "").strip()
    if not pat:
        return None
    try:
        return re.compile(pat)
    except Exception:
        return None


def _truthy(v: str) -> bool:
    v = (v or "").strip().lower()
    if v in ("", "0", "false", "no", "off", "none", "null"):
        return False
    return True


def _redis() -> redis.Redis:
    url = os.getenv("REDIS_URL", "")
    if not url:
        raise RuntimeError("REDIS_URL is required")
    return redis.Redis.from_url(url, decode_responses=False)


def _scan_block_keys(
    r: redis.Redis, prefix: str, scan_count: int = 200, max_keys: int = 2000
) -> List[bytes]:
    # Match any suffix after prefix with colon separator
    pat = f"{prefix}:*"
    cursor = 0
    keys: List[bytes] = []
    while True:
        cursor, batch = r.scan(cursor=cursor, match=pat, count=scan_count)
        if batch:
            keys.extend(batch)
            if len(keys) >= max_keys:
                return keys[:max_keys]
        if cursor == 0:
            return keys


def _read_block_state(r: redis.Redis, key: bytes, existence_blocks: bool) -> Tuple[bool, str, str]:
    """
    Returns (blocked, reason, raw_repr).

    Conservative rules:
      - HASH:
          - if field 'blocked' exists -> truthy(blocked) is authoritative
          - else -> if existence_blocks==True: presence of key implies blocked (legacy)
                   if existence_blocks==False: key without 'blocked' is NOT blocking
      - STRING:
          - value in ('0','false','no','off','') => not blocked; else blocked
      - SET/LIST/ZSET:
          - len>0 => blocked; else not blocked
      - anything else:
          - key presence => blocked
    """
    t = b""
    try:
        t = r.type(key)  # bytes
    except Exception:
        # If we cannot determine type, be conservative: treat as blocked
        return True, "type_read_error", f"type=? key={_b2s(key)}"

    t_s = _b2s(t).lower()
    k_s = _b2s(key)

    try:
        if t_s == "hash":
            d = r.hgetall(key)  # bytes->bytes
            # reason can live under 'reason' or 'why' or 'msg'
            blocked_val = d.get(b"blocked", None)
            if blocked_val is not None:
                b_s = _b2s(blocked_val)
                reason = _b2s(d.get(b"reason", b"")) or _b2s(d.get(b"why", b"")) or ""
                raw = f"hash blocked={b_s} reason={reason}"
                return _truthy(b_s), (reason or "blocked"), raw
            # no explicit field -> existence-only semantics
            reason = _b2s(d.get(b"reason", b"")) or "blocked_key_present"
            if existence_blocks:
                raw = "hash blocked=? (no field) => blocked (existence_blocks=1)"
                return True, reason, raw
            raw = "hash blocked=? (no field) => NOT blocked (existence_blocks=0)"
            return False, "", raw

        if t_s == "string":
            v = r.get(key)
            v_s = _b2s(v)
            raw = f"string value={v_s[:200]}"
            
            v_stripped = v_s.strip()
            if v_stripped.startswith("{") and v_stripped.endswith("}"):
                try:
                    d = json.loads(v_stripped)
                    if "blocked" in d:
                        b_val = d.get("blocked")
                        b_s = str(b_val).lower() if b_val is not None else ""
                        reason = _b2s(d.get("reason", b"")) or _b2s(d.get("why", b"")) or _b2s(d.get("pinned_reason", b"")) or ""
                        raw_json = f"string_json blocked={b_s} reason={reason[:100]}"
                        return _truthy(b_s), reason, raw_json
                except Exception:
                    pass

            return _truthy(v_s), (v_s or "blocked"), raw

        if t_s == "set":
            n = int(r.scard(key))
            raw = f"set size={n}"
            return (n > 0), ("blocked_set" if n > 0 else ""), raw

        if t_s == "list":
            n = int(r.llen(key))
            raw = f"list size={n}"
            return (n > 0), ("blocked_list" if n > 0 else ""), raw

        if t_s == "zset":
            n = int(r.zcard(key))
            raw = f"zset size={n}"
            return (n > 0), ("blocked_zset" if n > 0 else ""), raw

        # unknown type => conservative
        return True, f"blocked_type_{t_s}", f"type={t_s}"
    except Exception as e:
        return True, "block_state_read_error", f"exc={type(e).__name__} key={k_s}"


def _xadd_best_effort(
    r: redis.Redis, stream: str, fields: Dict[str, Any], maxlen: int = 20000
) -> None:
    try:
        flat: Dict[str, str] = {}
        for k, v in fields.items():
            if v is None:
                continue
            if isinstance(v, (dict, list)):
                flat[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
            else:
                flat[k] = _b2s(v)
        r.xadd(stream, flat, maxlen=maxlen, approximate=True)
    except Exception:
        # best-effort only
        pass


def _hincrby_with_ttl_best_effort(r: redis.Redis, key: str, field: str, amount: int, ttl_sec: int) -> None:
    try:
        r.hincrby(key, field, int(amount))
        # only set ttl if no ttl yet
        try:
            t = r.ttl(key)
            if t is None or int(t) < 0:
                r.expire(key, int(ttl_sec))
        except Exception:
            pass
    except Exception:
        pass


def _emit_guard_metrics_best_effort(
    r: redis.Redis,
    decision: str,
    ts_ms: int,
    block_reason: str,
    block_key: str,
    stream: str,
    stream_maxlen: int,
    win_prefix: str,
    win_ttl_sec: int,
) -> None:
    # stream event
    _xadd_best_effort(r, stream, {
        "ts_ms": ts_ms,
        "decision": decision,
        "block_reason": block_reason,
        "block_key": block_key,
    }, maxlen=stream_maxlen)
    # rolling counters (1m buckets)
    bucket = int(ts_ms // 60000)
    hk = f"{win_prefix}:{bucket}"
    if decision == "SKIPPED_FROZEN":
        _hincrby_with_ttl_best_effort(r, hk, "blocked_total", 1, win_ttl_sec)
        if block_reason:
            _hincrby_with_ttl_best_effort(r, hk, f"blocked:{block_reason}", 1, win_ttl_sec)
    elif decision == "OK":
        _hincrby_with_ttl_best_effort(r, hk, "run_ok_total", 1, win_ttl_sec)
    else:
        _hincrby_with_ttl_best_effort(r, hk, "run_err_total", 1, win_ttl_sec)


def _notify_best_effort(r: redis.Redis, stream: str, payload: Dict[str, Any]) -> None:
    try:
        flat: Dict[str, str] = {}
        for k, v in payload.items():
            if v is None:
                continue
            if isinstance(v, (dict, list)):
                flat[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
            else:
                flat[k] = _b2s(v)
        r.xadd(stream, flat, maxlen=20000, approximate=True)
    except Exception:
        pass


def _run_cmd(cmd: str, workdir: str, timeout_s: int, out_lim: int, err_lim: int) -> Tuple[int, str, str, int]:
    argv = shlex.split(cmd)
    t0 = time.time()
    p = subprocess.run(
        argv,
        cwd=workdir or None,
        capture_output=True,
        text=True,
        timeout=timeout_s if timeout_s > 0 else None,
        env=os.environ.copy(),
    )
    dur_ms = int((time.time() - t0) * 1000)
    out = (p.stdout or "")[-out_lim:]
    err = (p.stderr or "")[-err_lim:]
    return int(p.returncode), out, err, dur_ms


def main() -> int:
    run_id = os.getenv("AUTO_APPLY_RUN_ID_TAG", "auto_apply_job_entrypoint_hardguard_v1")
    rid = f"{run_id}:{uuid.uuid4().hex[:10]}"

    cmd = os.getenv("AUTO_APPLY_CMD", "")
    if not cmd:
        raise RuntimeError("AUTO_APPLY_CMD is required")

    workdir = os.getenv("AUTO_APPLY_WORKDIR", "/app")
    timeout_s = _env_int("AUTO_APPLY_TIMEOUT_S", 600)
    out_lim = _env_int("AUTO_APPLY_STDOUT_LIMIT", 4000)
    err_lim = _env_int("AUTO_APPLY_STDERR_LIMIT", 4000)

    ops_stream = os.getenv("AUTO_APPLY_OPS_STREAM", "ops:auto_apply_runs")
    ops_maxlen = _env_int("AUTO_APPLY_OPS_STREAM_MAXLEN", 20000)

    block_prefix = os.getenv("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block")
    fail_mode = (os.getenv("AUTO_APPLY_FAIL_MODE", "fail_open") or "fail_open").strip().lower()
    skip_exit = _env_int("AUTO_APPLY_SKIP_EXIT_CODE", 0)
    existence_blocks = _env_bool("AUTO_APPLY_BLOCK_EXISTENCE_BLOCKS", True)

    ignore_keys_re = _compile_regex(os.getenv("AUTO_APPLY_BLOCK_IGNORE_KEYS_REGEX", ""))
    ignore_reasons_re = _compile_regex(os.getenv("AUTO_APPLY_BLOCK_IGNORE_REASONS_REGEX", ""))
    
    # Default ignore common telemetry suffixes used by tick_gate
    default_ignore = ":meta,:state,:ts_ms"
    ignore_suffixes = _split_csv(os.getenv("AUTO_APPLY_BLOCK_IGNORE_KEY_SUFFIXES", default_ignore))

    guard_metrics_enable = _env_bool("AUTO_APPLY_GUARD_METRICS_ENABLE", True)
    guard_metrics_stream = os.getenv("AUTO_APPLY_GUARD_METRICS_STREAM", "metrics:auto_apply_guard") or "metrics:auto_apply_guard"
    guard_metrics_stream_maxlen = _env_int("AUTO_APPLY_GUARD_METRICS_STREAM_MAXLEN", 20000)
    guard_metrics_win_prefix = os.getenv("AUTO_APPLY_GUARD_METRICS_WIN1M_PREFIX", "metrics:auto_apply_guard:win1m") or "metrics:auto_apply_guard:win1m"
    guard_metrics_win_ttl_sec = _env_int("AUTO_APPLY_GUARD_METRICS_WIN_TTL_SEC", 10800)

    notify_on_skip = _env_bool("AUTO_APPLY_NOTIFY_ON_SKIP", True)
    notify_stream = os.getenv("AUTO_APPLY_NOTIFY_STREAM", "notify:telegram") or "notify:telegram"
    notify_level = (os.getenv("AUTO_APPLY_NOTIFY_LEVEL", "warn") or "warn").strip().lower()
    notify_mirror_base = _env_bool("AUTO_APPLY_NOTIFY_MIRROR_BASE", True)

    ts_ms = _now_ms()
    decision = "UNKNOWN"
    block_key = ""
    block_reason = ""
    block_raw = ""
    rc: Optional[int] = None
    dur_ms: Optional[int] = None
    out_tail = ""
    err_tail = ""
    guard_error = ""

    # Connect Redis
    r: Optional[redis.Redis] = None
    try:
        r = _redis()
        # ping to fail fast if URL is wrong
        r.ping()
    except Exception as e:
        guard_error = f"redis_connect_error:{type(e).__name__}"
        if fail_mode == "fail_closed":
            decision = "SKIPPED_FROZEN"
            block_reason = guard_error
        else:
            decision = "GUARD_ERROR_FAIL_OPEN"

    # Guard check
    if r is not None and decision not in ("SKIPPED_FROZEN",):
        try:
            keys = _scan_block_keys(r, block_prefix)
            # pick the first key that is actually blocking
            for k in keys:
                k_s = _b2s(k)
                if ignore_suffixes and any(k_s.endswith(suf) for suf in ignore_suffixes):
                    continue
                if ignore_keys_re and ignore_keys_re.search(k_s):
                    continue

                blocked, reason, raw = _read_block_state(r, k, existence_blocks)

                if ignore_reasons_re and reason and ignore_reasons_re.search(reason):
                    blocked = False
                    reason = ""

                if blocked:
                    decision = "SKIPPED_FROZEN"
                    block_key = k_s
                    block_reason = reason or "blocked"
                    block_raw = raw
                    break
            if decision == "UNKNOWN":
                decision = "RUN"
        except Exception as e:
            guard_error = f"guard_scan_error:{type(e).__name__}"
            if fail_mode == "fail_closed":
                decision = "SKIPPED_FROZEN"
                block_reason = guard_error
            else:
                decision = "GUARD_ERROR_FAIL_OPEN"

    # Execute command unless skipped
    if decision in ("RUN", "GUARD_ERROR_FAIL_OPEN"):
        try:
            rc, out_tail, err_tail, dur_ms = _run_cmd(cmd, workdir, timeout_s, out_lim, err_lim)
            decision = "OK" if int(rc) == 0 else "CMD_FAILED"
        except subprocess.TimeoutExpired:
            decision = "CMD_TIMEOUT"
            rc = 124
            dur_ms = int(timeout_s * 1000)
        except Exception as e:
            decision = "CMD_EXCEPTION"
            rc = 125
            err_tail = f"{type(e).__name__}: {_b2s(e)}"

    # [NEW] Log decision to stdout for monitoring/drills
    print(f"AUTO_APPLY_DECISION: {decision}")
    if decision == "SKIPPED_FROZEN":
        print(f"AUTO_APPLY_BLOCK_REASON: {block_reason}")

    # Audit record (best-effort)
    if r is not None:
        fields: Dict[str, Any] = {
            "ts_ms": ts_ms,
            "rid": rid,
            "run_id": run_id,
            "decision": decision,
            "cmd": cmd,
            "workdir": workdir,
            "timeout_s": timeout_s,
            "rc": rc,
            "dur_ms": dur_ms,
            "blocked": 1 if decision == "SKIPPED_FROZEN" else 0,
            "block_prefix": block_prefix,
            "block_key": block_key,
            "block_reason": block_reason,
            "block_raw": block_raw,
            "guard_error": guard_error,
            "stdout_tail": out_tail,
            "stderr_tail": err_tail,
        }
        _xadd_best_effort(r, ops_stream, fields, maxlen=ops_maxlen)

        # Guard metrics (best-effort)
        if guard_metrics_enable:
            _emit_guard_metrics_best_effort(
                r, decision=decision, ts_ms=ts_ms, block_reason=block_reason,
                block_key=block_key, stream=guard_metrics_stream, stream_maxlen=guard_metrics_stream_maxlen,
                win_prefix=guard_metrics_win_prefix, win_ttl_sec=guard_metrics_win_ttl_sec
            )

        # Optional SRE notify on skip
        if decision == "SKIPPED_FROZEN" and notify_on_skip:
            import html
            reason_safe = html.escape(str(block_reason))
            raw_safe = html.escape(str(block_raw))
            title = "Auto-Apply SKIPPED (frozen)"
            text = f"rid={rid} cmd={cmd} block_key={block_key} reason={reason_safe} raw={raw_safe}"
            payload = {
                "type": "auto_apply_skip",
                "level": notify_level,
                "title": title,
                "text": text,
                "ts_ms": ts_ms,
            }
            _notify_best_effort(r, notify_stream, payload)

            # Mirror to base stream if configured and stream is not base
            if notify_mirror_base and notify_stream != "notify:telegram":
                _notify_best_effort(r, "notify:telegram", payload)

    # Exit code policy
    if decision == "SKIPPED_FROZEN":
        return int(skip_exit)
    if decision in ("OK",):
        return 0
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
