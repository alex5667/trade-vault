from __future__ import annotations
"""Auto-apply blocker v2: Tick Gate -> Redis block keys with anti-flap.

This daemon periodically runs tick-quality gate (tools.tick_quality_gate_check)
and sets or clears the auto-apply block key.

Anti-flap:
  - min hold time while blocked (AUTO_APPLY_BLOCK_MIN_HOLD_S)
  - require N consecutive PASS to unblock (AUTO_APPLY_UNBLOCK_PASS_STREAK)
  - pin fail reason for a window (AUTO_APPLY_BLOCK_REASON_PIN_S)
"""

from utils.time_utils import get_ny_time_millis

import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, Optional, Tuple

from common.redis_errors import retry_redis_operation


DEFAULT_PREFIX = "cfg:suggestions:entry_policy:auto_apply_block"


def _now_ms() -> int:
    return get_ny_time_millis()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default


def _connect_redis(redis_url: str):
    import redis  # type: ignore
    return redis.Redis.from_url(redis_url, decode_responses=True)


def _load_json(s: Optional[str]) -> Dict[str, Any]:
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _dump_json(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _run_gate(metrics_url: str, window_s: int, symbol: Optional[str]) -> Tuple[int, Dict[str, Any]]:
    cmd = [sys.executable, "-m", "tools.tick_quality_gate_check", "--metrics-url", metrics_url, "--window-s", str(window_s), "--json"]
    if symbol:
        cmd += ["--symbol", symbol]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out = (p.stdout or "").strip()
    data: Dict[str, Any] = {}
    if out:
        data = _load_json(out)
    data.setdefault("_stderr", (p.stderr or "").strip()[:2000])
    data.setdefault("_cmd", " ".join(cmd))
    return p.returncode, data


def _extract_reason(gate_json: Dict[str, Any]) -> str:
    # Prefer explicit "failed_checks" list; else fall back to status
    failed = gate_json.get("failed_checks")
    if isinstance(failed, list) and failed:
        # keep it stable and short
        return str(failed[0])[:64]
    return str(gate_json.get("status") or "unknown")[:64]


def _should_pin_reason(prev: Dict[str, Any], now_ms: int, pin_ms: int) -> bool:
    try:
        t = int(prev.get("pinned_reason_ts_ms") or 0)
        return t > 0 and (now_ms - t) <= pin_ms
    except Exception:
        return False


def _apply_state_transition(
    prev: Dict[str, Any],
    *,
    rc: int,
    gate: Dict[str, Any],
    now_ms: int,
    min_hold_ms: int,
    pass_streak_to_unblock: int,
    pin_ms: int,
    insuff_mode: str,
) -> Dict[str, Any]:
    """Return next state dict."""
    st = dict(prev or {})
    st["ts_ms"] = now_ms
    st["last_rc"] = int(rc)
    st["last_gate_status"] = str(gate.get("status") or "")
    st["last_gate_json"] = gate  # may be truncated by Redis size policies upstream

    # Determine status class from return code: 0 PASS, 2 FAIL, 1 INSUFF, else ERROR
    if rc == 0:
        cls = "pass"
    elif rc == 2:
        cls = "fail"
    elif rc == 1:
        cls = "insufficient"
    else:
        cls = "error"
    st["status_class"] = cls

    blocked = bool(st.get("blocked"))
    hold_until = int(st.get("hold_until_ms") or 0)
    pass_streak = int(st.get("pass_streak") or 0)

    if cls == "fail" or (cls == "insufficient" and insuff_mode == "fail_closed") or cls == "error":
        # Block
        st["blocked"] = True
        st["pass_streak"] = 0
        st["hold_until_ms"] = max(hold_until, now_ms + min_hold_ms)

        new_reason = _extract_reason(gate if cls != "error" else {"failed_checks": ["gate_error"]})
        if _should_pin_reason(st, now_ms, pin_ms):
            # keep pinned reason
            pass
        else:
            st["pinned_reason"] = new_reason
            st["pinned_reason_ts_ms"] = now_ms
        st["last_fail_ts_ms"] = now_ms
        return st

    if cls == "pass":
        st["pass_streak"] = pass_streak + 1
        if blocked:
            # Respect hold
            if now_ms < hold_until:
                st["blocked"] = True
                return st
            # Require consecutive PASS to unblock
            if st["pass_streak"] >= pass_streak_to_unblock:
                st["blocked"] = False
                st["last_unblocked_ts_ms"] = now_ms
        return st

    # insufficient + fail_open: do not change blocked state, but do not advance pass streak
    st["pass_streak"] = 0
    return st


def _write_block_keys(cli, prefix: str, state: Dict[str, Any], ttl_s: int) -> None:
    block_key = f"{prefix}:tick_gate"
    meta_key = f"{prefix}:tick_gate:meta"
    ts_key = f"{prefix}:tick_gate:ts_ms"
    now_ms = int(state.get("ts_ms") or _now_ms())

    meta = {
        "blocked": bool(state.get("blocked")),
        "status_class": str(state.get("status_class") or ""),
        "pinned_reason": str(state.get("pinned_reason") or ""),
        "pass_streak": int(state.get("pass_streak") or 0),
        "hold_until_ms": int(state.get("hold_until_ms") or 0),
        "ts_ms": now_ms,
    }

    pipe = cli.pipeline(transaction=False)
    pipe.set(ts_key, str(now_ms))

    if meta["blocked"]:
        # hard block key with ttl
        pipe.setex(block_key, int(ttl_s), "1")
    else:
        pipe.delete(block_key)

    pipe.set(meta_key, _dump_json(meta))
    pipe.execute()


def _maybe_publish_ops(cli, stream: str, state: Dict[str, Any], maxlen: int = 20000) -> None:
    try:
        payload = {
            "ts_ms": str(int(state.get("ts_ms") or _now_ms())),
            "blocked": "1" if state.get("blocked") else "0",
            "status": str(state.get("status_class") or ""),
            "reason": str(state.get("pinned_reason") or ""),
            "pass_streak": str(int(state.get("pass_streak") or 0)),
            "hold_until_ms": str(int(state.get("hold_until_ms") or 0)),
            "rc": str(int(state.get("last_rc") or 0)),
        }
        cli.xadd(stream, payload, maxlen=maxlen, approximate=True)
    except Exception:
        return


def main(argv: list[str]) -> int:
    once = ("--once" in argv)
    redis_url = _env_str("REDIS_URL", _env_str("CRYPTO_NOTIFY_REDIS_URL", ""))
    if not redis_url:
        sys.stderr.write("REDIS_URL is required\n")
        return 2

    metrics_url = _env_str("TICK_GATE_METRICS_URL", "http://localhost:8000/metrics")
    window_s = _env_int("TICK_GATE_WINDOW_S", 60)
    symbol = os.getenv("TICK_GATE_SYMBOL")  # optional

    prefix = _env_str("AUTO_APPLY_BLOCK_PREFIX", DEFAULT_PREFIX)
    interval_s = _env_int("AUTO_APPLY_BLOCKER_INTERVAL_S", 30)
    ttl_s = _env_int("AUTO_APPLY_BLOCK_TTL_S", 900)

    min_hold_ms = _env_int("AUTO_APPLY_BLOCK_MIN_HOLD_S", 300) * 1000
    pass_streak_to_unblock = _env_int("AUTO_APPLY_UNBLOCK_PASS_STREAK", 3)
    pin_ms = _env_int("AUTO_APPLY_BLOCK_REASON_PIN_S", 600) * 1000
    insuff_mode = _env_str("AUTO_APPLY_BLOCKER_INSUFF_MODE", "fail_open").strip().lower()  # fail_open|fail_closed

    publish_ops = _env_int("AUTO_APPLY_TICK_GATE_PUBLISH_REDIS", _env_int("TICK_GATE_PUBLISH_REDIS", 0)) == 1
    ops_stream = _env_str("AUTO_APPLY_TICK_GATE_STREAM", "ops:auto_apply_tick_gate")
    ops_maxlen = _env_int("AUTO_APPLY_TICK_GATE_STREAM_MAXLEN", 20000)

    cli = _connect_redis(redis_url)

    state_key = f"{prefix}:tick_gate:state"
    # Step P6.3: Use retry_redis_operation for initial state load (handles BusyLoading during startup)
    prev_raw = retry_redis_operation(
        operation=lambda: cli.get(state_key),
        operation_name="get_initial_state",
        max_retries=20,
        base_delay=1.0,
        max_delay=30.0,
        on_final_failure=lambda e: None
    )
    prev = _load_json(prev_raw)

    def loop_once() -> int:
        nonlocal prev
        now_ms = _now_ms()
        rc, gate = _run_gate(metrics_url=metrics_url, window_s=window_s, symbol=symbol)
        nxt = _apply_state_transition(
            prev,
            rc=rc,
            gate=gate,
            now_ms=now_ms,
            min_hold_ms=min_hold_ms,
            pass_streak_to_unblock=pass_streak_to_unblock,
            pin_ms=pin_ms,
            insuff_mode=insuff_mode,
        )
        # Persist state + block keys
        retry_redis_operation(
            operation=lambda: cli.set(state_key, _dump_json(nxt)),
            operation_name="set_state",
            max_retries=5
        )
        retry_redis_operation(
            operation=lambda: _write_block_keys(cli, prefix, nxt, ttl_s=ttl_s),
            operation_name="write_block_keys",
            max_retries=5
        )
        if publish_ops:
            _maybe_publish_ops(cli, ops_stream, nxt, maxlen=ops_maxlen)
        prev = nxt
        # Print one-line status for journald
        sys.stdout.write(_dump_json({
            "ts_ms": now_ms,
            "blocked": bool(nxt.get("blocked")),
            "status": nxt.get("status_class"),
            "reason": nxt.get("pinned_reason", ""),
            "pass_streak": int(nxt.get("pass_streak") or 0),
            "hold_until_ms": int(nxt.get("hold_until_ms") or 0),
            "rc": int(rc),
        }) + "\n")
        sys.stdout.flush()
        return 0

    loop_once()
    if once:
        return 0

    while True:
        time.sleep(max(1, int(interval_s)))
        loop_once()

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
