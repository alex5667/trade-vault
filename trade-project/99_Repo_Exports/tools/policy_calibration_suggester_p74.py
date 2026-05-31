#!/usr/bin/env python3
"""P74: Policy calibration suggester.

Consumes P71 + P72 snapshots from cfg2 (Redis hash) and emits a recommendation
(tighten/loosen warn/block) as a report + a small set of cfg2 keys exported to Prometheus.

It is **advisory**: it does not change any thresholds automatically.

Inputs (cfg2):
- policy_effectiveness_* (P71)
- policy_regime_effectiveness_* (P72)

Outputs:
- reports:policy_calibration_suggestions:p74:last_json
- reports:policy_calibration_suggestions:p74:last_md
- cfg2 keys (for exporter):
    - policy_calibration_suggest_last_ts_ms
    - policy_calibration_suggest_staleness_sec
    - policy_calibration_suggest_warn_action_code
    - policy_calibration_suggest_warn_severity
    - policy_calibration_suggest_warn_share_24h
    - policy_calibration_suggest_block_action_code
    - policy_calibration_suggest_block_severity
    - policy_calibration_suggest_block_share_24h
    - policy_calibration_suggest_unknown_share_24h

Action codes:
-1 = loosen, 0 = no action, +1 = tighten

Env:
- REDIS_URL (required)
- DYN_CFG_KEY (default: settings:dynamic_cfg)
- POLICY_CALIBRATION_SUGGEST_INTERVAL_SEC (default: 300)
- POLICY_CALIBRATION_SUGGEST_STALE_MAX_SEC (default: 7200)

Heuristics (defaults):
- WARN: if share > 0.30 and severity < 0.5 => loosen
        if share > 0.05 and severity > 1.0 => tighten
- BLOCK: if share > 0.05 and severity < 0.5 => loosen
         if share > 0.01 and severity > 1.0 => tighten

Severity is the max of normalized deltas (global + worst-regime):
- expectancy delta (negative is worse)
- precision delta (negative is worse)
- ECE delta (positive is worse)

"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, Optional

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None


def now_ms() -> int:
    return int(time.time() * 1000)


def _f(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v))
    except Exception:
        return default


def _i(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    if isinstance(v, int):
        return v
    try:
        return int(float(str(v)))
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _severity(exp_delta: float, pr_delta: float, ece_delta: float, exp_scale: float, pr_scale: float, ece_scale: float) -> float:
    s_exp = max(0.0, (-exp_delta) / max(1e-9, exp_scale))
    s_pr = max(0.0, (-pr_delta) / max(1e-9, pr_scale))
    s_ece = max(0.0, (ece_delta) / max(1e-9, ece_scale))
    return max(s_exp, s_pr, s_ece)


def build_suggestion(cfg2: Dict[str, Any], *, stale_max_sec: int = 7200, now_ms: Optional[int] = None) -> Dict[str, Any]:
    ts = now_ms if now_ms is not None else int(time.time() * 1000)

    ok_baseline_present = _i(cfg2.get("policy_effectiveness_baseline_ok_present"), 0) == 1

    ok_share = _f(cfg2.get("policy_effectiveness_share_24h_ok"), 0.0)
    warn_share = _f(cfg2.get("policy_effectiveness_share_24h_warn"), 0.0)
    block_share = _f(cfg2.get("policy_effectiveness_share_24h_block"), 0.0)
    unknown_share = _f(cfg2.get("policy_effectiveness_share_24h_unknown"), 0.0)

    # P71 deltas (mode - ok)
    warn_exp_d = _f(cfg2.get("policy_effectiveness_expectancy_r_delta_24h_warn"), 0.0)
    warn_pr_d = _f(cfg2.get("policy_effectiveness_precision_top5p_delta_24h_warn"), 0.0)
    warn_ece_d = _f(cfg2.get("policy_effectiveness_ece_delta_24h_warn"), 0.0)

    block_exp_d = _f(cfg2.get("policy_effectiveness_expectancy_r_delta_24h_block"), 0.0)
    block_pr_d = _f(cfg2.get("policy_effectiveness_precision_top5p_delta_24h_block"), 0.0)
    block_ece_d = _f(cfg2.get("policy_effectiveness_ece_delta_24h_block"), 0.0)

    # P72 worst regime deltas
    w_warn_exp = _f(cfg2.get("policy_regime_effectiveness_worst_warn_expectancy_r_delta"), 0.0)
    w_warn_pr = _f(cfg2.get("policy_regime_effectiveness_worst_warn_precision_top5p_delta"), 0.0)
    w_warn_ece = _f(cfg2.get("policy_regime_effectiveness_worst_warn_ece_delta"), 0.0)

    w_block_exp = _f(cfg2.get("policy_regime_effectiveness_worst_block_expectancy_r_delta"), 0.0)
    w_block_pr = _f(cfg2.get("policy_regime_effectiveness_worst_block_precision_top5p_delta"), 0.0)
    w_block_ece = _f(cfg2.get("policy_regime_effectiveness_worst_block_ece_delta"), 0.0)

    pe_ts = _i(cfg2.get("policy_effectiveness_last_ts_ms"), 0)
    pre_ts = _i(cfg2.get("policy_regime_effectiveness_last_ts_ms"), 0)
    staleness_sec = int(max(0, ts - max(pe_ts, pre_ts)) / 1000) if (pe_ts or pre_ts) else 10**9

    global_notes = []
    if not ok_baseline_present:
        global_notes.append("ok baseline missing: deltas are unreliable (need enough ok-mode samples)")

    if unknown_share > 0.02:
        global_notes.append("unknown_share > 2%: check policy_mode propagation into decision_record")

    if staleness_sec > stale_max_sec:
        global_notes.append("inputs stale: P71/P72 snapshots are too old; do not act on this suggestion")

    # severity (global + worst regime)
    warn_sev = max(
        _severity(warn_exp_d, warn_pr_d, warn_ece_d, exp_scale=0.25, pr_scale=0.05, ece_scale=0.10),
        _severity(w_warn_exp, w_warn_pr, w_warn_ece, exp_scale=0.50, pr_scale=0.10, ece_scale=0.12),
    )
    block_sev = max(
        _severity(block_exp_d, block_pr_d, block_ece_d, exp_scale=0.10, pr_scale=0.03, ece_scale=0.12),
        _severity(w_block_exp, w_block_pr, w_block_ece, exp_scale=0.30, pr_scale=0.08, ece_scale=0.15),
    )

    def decide(mode: str, share: float, sev: float) -> Dict[str, Any]:
        action_code = 0
        notes = []
        if staleness_sec > stale_max_sec:
            notes.append("stale")
            return {"action_code": 0, "severity": sev, "share": share, "notes": notes}

        if not ok_baseline_present:
            notes.append("no action: baseline missing")
            return {"action_code": 0, "severity": sev, "share": share, "notes": notes}

        if mode == "warn":
            if share > 0.30 and sev < 0.5:
                action_code = -1
                notes.append("loosen: high share, low severity")
            elif share > 0.05 and sev > 1.0:
                action_code = 1
                notes.append("tighten: non-trivial share, high severity")
        elif mode == "block":
            if share > 0.05 and sev < 0.5:
                action_code = -1
                notes.append("loosen: high share, low severity")
            elif share > 0.01 and sev > 1.0:
                action_code = 1
                notes.append("tighten: non-trivial share, high severity")
        return {"action_code": action_code, "severity": sev, "share": share, "notes": notes}

    suggestion = {
        "ts_ms": ts,
        "staleness_sec": staleness_sec,
        "inputs_stale": int(staleness_sec > stale_max_sec),
        "ok_baseline_present": int(ok_baseline_present),
        "global_notes": global_notes,
        "shares": {
            "ok": ok_share,
            "warn": warn_share,
            "block": block_share,
            "unknown": unknown_share,
        },
        "warn": decide("warn", warn_share, warn_sev),
        "block": decide("block", block_share, block_sev),
        "inputs": {
            "p71": {
                "warn": {"expectancy_r_delta": warn_exp_d, "precision_top5p_delta": warn_pr_d, "ece_delta": warn_ece_d},
                "block": {"expectancy_r_delta": block_exp_d, "precision_top5p_delta": block_pr_d, "ece_delta": block_ece_d},
                "last_ts_ms": pe_ts,
            },
            "p72": {
                "warn": {"worst_expectancy_r_delta": w_warn_exp, "worst_precision_top5p_delta": w_warn_pr, "worst_ece_delta": w_warn_ece},
                "block": {"worst_expectancy_r_delta": w_block_exp, "worst_precision_top5p_delta": w_block_pr, "worst_ece_delta": w_block_ece},
                "last_ts_ms": pre_ts,
            },
        },
    }

    # Clamp shares into [0,1] for safety
    for k in ("ok", "warn", "block", "unknown"):
        suggestion["shares"][k] = _clamp(float(suggestion["shares"][k]), 0.0, 1.0)

    return suggestion


def render_markdown(s: Dict[str, Any]) -> str:
    lines = []
    lines.append("P74 — policy calibration suggestions")
    lines.append("")
    lines.append(f"ts_ms: {s['ts_ms']}")
    lines.append(f"staleness_sec: {s['staleness_sec']}")
    lines.append(f"inputs_stale: {s.get('inputs_stale',0)}")
    lines.append(f"ok_baseline_present: {s.get('ok_baseline_present',0)}")
    lines.append("")
    sh = s.get("shares", {})
    lines.append(f"shares_24h: ok={sh.get('ok',0):.3f}, warn={sh.get('warn',0):.3f}, block={sh.get('block',0):.3f}, unknown={sh.get('unknown',0):.3f}")
    lines.append("")

    def fmt_mode(name: str):
        m = s.get(name, {})
        lines.append(f"{name}: action_code={m.get('action_code')}, severity={m.get('severity',0):.3f}, share={m.get('share',0):.3f}")
        for n in (m.get("notes") or []):
            lines.append(f"  - {n}")

    fmt_mode("warn")
    fmt_mode("block")

    if s.get("global_notes"):
        lines.append("")
        lines.append("global_notes:")
        for n in s["global_notes"]:
            lines.append(f"- {n}")

    return "\n".join(lines) + "\n"


def write_outputs(r, dyn_key: str, s: Dict[str, Any]) -> None:
    # reports
    r.set("reports:policy_calibration_suggestions:p74:last_json", json.dumps(s, sort_keys=True))
    r.set("reports:policy_calibration_suggestions:p74:last_md", render_markdown(s))

    # cfg2 exported keys
    h = {
        "policy_calibration_suggest_last_ts_ms": str(s.get("ts_ms", 0)),
        "policy_calibration_suggest_staleness_sec": str(s.get("staleness_sec", 0)),
        "policy_calibration_suggest_inputs_stale": str(_i(s.get("inputs_stale", 0), 0)),
        "policy_calibration_suggest_ok_baseline_present": str(_i(s.get("ok_baseline_present", 0), 0)),
        "policy_calibration_suggest_warn_action_code": str(_i(s.get("warn", {}).get("action_code"), 0)),
        "policy_calibration_suggest_warn_severity": str(_f(s.get("warn", {}).get("severity"), 0.0)),
        "policy_calibration_suggest_warn_share_24h": str(_f(s.get("shares", {}).get("warn"), 0.0)),
        "policy_calibration_suggest_block_action_code": str(_i(s.get("block", {}).get("action_code"), 0)),
        "policy_calibration_suggest_block_severity": str(_f(s.get("block", {}).get("severity"), 0.0)),
        "policy_calibration_suggest_block_share_24h": str(_f(s.get("shares", {}).get("block"), 0.0)),
        "policy_calibration_suggest_unknown_share_24h": str(_f(s.get("shares", {}).get("unknown"), 0.0)),
    }
    r.hset(dyn_key, mapping=h)


def _require_redis() -> None:
    if redis is None:
        raise SystemExit("redis module not available; pip install redis")


_CONNECT_TIMEOUT = int(os.getenv("REDIS_CONNECT_TIMEOUT_SEC", "15"))
_SOCKET_TIMEOUT = int(os.getenv("REDIS_SOCKET_TIMEOUT_SEC", "15"))


def _make_redis(redis_url: str) -> "redis.Redis":
    """Create a Redis client with persistent connection pool and TCP keepalive."""
    _require_redis()
    return redis.Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=_CONNECT_TIMEOUT,
        socket_timeout=_SOCKET_TIMEOUT,
        socket_keepalive=True,
        retry_on_timeout=True,
        health_check_interval=30,
    )


def _ensure_connected(r: "redis.Redis", *, max_attempts: int = 10) -> None:
    """Ping Redis with exponential backoff retry."""
    delay = 1.0
    for attempt in range(max_attempts):
        try:
            r.ping()
            return
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            print(f"⚠️ Redis not ready (attempt {attempt + 1}/{max_attempts}): {e}. Retry in {delay:.0f}s...")
            time.sleep(delay)
            delay = min(delay * 2, 10.0)


def run_once(r: "redis.Redis", dyn_key: str, stale_max_sec: int) -> None:
    cfg2 = r.hgetall(dyn_key)
    s = build_suggestion(cfg2, stale_max_sec=stale_max_sec)
    write_outputs(r, dyn_key, s)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="run once and exit")
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        raise SystemExit("REDIS_URL is required")
    dyn_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")

    interval = int(os.getenv("POLICY_CALIBRATION_SUGGEST_INTERVAL_SEC", "300"))
    stale_max_sec = int(os.getenv("POLICY_CALIBRATION_SUGGEST_STALE_MAX_SEC", "7200"))

    r = _make_redis(redis_url)

    if args.once:
        try:
            _ensure_connected(r)
            run_once(r, dyn_key, stale_max_sec)
        except Exception as e:
            print(f"⚠️ [p74] error during --once execution: {e}")
            raise SystemExit(1)
        return

    _ensure_connected(r)

    while True:
        try:
            run_once(r, dyn_key, stale_max_sec)
        except Exception as e:
            # best-effort; do not crash the whole monitor loop
            print(f"[p74] error: {e}")
            # try to reconnect on next cycle
            try:
                r.ping()
            except Exception:
                try:
                    r = _make_redis(redis_url)
                    _ensure_connected(r)
                except Exception as re_e:
                    print(f"[p74] reconnect failed: {re_e}")
        time.sleep(max(5, interval))


if __name__ == "__main__":
    main()
