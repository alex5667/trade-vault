from __future__ import annotations

# -*- coding: utf-8 -*-
"""conf_score_guardrails_autopromo_controller_v1.py

World practice: automated, gated promotion with canary window + auto rollback.

This sits AFTER the two-phase pipeline:

  - STAGE:
      python -m orderflow_services.conf_score_guardrails_apply_v1 --stage 1 --apply 1 ...
    writes:
      <bundle_dir>/bundle_<ts>.json
      <bundle_dir>/staged.json   (candidate pointer)
      Redis staged keys: cfg:crypto_of:overrides_staged:{SYMBOL} (optional)

  - PROMOTE (manual / timer):
      python -m orderflow_services.conf_score_guardrails_promote_v1 --apply 1 ...
    promotes staged -> live when health gates pass.

What this controller adds (world-practice next step):
  1) Promote CANARY only (bounded blast radius), WITHOUT flipping live pointer.
  2) Observe an evaluation window.
  3) Compare health/SLO metrics vs baseline snapshot and decide:
       - Full promote (pointer update + clear staged), OR
       - Rollback to current stable bundle.
  4) Record a durable state JSON for exporter/alerts/runbook.

Design constraints:
  - uses stdlib only; interacts with promote/rollback via subprocess calls.
  - relies on the same POSIX flock lock used by stage/promote to avoid multi-writer races.
  - does not assume your Prometheus access; reads health JSON written by your live loop
    (CONF_SCORE_GUARD_HEALTH_STATE_PATH).

Typical deployment:
  - keep STAGE timer enabled
  - disable PROMOTE timer
  - enable AUTOPROMO timer (this file) to control promote/rollback

Env (recommended):
  REDIS_URL
  CONF_SCORE_GUARD_BUNDLE_DIR
  CONF_SCORE_GUARD_BUNDLE_STAGED_POINTER
  CONF_SCORE_GUARD_BUNDLE_POINTER
  CONF_SCORE_GUARD_LOCK_PATH
  CONF_SCORE_GUARD_HEALTH_STATE_PATH,
""",
import argparse
import fcntl
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from orderflow_services.research_guard_blocker_v1 import assert_research_guard_open
from orderflow_services.strategy_research_stats_gate_v1 import evaluate_strategy_research_stats_gate, gate_check_message
from utils.time_utils import get_ny_time_millis

# ----------------------- utils -----------------------

def now_ms() -> int:
    return get_ny_time_millis()


def load_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        obj = json.load(f)
    return obj if isinstance(obj, dict) else {}


def load_json_if_exists(path: str) -> dict[str, Any]:
    try:
        if path and os.path.exists(path):
            return load_json(path)
    except Exception:
        return {}
    return {}


def atomic_write_json(path: str, obj: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def acquire_flock(lock_path: str):
    """Single-writer guard used across stage/promote/autopromo.""",
    try:
        os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
        f = open(lock_path, "w", encoding="utf-8")
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.write(json.dumps({"pid": os.getpid(), "ts_ms": now_ms()}, ensure_ascii=False))
        f.flush()
        return f
    except Exception:
        try:
            f.close()  # type: ignore
        except Exception:
            pass
        return None


def tail(s: str, n: int = 1200) -> str:
    if not s:
        return ""
    return s[-n:]


def safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return 1.0 if x else 0.0
        return float(x)
    except Exception:
        return None


def extract_health_metrics(health: dict[str, Any]) -> dict[str, Any]:
    """,
    Best-effort normalization (same spirit as promote_v1).
    Accepts either flat dict or nested dict under GLOBAL/status/metrics.
    """,
    obj = health if isinstance(health, dict) else {}
    global_section = obj.get("GLOBAL") if isinstance(obj.get("GLOBAL"), dict) else {}
    status_section = obj.get("status") if isinstance(obj.get("status"), dict) else {}

    def pick(d: dict[str, Any], keys: tuple[str, ...]) -> float | None:
        for k in keys:
            v = safe_float(d.get(k))
            if v is not None:
                return v
        return None

    ts_ms = pick(obj, ("ts_ms", "timestamp_ms", "time_ms")) or pick(global_section, ("ts_ms", "timestamp_ms"))
    degrade = pick(obj, ("degrade", "degraded", "is_degraded"))
    if degrade is None:
        degrade = pick(status_section, ("degrade", "degraded", "is_degraded"))
    ece_cal = pick(obj, ("ece_cal", "live_ece_cal", "ece_after")) or pick(global_section, ("ece_cal", "live_ece_cal"))
    brier_cal = pick(obj, ("brier_cal", "live_brier_cal", "brier_after")) or pick(global_section, ("brier_cal", "live_brier_cal"))
    # optional sample size
    # optional sample size
    n = pick(obj, ("n", "n_samples", "count")) or pick(global_section, ("n", "n_samples", "count"))

    arms_section = obj.get("arms") if isinstance(obj.get("arms"), dict) else {}
    arms_delta = arms_section.get("delta") if isinstance(arms_section.get("delta"), dict) else {}
    if not arms_delta and isinstance(global_section.get("delta"), dict):
        arms_delta = global_section.get("delta")
    arm_delta_ece = pick(arms_delta, ("ece_cal", "delta_ece_cal", "ece_delta", "delta_ece"))
    arm_delta_brier = pick(arms_delta, ("brier_cal", "delta_brier_cal", "brier_delta", "delta_brier"))
    arm_n = pick(arms_delta, ("n", "count", "n_samples"))

    cohorts_section = obj.get("cohorts") if isinstance(obj.get("cohorts"), dict) else {}
    if not cohorts_section and isinstance(global_section.get("cohorts"), dict):
        cohorts_section = global_section.get("cohorts")
    agg = cohorts_section.get("agg") if isinstance(cohorts_section.get("agg"), dict) else {}
    worst = cohorts_section.get("worst") if isinstance(cohorts_section.get("worst"), dict) else {}
    cohort_delta_ece_wmean = pick(agg, ("delta_ece_cal_wmean", "delta_ece_wmean"))
    cohort_delta_brier_wmean = pick(agg, ("delta_brier_cal_wmean", "delta_brier_wmean"))
    cohort_delta_ece_max = pick(worst, ("delta_ece_cal_max", "delta_ece_max"))
    cohort_delta_brier_max = pick(worst, ("delta_brier_cal_max", "delta_brier_max"))

    out = {
        "ts_ms": int(ts_ms) if ts_ms is not None else None,
        "degrade": int(degrade) if degrade is not None else 0,
        "ece_cal": float(ece_cal) if ece_cal is not None else None,
        "brier_cal": float(brier_cal) if brier_cal is not None else None,
        "n": int(n) if n is not None else None,
        "arm_delta_ece_cal": float(arm_delta_ece) if arm_delta_ece is not None else None,
        "arm_delta_brier_cal": float(arm_delta_brier) if arm_delta_brier is not None else None,
        "arm_n": int(arm_n) if arm_n is not None else None,
        "cohort_delta_ece_cal_wmean": float(cohort_delta_ece_wmean) if cohort_delta_ece_wmean is not None else None,
        "cohort_delta_brier_cal_wmean": float(cohort_delta_brier_wmean) if cohort_delta_brier_wmean is not None else None,
        "cohort_delta_ece_cal_max": float(cohort_delta_ece_max) if cohort_delta_ece_max is not None else None,
        "cohort_delta_brier_cal_max": float(cohort_delta_brier_max) if cohort_delta_brier_max is not None else None,
    }
    return out


def age_sec(ts_ms: int | None, now: int) -> float | None:
    if ts_ms is None:
        return None
    return max(0.0, (now - int(ts_ms)) / 1000.0)


# ----------------------- policy pointers -----------------------

@dataclass
class Candidate:
    bundle_file: str
    version: str  # best-effort
    ts_ms: int | None


def read_staged_pointer(bundle_dir: str, staged_pointer_path: str) -> Candidate | None:
    pointer_path = staged_pointer_path or str(Path(bundle_dir) / "staged.json")
    if not os.path.exists(pointer_path):
        return None
    p = load_json(pointer_path)
    # expected fields (from previous patch): staged_file, staged_version, ts_ms
    bundle_file = p.get("staged_file") or p.get("file") or p.get("bundle_file")
    if not bundle_file:
        return None
    # relative -> bundle_dir
    if not os.path.isabs(bundle_file):
        bundle_file = str(Path(bundle_dir) / bundle_file)

    ver = str(p.get("staged_version") or p.get("version") or Path(bundle_file).name)
    ts = p.get("staged_ts_ms") or p.get("ts_ms") or None
    try:
        ts_i = int(ts) if ts is not None else None
    except Exception:
        ts_i = None
    return Candidate(bundle_file=bundle_file, version=ver, ts_ms=ts_i)


def read_current_pointer(bundle_dir: str, pointer_path: str) -> dict[str, Any]:
    path = pointer_path or str(Path(bundle_dir) / "current.json")
    return load_json_if_exists(path)


# ----------------------- evaluation -----------------------

@dataclass
class EvalResult:
    ok: bool
    reasons: list[str]
    delta: dict[str, Any]
    current: dict[str, Any]
    baseline: dict[str, Any]


def evaluate_canary(
    *,
    baseline: dict[str, Any],
    current: dict[str, Any],
    now: int,
    max_health_age_sec: int,
    min_n: int,
    max_delta_ece: float,
    max_delta_brier: float,
    max_arm_delta_ece: float,
    max_arm_delta_brier: float,
    max_cohort_delta_ece_wmean: float,
    max_cohort_delta_brier_wmean: float,
    max_cohort_delta_ece_max: float,
    max_cohort_delta_brier_max: float,
    allow_missing: bool,
) -> EvalResult:
    reasons: list[str] = []
    ok = True

    # freshness + degrade
    ts = current.get("ts_ms")
    cur_age = age_sec(ts, now)
    if cur_age is None:
        if not allow_missing:
            ok = False
            reasons.append("missing_health_ts")
    else:
        if cur_age > float(max_health_age_sec):
            ok = False
            reasons.append("stale_health")

    if int(current.get("degrade") or 0) > 0:
        ok = False
        reasons.append("degrade_active")

    # sample size (optional)
    if current.get("n") is not None:
        if int(current["n"]) < int(min_n):
            ok = False
            reasons.append("insufficient_n")

    # compare deltas (use cal metrics; baseline snapshot is pre-canary)
    delta: dict[str, Any] = {}

    paired_used = False
    arm_n = current.get("arm_n")
    if arm_n is not None and int(arm_n) < int(min_n):
        ok = False
        reasons.append("insufficient_arm_n")

    a_dece = safe_float(current.get("arm_delta_ece_cal"))
    a_dbrier = safe_float(current.get("arm_delta_brier_cal"))
    if a_dece is not None:
        paired_used = True
        delta["arm_delta_ece_cal"] = float(a_dece)
        if float(a_dece) > float(max_arm_delta_ece):
            ok = False
            reasons.append("arm_ece_regression")
    if a_dbrier is not None:
        paired_used = True
        delta["arm_delta_brier_cal"] = float(a_dbrier)
        if float(a_dbrier) > float(max_arm_delta_brier):
            ok = False
            reasons.append("arm_brier_regression")

    cwe = safe_float(current.get("cohort_delta_ece_cal_wmean"))
    cwb = safe_float(current.get("cohort_delta_brier_cal_wmean"))
    cme = safe_float(current.get("cohort_delta_ece_cal_max"))
    cmb = safe_float(current.get("cohort_delta_brier_cal_max"))
    if cwe is not None:
        paired_used = True
        delta["cohort_delta_ece_cal_wmean"] = float(cwe)
        if float(cwe) > float(max_cohort_delta_ece_wmean):
            ok = False
            reasons.append("cohort_ece_wmean_regression")
    if cwb is not None:
        paired_used = True
        delta["cohort_delta_brier_cal_wmean"] = float(cwb)
        if float(cwb) > float(max_cohort_delta_brier_wmean):
            ok = False
            reasons.append("cohort_brier_wmean_regression")
    if cme is not None:
        paired_used = True
        delta["cohort_delta_ece_cal_max"] = float(cme)
        if float(cme) > float(max_cohort_delta_ece_max):
            ok = False
            reasons.append("cohort_ece_max_regression")
    if cmb is not None:
        paired_used = True
        delta["cohort_delta_brier_cal_max"] = float(cmb)
        if float(cmb) > float(max_cohort_delta_brier_max):
            ok = False
            reasons.append("cohort_brier_max_regression")

    def delta_metric(k: str) -> float | None:
        b = safe_float(baseline.get(k))
        c = safe_float(current.get(k))
        if b is None or c is None:
            return None
        return float(c) - float(b)

    d_ece = delta_metric("ece_cal")
    d_brier = delta_metric("brier_cal")
    if d_ece is not None:
        delta["ece_cal"] = d_ece
        if d_ece > float(max_delta_ece):
            # fallback only if paired metrics not available, or always?
            # User requirement says "baseline->current comparison stays as fallback".
            # So if paired_used is True, we might skip this?
            # Actually, the diff says: if not allow_missing and not paired_used: reasons.append("missing_ece_cal")
            # But the diff logic logic for checks seems to apply both if available?
            # Ah, the user request says "MAX_ARM_DELTA_* ... baseline->current comparison stays as fallback (legacy)".
            # In proper implementation, if we have better metrics (paired), we should rely on them.
            # But let's follow the diff's logic if possible.
            # The diff logic for missing checks:
            # -        if not allow_missing:
            # +        if not allow_missing and not paired_used:
            # This implies if paired metrics are used, we don't strict-require baseline-current delta.
            pass

            # The previous code checked d_ece > max_delta_ece.
            # We should probably still check it if it exists, to be safe.
            if d_ece > float(max_delta_ece):
                 ok = False
                 reasons.append("ece_regression")

    else:
        if not allow_missing and not paired_used:
            reasons.append("missing_ece_cal")

    if d_brier is not None:
        delta["brier_cal"] = d_brier
        if d_brier > float(max_delta_brier):
            ok = False
            reasons.append("brier_regression")
    else:
        if not allow_missing and not paired_used:
            reasons.append("missing_brier_cal")

    return EvalResult(ok=ok, reasons=reasons, delta=delta, current=current, baseline=baseline)


# ----------------------- subprocess helpers -----------------------

def run_module(args: list[str], timeout_sec: int = 120) -> tuple[int, str, str]:
    """Run `python -m <module> ...` and return (rc, stdout, stderr).""",
    p = subprocess.run(
        args,
        capture_output=True,
        encoding="utf-8",
        timeout=timeout_sec,
    )
    return p.returncode, p.stdout or "", p.stderr or ""


def promote_canary(
    *,
    bundle_dir: str,
    redis_url: str,
    key_prefix: str,
    staged_key_prefix: str,
    health_state_path: str,
    staged_pointer: str,
    pointer_path: str,
    promote_state_path: str,
    lock_path: str,
    apply: int,
) -> tuple[int, str, str]:
    cmd = [
        "python", "-m", "orderflow_services.conf_score_guardrails_promote_v1",
        "--bundle-dir", bundle_dir,
        "--redis-url", redis_url,
        "--key-prefix", key_prefix,
        "--staged-key-prefix", staged_key_prefix,
        "--staged-pointer-path", staged_pointer,
        "--pointer-path", pointer_path,
        "--health-state-path", health_state_path,
        "--max-health-age-sec", "600",
        "--promote-canary-only", "1",
        "--apply", str(int(apply)),
        "--promote-pointer", "0",
        "--clear-staged", "0",
        "--promote-state-path", promote_state_path,
        "--lock-path", lock_path,
    ]
    return run_module(cmd, timeout_sec=180)


def promote_full(
    *,
    bundle_dir: str,
    redis_url: str,
    key_prefix: str,
    staged_key_prefix: str,
    health_state_path: str,
    staged_pointer: str,
    pointer_path: str,
    promote_state_path: str,
    lock_path: str,
    apply: int,
    clear_staged: int,
    promote_pointer: int,
) -> tuple[int, str, str]:
    cmd = [
        "python", "-m", "orderflow_services.conf_score_guardrails_promote_v1",
        "--bundle-dir", bundle_dir,
        "--redis-url", redis_url,
        "--key-prefix", key_prefix,
        "--staged-key-prefix", staged_key_prefix,
        "--staged-pointer-path", staged_pointer,
        "--pointer-path", pointer_path,
        "--health-state-path", health_state_path,
        "--max-health-age-sec", "600",
        "--promote-canary-only", "0",
        "--apply", str(int(apply)),
        "--promote-pointer", str(int(promote_pointer)),
        "--clear-staged", str(int(clear_staged)),
        "--promote-state-path", promote_state_path,
        "--lock-path", lock_path,
    ]
    return run_module(cmd, timeout_sec=240)


def rollback_to_current(
    *,
    bundle_dir: str,
    redis_url: str,
    state_path: str,
    lock_path: str,
    apply: int,
) -> tuple[int, str, str]:
    # World-practice rollback: re-apply CURRENT bundle decisions into live keys.
    # This should safely restore canary changes because only guardrails keys are affected.
    cmd = [
        "python", "-m", "orderflow_services.conf_score_guardrails_bundle_rollback_v1",
        "--bundle-dir", bundle_dir,
        "--target", "current",
        "--apply", str(int(apply)),
        "--redis-url", redis_url,
        "--state-path", state_path,
        "--lock-path", lock_path,
    ]
    return run_module(cmd, timeout_sec=240)


# ----------------------- main state machine -----------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bundle-dir", default=os.getenv("CONF_SCORE_GUARD_BUNDLE_DIR", "/tmp/conf_score_guard_bundles"))
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    ap.add_argument("--key-prefix", default=os.getenv("CONF_SCORE_GUARD_KEY_PREFIX", "cfg:crypto_of:overrides:"))
    ap.add_argument("--staged-key-prefix", default=os.getenv("CONF_SCORE_GUARD_STAGED_KEY_PREFIX", "cfg:crypto_of:overrides_staged:"))

    ap.add_argument("--staged-pointer-path", default=os.getenv("CONF_SCORE_GUARD_BUNDLE_STAGED_POINTER", ""))
    ap.add_argument("--pointer-path", default=os.getenv("CONF_SCORE_GUARD_BUNDLE_POINTER", ""))
    ap.add_argument("--state-path", default=os.getenv("CONF_SCORE_GUARD_AUTOPROMO_STATE_PATH", "/tmp/conf_score_guard_autopromo_state.json"))
    ap.add_argument("--promote-state-path", default=os.getenv("CONF_SCORE_GUARD_PROMOTE_STATE_PATH", "/tmp/conf_score_guard_promote_state.json"))
    ap.add_argument("--health-state-path", default=os.getenv("CONF_SCORE_GUARD_HEALTH_STATE_PATH", "/tmp/conf_cal_proof_state.json"))
    ap.add_argument("--lock-path", default=os.getenv("CONF_SCORE_GUARD_LOCK_PATH", "/tmp/conf_score_guardrails.lock"))

    ap.add_argument("--observe-window-sec", type=int, default=int(os.getenv("CONF_SCORE_GUARD_CANARY_OBSERVE_SEC", "900")))
    ap.add_argument("--max-health-age-sec", type=int, default=int(os.getenv("CONF_SCORE_GUARD_MAX_HEALTH_AGE_SEC", "600")))
    ap.add_argument("--min-n", type=int, default=int(os.getenv("CONF_SCORE_GUARD_MIN_N", "200")))
    ap.add_argument("--max-delta-ece", type=float, default=float(os.getenv("CONF_SCORE_GUARD_MAX_DELTA_ECE", "0.005")))
    ap.add_argument("--max-delta-brier", type=float, default=float(os.getenv("CONF_SCORE_GUARD_MAX_DELTA_BRIER", "0.005")))
    ap.add_argument("--max-arm-delta-ece", type=float, default=float(os.getenv("CONF_SCORE_GUARD_MAX_ARM_DELTA_ECE", "0.003")))
    ap.add_argument("--max-arm-delta-brier", type=float, default=float(os.getenv("CONF_SCORE_GUARD_MAX_ARM_DELTA_BRIER", "0.003")))
    ap.add_argument("--max-cohort-delta-ece-wmean", type=float, default=float(os.getenv("CONF_SCORE_GUARD_MAX_COHORT_DELTA_ECE_WMEAN", "0.002")))
    ap.add_argument("--max-cohort-delta-brier-wmean", type=float, default=float(os.getenv("CONF_SCORE_GUARD_MAX_COHORT_DELTA_BRIER_WMEAN", "0.002")))
    ap.add_argument("--max-cohort-delta-ece-max", type=float, default=float(os.getenv("CONF_SCORE_GUARD_MAX_COHORT_DELTA_ECE_MAX", "0.01")))
    ap.add_argument("--max-cohort-delta-brier-max", type=float, default=float(os.getenv("CONF_SCORE_GUARD_MAX_COHORT_DELTA_BRIER_MAX", "0.01")))
    ap.add_argument("--allow-missing", type=int, default=int(os.getenv("CONF_SCORE_GUARD_ALLOW_MISSING", "0")))

    ap.add_argument("--apply", type=int, default=int(os.getenv("CONF_SCORE_GUARD_AUTOPROMO_APPLY", "1")))
    ap.add_argument("--clear-staged-on-success", type=int, default=int(os.getenv("CONF_SCORE_GUARD_AUTOPROMO_CLEAR_STAGED", "1")))
    ap.add_argument("--promote-pointer-on-success", type=int, default=int(os.getenv("CONF_SCORE_GUARD_AUTOPROMO_PROMOTE_POINTER", "1")))
    ap.add_argument("--rollback-on-fail", type=int, default=int(os.getenv("CONF_SCORE_GUARD_AUTOPROMO_ROLLBACK_ON_FAIL", "1")))

    args = ap.parse_args()

    # Research guard hard-gate (P5.2): autopromo is effectively an automated promote path,
    # so it must stop before taking the lock or touching staged/live pointers when blocker is active.
    if int(args.apply) == 1 and os.getenv("ENABLE_STRATEGY_RESEARCH_GUARD_HARD_GATE", "0") == "1":
        assert_research_guard_open(
            args.redis_url,
            purpose="conf_score_guardrails_autopromo_controller",
            stage_mode=False,
        )

    # Strategy research stats gate (P6.1): fail-open; blocks automated promote path
    # when gate_mode=hard and research stats fail thresholds.
    if int(args.apply) == 1 and os.getenv("ENABLE_STRATEGY_RESEARCH_STATS_GATE", "1") in ("1", "true", "True", "yes", "on"):
        gate = evaluate_strategy_research_stats_gate(
            args.redis_url,
            os.getenv("STRATEGY_RESEARCH_STATS_BLOCKER_KEY", "cfg:strategy_research_stats:blocker:v1"),
            os.getenv("STRATEGY_RESEARCH_STATS_SUMMARY_KEY", "metrics:strategy_research_stats:last"),
            max_age_sec=float(os.getenv("STRATEGY_RESEARCH_STATS_MAX_AGE_SEC", "129600") or 129600),
            fail_closed_missing=int(os.getenv("STRATEGY_RESEARCH_STATS_FAIL_CLOSED_MISSING", "0") or 0),
        )
        if (gate.get("status")) in ("block", "invalid"):
            # Autopromo: treat both hard block and invalid state as abort
            return 0
        if (gate.get("status")) == "soft":
            print(gate_check_message(gate, purpose="conf_score_guardrails_autopromo_controller"))

    lock = acquire_flock(args.lock_path)
    if lock is None:
        # Another writer is active; exit quietly (timer will retry).
        return 0

    t0 = now_ms()
    state_path = args.state_path
    state = load_json_if_exists(state_path)
    state.setdefault("history", [])
    state["ts_ms"] = t0
    state["pid"] = os.getpid()

    # Find candidate
    cand = read_staged_pointer(args.bundle_dir, args.staged_pointer_path)
    if cand is None:
        state["phase"] = "idle"
        state["candidate"] = None
        atomic_write_json(state_path, state)
        return 0

    state["candidate"] = {"bundle_file": cand.bundle_file, "version": cand.version, "ts_ms": cand.ts_ms}
    state.setdefault("phase", "idle")

    # If candidate changes -> reset cycle
    prev_cand = (state.get("cycle") or {}).get("version")
    if prev_cand != cand.version:
        state["cycle"] = {
            "version": cand.version,
            "started_ts_ms": t0,
            "baseline": None,
            "canary": None,
            "last_eval": None,
        }
        state["phase"] = "baseline"

    cycle = state["cycle"]

    # baseline snapshot
    if state["phase"] == "baseline":
        health_raw = load_json_if_exists(args.health_state_path)
        base = extract_health_metrics(health_raw)
        cycle["baseline"] = {"ts_ms": t0, "health": base}
        state["phase"] = "canary_promote"
        state["history"].append({"ts_ms": t0, "action": "baseline_snapshot", "baseline": base})

    # promote canary
    if state["phase"] == "canary_promote":
        rc, out, err = promote_canary(
            bundle_dir=args.bundle_dir,
            redis_url=args.redis_url,
            key_prefix=args.key_prefix,
            staged_key_prefix=args.staged_key_prefix,
            health_state_path=args.health_state_path,
            staged_pointer=args.staged_pointer_path or str(Path(args.bundle_dir) / "staged.json"),
            pointer_path=args.pointer_path or str(Path(args.bundle_dir) / "current.json"),
            promote_state_path=args.promote_state_path,
            lock_path=args.lock_path,
            apply=args.apply,
        )
        cycle["canary"] = {
            "promote_ts_ms": t0,
            "observe_until_ms": t0 + int(args.observe_window_sec) * 1000,
            "rc": rc,
        }
        state["history"].append(
            {"ts_ms": t0, "action": "promote_canary", "rc": rc, "stdout_tail": tail(out), "stderr_tail": tail(err)}
        )
        if rc != 0:
            state["phase"] = "blocked"
            cycle["last_eval"] = {"ok": False, "reasons": ["canary_promote_failed"], "rc": rc}
            atomic_write_json(state_path, state)
            return 0
        state["phase"] = "observing"

    # observing window
    if state["phase"] == "observing":
        can = cycle.get("canary") or {}
        until_ms = int(can.get("observe_until_ms") or 0)
        if t0 < until_ms:
            # still observing
            state["observing_remaining_sec"] = max(0, int((until_ms - t0) / 1000))
            atomic_write_json(state_path, state)
            return 0
        state.pop("observing_remaining_sec", None)
        state["phase"] = "evaluate"

    # evaluate
    if state["phase"] == "evaluate":
        baseline = ((cycle.get("baseline") or {}).get("health")) or {}
        health_raw = load_json_if_exists(args.health_state_path)
        cur = extract_health_metrics(health_raw)
        res = evaluate_canary(
            baseline=baseline,
            current=cur,
            now=t0,
            max_health_age_sec=args.max_health_age_sec,
            min_n=args.min_n,
            max_delta_ece=args.max_delta_ece,
            max_delta_brier=args.max_delta_brier,
            max_arm_delta_ece=args.max_arm_delta_ece,
            max_arm_delta_brier=args.max_arm_delta_brier,
            max_cohort_delta_ece_wmean=args.max_cohort_delta_ece_wmean,
            max_cohort_delta_brier_wmean=args.max_cohort_delta_brier_wmean,
            max_cohort_delta_ece_max=args.max_cohort_delta_ece_max,
            max_cohort_delta_brier_max=args.max_cohort_delta_brier_max,
            allow_missing=bool(args.allow_missing),
        )
        cycle["last_eval"] = {
            "ts_ms": t0,
            "ok": res.ok,
            "reasons": res.reasons,
            "delta": res.delta,
            "baseline": res.baseline,
            "current": res.current,
        }
        state["history"].append({"ts_ms": t0, "action": "evaluate", "ok": res.ok, "reasons": res.reasons, "delta": res.delta})
        state["phase"] = "promote_full" if res.ok else "rollback"

    # promote full or rollback
    if state["phase"] == "promote_full":
        rc, out, err = promote_full(
            bundle_dir=args.bundle_dir,
            redis_url=args.redis_url,
            key_prefix=args.key_prefix,
            staged_key_prefix=args.staged_key_prefix,
            health_state_path=args.health_state_path,
            staged_pointer=args.staged_pointer_path or str(Path(args.bundle_dir) / "staged.json"),
            pointer_path=args.pointer_path or str(Path(args.bundle_dir) / "current.json"),
            promote_state_path=args.promote_state_path,
            lock_path=args.lock_path,
            apply=args.apply,
            clear_staged=args.clear_staged_on_success,
            promote_pointer=args.promote_pointer_on_success,
        )
        state["history"].append({"ts_ms": t0, "action": "promote_full", "rc": rc, "stdout_tail": tail(out), "stderr_tail": tail(err)})
        if rc == 0:
            state["phase"] = "promoted"
        else:
            state["phase"] = "blocked"
            cycle["last_eval"] = {"ok": False, "reasons": ["full_promote_failed"], "rc": rc}

    if state["phase"] == "rollback":
        if int(args.rollback_on_fail) == 0:
            state["phase"] = "blocked"
            cycle["last_eval"] = {"ok": False, "reasons": ["rollback_disabled"]}
        else:
            rc, out, err = rollback_to_current(
                bundle_dir=args.bundle_dir,
                redis_url=args.redis_url,
                state_path=state_path,
                lock_path=args.lock_path,
                apply=args.apply,
            )
            state["history"].append({"ts_ms": t0, "action": "rollback_current", "rc": rc, "stdout_tail": tail(out), "stderr_tail": tail(err)})
            state["phase"] = "rolled_back" if rc == 0 else "blocked"
            if rc != 0:
                cycle["last_eval"] = {"ok": False, "reasons": ["rollback_failed"], "rc": rc}

    atomic_write_json(state_path, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
