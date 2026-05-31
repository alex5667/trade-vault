from __future__ import annotations

"""conf_score_guardrails_apply_v1.py

World practice: close-the-loop guardrails for confidence scoring.

Consumes a drift report JSON produced by:
  - ml_analysis/tools/confidence_parts_drift_report_v1.py

Decides and optionally applies per-symbol overrides (Redis JSON at
cfg:crypto_of:overrides:{SYMBOL}) for:
  - confidence_score_freeze (0/1)
  - confidence_score_scale (float)

This is intentionally conservative:
  - it never removes or overwrites unrelated override keys
  - it writes only the allowlisted keys above plus non-critical metadata

Policy Bundles:
  If --bundle-enable=1, decisions are written to immutable JSON files in --bundle-dir.
  A 'current.json' pointer is updated to point to the latest applied bundle.
  Single-writer guard is enforced via flock on --lock-path.

Environment:
  REDIS_URL (optional)
  CONF_SCORE_GUARD_STATE_PATH (optional)
  CONF_SCORE_GUARD_BUNDLE_ENABLE (optional)

Example:
  python -m orderflow_services.conf_score_guardrails_apply_v1 \
    --drift-report /tmp/conf_parts_drift.json \
    --apply 1 --redis-url redis://localhost:6379/0
"""

import argparse
import fcntl
import glob
import hashlib
import json
import os
import sys
import zlib
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from typing import Any, ContextManager

from orderflow_services.research_guard_blocker_v1 import assert_research_guard_open, check_research_guard_blocker
from orderflow_services.strategy_research_stats_gate_v1 import evaluate_strategy_research_stats_gate, gate_check_message
from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_float(x: Any, default: float | None = None) -> float | None:
    if x is None:
        return default
    try:
        if isinstance(x, bool):
            return 1.0 if x else 0.0
        return float(x)
    except Exception:
        return default


def _load_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        obj = json.load(f)
    return obj if isinstance(obj, dict) else {}


def _load_state_if_exists(path: str) -> dict[str, Any]:
    try:
        if path and os.path.exists(path):
            return _load_json(path)
    except Exception:
        return {}
    return {}


@contextmanager
def _acquire_lock(path: str) -> ContextManager[Any]:
    """Single-writer guard using POSIX flock."""
    f = None
    try:
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        f = open(path, "w")
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield f
    except BlockingIOError:
        print(f"FATAL: Could not acquire lock {path}. Another instance running?")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL: Lock error {path}: {e}")
        sys.exit(1)
    finally:
        if f:
            try:
                fcntl.flock(f, fcntl.LOCK_UN)
                f.close()
            except Exception:
                pass


def _extract_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = report.get("rows")
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    # fallback legacy
    rows = report.get("groups")
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    return []


def _extract_symbol(group: Any, default: str = "UNKNOWN") -> str:
    """Best-effort extraction of symbol/group name across report schema variants."""
    if isinstance(group, dict):
        for k in ("symbol", "sym", "instrument"):
            v = group.get(k)
            if isinstance(v, str) and v:
                return v
    if isinstance(group, (list, tuple)) and group:
        try:
            v = group[0]
            if isinstance(v, str) and v:
                return v
            if v is not None:
                return str(v)
        except Exception:
            pass
    if isinstance(group, str) and group:
        return group
    return default


def _extract_row_n(row: dict[str, Any]) -> int:
    """Extract a conservative n for decisioning.

    Newer drift report schema includes explicit 'n'. Older schema (groups with parts list)
    provides per-part n_target / n_base; we use max n_target as a proxy.
    """
    n = row.get("n")
    try:
        if n is not None:
            return int(float(n))
    except Exception:
        pass

    parts = row.get("parts")
    if isinstance(parts, list):
        n_t = 0
        for it in parts:
            if not isinstance(it, dict):
                continue
            try:
                n_t = max(n_t, int(float(it.get("n_target", 0) or 0)))
            except Exception:
                continue
        return int(n_t)

    return 0


def _row_part_dz(part_metrics: Any) -> float | None:
    if not isinstance(part_metrics, dict):
        return None
    # preferred
    if "dz" in part_metrics:
        return _safe_float(part_metrics.get("dz"), default=None)
    # fallback: shift
    if "shift" in part_metrics:
        return _safe_float(part_metrics.get("shift"), default=None)
    return None


def _compute_row_max_abs_dz(
    row: dict[str, Any],
    parts_allow: list[str] | None = None,
) -> tuple[float | None, list[tuple[str, float]]]:
    """Return max abs drift score for a row.

    Supports two schemas:
      - New: row['parts'] is dict {part_key: {dz:..}} (dz or shift)
      - Old: row['parts'] is list of dicts with keys: key, drift_z (or dz)
    """

    parts = row.get("parts")

    top: list[tuple[str, float]] = []
    max_abs: float | None = None

    if isinstance(parts, dict):
        for k, v in parts.items():
            if parts_allow and k not in parts_allow:
                continue
            dz = _row_part_dz(v)
            if dz is None:
                continue
            try:
                dzf = float(dz)
            except Exception:
                continue
            top.append((str(k), dzf))
            adz = abs(dzf)
            if max_abs is None or adz > max_abs:
                max_abs = adz

    elif isinstance(parts, list):
        for it in parts:
            if not isinstance(it, dict):
                continue
            k = it.get("key")
            if not k:
                continue
            if parts_allow and str(k) not in parts_allow:
                continue
            dz = it.get("drift_z")
            if dz is None:
                dz = it.get("dz")
            if dz is None:
                dz = it.get("shift")
            try:
                dzf = float(dz)
            except Exception:
                continue
            top.append((str(k), dzf))
            adz = abs(dzf)
            if max_abs is None or adz > max_abs:
                max_abs = adz

    else:
        return None, []

    # sort by abs(dz) desc
    top.sort(key=lambda kv: abs(kv[1]), reverse=True)
    return max_abs, top


def decide_actions_thresholds(
    report: dict[str, Any],
    *,
    warn_z: float,
    crit_z: float,
    min_n: int,
    parts_allow: list[str] | None = None,
    top_k: int = 4,
) -> dict[str, dict[str, Any]]:
    """Return per-symbol checks based purely on thresholds (no hysteresis).

    Output per symbol:
      {freeze:int, desired_scale:float, max_abs_dz:float, n:int, top:[(k,dz)...], reason:str}
    """

    by_sym: dict[str, dict[str, Any]] = {}

    for row in _extract_rows(report):
        group = row.get("group")
        sym = _extract_symbol(group)
        n = _extract_row_n(row)

        max_abs, top = _compute_row_max_abs_dz(row, parts_allow=parts_allow)
        if max_abs is None:
            continue

        cur = by_sym.get(sym)
        if cur is None:
            by_sym[sym] = {
                "max_abs_dz": float(max_abs),
                "n": int(n),
                "top": top[:top_k],
                "rows": 1,
            }
        else:
            cur["n"] = int(cur.get("n", 0)) + int(n)
            cur["rows"] = int(cur.get("rows", 0)) + 1
            if float(max_abs) > float(cur.get("max_abs_dz", 0.0)):
                cur["max_abs_dz"] = float(max_abs)
                cur["top"] = top[:top_k]

    # decide
    out: dict[str, dict[str, Any]] = {}
    for sym, st in by_sym.items():
        n = int(st.get("n", 0))
        max_abs = float(st.get("max_abs_dz", 0.0))

        freeze = 0
        desired_scale = 1.0
        reason = "ok"
        if n < int(min_n):
            # not enough evidence; do nothing
            freeze = 0
            desired_scale = 1.0
            reason = "insufficient_n"
        else:
            if max_abs >= float(crit_z):
                freeze = 1
                desired_scale = 0.85
                reason = "crit"
            elif max_abs >= float(warn_z):
                freeze = 0
                desired_scale = 0.92
                reason = "warn"

        out[sym] = {
            "freeze": int(freeze),
            "desired_scale": float(desired_scale),
            "reason": reason,
            "max_abs_dz": float(max_abs),
            "n": int(n),
            "rows": int(st.get("rows", 1)),
            "top": st.get("top", []),
        }

    return out


def decide_actions(
    report: dict[str, Any],
    *,
    warn_z: float,
    crit_z: float,
    min_n: int,
    parts_allow: list[str] | None = None,
    top_k: int = 4,
) -> dict[str, dict[str, Any]]:
    """Backward-compatible wrapper (v1 API).

    Returns {freeze, scale, max_abs_dz, n, top...} without hysteresis.
    """
    raw = decide_actions_thresholds(
        report,
        warn_z=warn_z,
        crit_z=crit_z,
        min_n=min_n,
        parts_allow=parts_allow,
        top_k=top_k,
    )
    out: dict[str, dict[str, Any]] = {}
    for sym, d in raw.items():
        out[sym] = dict(d)
        out[sym]["scale"] = float(d.get("desired_scale", 1.0) or 1.0)
    return out


def _in_canary(symbol: str, share: float, salt: str) -> bool:
    """Deterministic canary gate by crc32(salt:symbol)."""
    try:
        sh = float(share)
    except Exception:
        sh = 1.0
    sh = max(0.0, min(1.0, sh))
    if sh >= 0.999:
        return True
    if sh <= 0.001:
        return False
    s = f"{salt}:{symbol}".encode("utf-8", errors="ignore")
    h = zlib.crc32(s) & 0xFFFFFFFF
    # map to [0,1)
    u = (h % 10000) / 10000.0
    return u < sh


def _prev_symbol_state(prev: dict[str, Any], sym: str) -> dict[str, Any]:
    decs = prev.get("decisions")
    if isinstance(decs, dict):
        st = decs.get(sym)
        if isinstance(st, dict):
            return st
    return {}


def apply_hysteresis_and_recovery(
    raw_decisions: dict[str, dict[str, Any]],
    *,
    prev_state: dict[str, Any],
    now_ms: int,
    recover_z: float,
    recover_runs: int,
    freeze_hold_sec: int,
    recover_scale_start: float,
    recover_scale_step: float,
    scale_bump_min_sec: int,
    canary_share: float,
    canary_salt: str,
) -> dict[str, dict[str, Any]]:
    """Apply stateful hysteresis (latch, recovery ramp) and canary gating.

    Returns final per-symbol decisions with added state fields:
      - latched_until_ms
      - stable_streak
      - canary (0/1)
      - scale (ramped)
    """
    out: dict[str, dict[str, Any]] = {}

    for sym, raw in raw_decisions.items():
        cur = dict(raw)
        prev = _prev_symbol_state(prev_state, sym)

        # 1. Canary check
        is_canary = _in_canary(sym, canary_share, canary_salt)
        cur["canary"] = 1 if is_canary else 0

        if not is_canary:
            # skipped -> force neutral
            cur["freeze"] = 0
            cur["scale"] = 1.0
            cur["reason"] = "canary_skip"
            # do not persist latch/streak state if skippped?
            # actually better to keep tracking state internally but not output effects?
            # For simplicity: reset state if skipped, so we don't latch while ignored.
            cur["stable_streak"] = 0
            cur["latch_remaining_sec"] = 0.0
            out[sym] = cur
            continue

        # 2. Hysteresis State
        # load prev
        p_latched_until = int(float(prev.get("latched_until_ms") or 0))
        p_streak = int(float(prev.get("stable_streak") or 0))
        p_scale = float(prev.get("scale", 1.0) or 1.0)
        p_ts = int(float(prev.get("ts_ms") or (now_ms - 3600000)))  # fallback long ago
        if p_ts <= 0:
            p_ts = now_ms - 3600000

        # inputs
        raw_freeze = int(raw.get("freeze", 0))
        raw_max_abs = float(raw.get("max_abs_dz", 0.0))

        # logic
        new_freeze = raw_freeze
        new_scale = float(raw.get("desired_scale", 1.0))
        new_streak = 0
        new_latched_until = p_latched_until

        if raw_freeze == 1:
            # ENTER/EXTEND FREEZE
            # If critical, we latch for N seconds from now
            new_latched_until = max(new_latched_until, now_ms + int(freeze_hold_sec * 1000))
            new_streak = 0
            new_scale = new_scale  # use the crit scale (e.g. 0.85)
            # reason is already 'crit'
        else:
            # NO FREEZE SIGNAL (raw)
            # Check if latched
            if now_ms < new_latched_until:
                # LATCHED
                new_freeze = 1
                new_streak = 0
                new_scale = new_scale  # keep desired (e.g. warn->0.92 or ok->1.0? usually we want safe scale)
                # For v1 simplicity: we just respect raw desired_scale BUT enforce freeze=1.
                if cur.get("reason") == "ok":
                    cur["reason"] = "latched_freeze"
                else:
                    cur["reason"] = f"{cur.get('reason')}+latched"
            else:
                # NOT LATCHED
                # Check recovery criteria (stable for N runs)
                is_stable = (raw_max_abs <= recover_z)
                if is_stable:
                    new_streak = p_streak + 1
                else:
                    new_streak = 0

                # RAMP LOGIC
                # if we are not frozen (raw=0, not latched), we can ramp up scale.
                if new_freeze == 0:
                     # target is 1.0 (or 0.92 if warn).
                     # if we are below target, we ramp.
                     if p_scale < (new_scale - 1e-9):
                         # ramping up
                         # Allowed info:
                         if new_streak >= recover_runs:
                             # We have enough stable runs to step up.
                             # For now, just step up per valid run.
                             ramped = min(new_scale, p_scale + recover_scale_step)
                             # if we started at very low, maybe jump to recover_scale_start?
                             if p_scale < recover_scale_start:
                                 ramped = max(ramped, recover_scale_start)
                             new_scale = ramped
                             cur["reason"] = "recovering"
                         else:
                             # Not enough stable runs yet, hold prev scale
                             new_scale = p_scale
                             cur["reason"] = "wait_stable_streak"

        cur["freeze"] = int(new_freeze)
        cur["scale"] = float(new_scale)
        cur["latched_until_ms"] = int(new_latched_until)
        cur["latch_remaining_sec"] = max(0, int((new_latched_until - now_ms) / 1000))
        cur["stable_streak"] = int(new_streak)
        cur["ts_ms"] = int(now_ms)  # for next run tracking

        out[sym] = cur

    return out


def apply_overrides_redis(
    decisions: dict[str, dict[str, Any]],
    *,
    redis_url: str,
    key_prefix: str,
    now_ms: int,
    dry_run: bool,
) -> dict[str, Any]:
    try:
        import redis  # type: ignore
    except Exception as exc:
        raise RuntimeError("redis package is required to apply overrides") from exc

    r = redis.Redis.from_url(redis_url, decode_responses=True)

    applied = 0
    for sym, d in decisions.items():
        key = f"{key_prefix}{sym}"
        raw = r.get(key)
        cur = {}
        if raw:
            try:
                cur0 = json.loads(raw)
                if isinstance(cur0, dict):
                    cur = cur0
            except Exception:
                cur = {}

        cur["confidence_score_freeze"] = int(d.get("freeze", 0))
        cur["confidence_score_scale"] = float(d.get("scale", 1.0))
        # non-critical metadata (ignored by allowlist)
        cur["conf_score_guard_ts_ms"] = int(now_ms)
        cur["conf_score_guard_max_abs_dz"] = float(d.get("max_abs_dz", 0.0))
        cur["conf_score_guard_n"] = int(d.get("n", 0))
        cur["conf_score_guard_top"] = d.get("top", [])

        if dry_run:
            continue

        r.set(key, json.dumps(cur, ensure_ascii=False, separators=(",", ":")))
        applied += 1

    return {"applied": applied, "dry_run": bool(dry_run)}


def _write_state(path: str, state: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _compute_changed_symbols(
    decisions: dict[str, dict[str, Any]], prev_decisions: dict[str, dict[str, Any]]
) -> tuple[int, list[str]]:
    changed = []
    for sym, curr in decisions.items():
        prev = prev_decisions.get(sym)
        if not prev:
            changed.append(sym)
            continue
        # Compare minimal critical fields
        c_fr = int(curr.get("freeze", 0))
        p_fr = int(prev.get("freeze", 0))
        c_sc = float(curr.get("scale", 1.0))
        p_sc = float(prev.get("scale", 1.0))
        if c_fr != p_fr or abs(c_sc - p_sc) > 1e-4:
            changed.append(sym)
            continue
    return len(changed), changed


def _write_bundle(
    bundle_dir: str,
    bundle_retain: int,
    state: dict[str, Any],
    promote: bool,
    tag: str,
) -> dict[str, Any]:
    """Writes immutable bundle and updates current pointer."""
    os.makedirs(bundle_dir, exist_ok=True)
    ts = int(state.get("ts_ms") or _now_ms())

    # 1. Create Bundle
    # We serialize the full state as the bundle content
    content = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True)
    # sha for integrity
    sha = hashlib.sha256(content.encode("utf-8")).hexdigest()[:8]

    filename = f"bundle_{ts}_{tag}_{sha}.json"
    bundle_path = os.path.join(bundle_dir, filename)

    tmp_path = bundle_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp_path, bundle_path)

    # 2. Cleanup old bundles (if needed)
    # List all bundles by pattern, sort by time (filename)
    all_bundles = sorted(glob.glob(os.path.join(bundle_dir, "bundle_*.json")))
    if len(all_bundles) > bundle_retain:
        to_remove = all_bundles[:-bundle_retain]
        for f in to_remove:
            with suppress(OSError):
                os.remove(f)

    # 3. Update Pointer (if promote)
    pointer_info = {}
    if promote:
        current_path = os.path.join(bundle_dir, "current.json")
        prev_info = {}
        if os.path.exists(current_path):
            with open(current_path) as f:
                prev_info = json.load(f)

        new_pointer = {
            "current_file": filename,
            "current_ts": ts,
            "current_sha": sha,
            "updated_at_iso": datetime.now(UTC).isoformat(),
            "prev_file": prev_info.get("current_file"),
            "prev_ts": prev_info.get("current_ts"),
        }

        tmp_ptr = current_path + ".tmp"
        with open(tmp_ptr, "w", encoding="utf-8") as f:
            json.dump(new_pointer, f, indent=2)
        os.replace(tmp_ptr, current_path)
        pointer_info = new_pointer

    return {
        "file": filename,
        "sha": sha,
        "promoted": promote,
        "pointer": pointer_info
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--drift-report", required=True, help="Path to drift report JSON")
    ap.add_argument("--apply", type=int, default=0, help="Set 1 to apply Redis overrides")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", ""), help="Redis URL")
    ap.add_argument(
        "--key-prefix",
        default="cfg:crypto_of:overrides:",
        help="Redis key prefix for per-symbol overrides",
    )
    ap.add_argument("--warn-z", type=float, default=4.0)
    ap.add_argument("--crit-z", type=float, default=6.0)
    ap.add_argument("--min-n", type=int, default=200)
    ap.add_argument(
        "--parts",
        default="",
        help="Comma-separated allowlist of parts keys to watch (empty = all)",
    )
    ap.add_argument("--top-k", type=int, default=4)
    ap.add_argument(
        "--state-path",
        default=os.getenv("CONF_SCORE_GUARD_STATE_PATH", "/tmp/conf_score_guard_state.json"),
    )
    # Hysteresis / Recovery / Canary args
    ap.add_argument("--freeze-hold-sec", type=int, default=3600, help="Min time to hold freeze")
    ap.add_argument("--recover-z", type=float, default=3.0, help="Max abs Z to count as stable")
    ap.add_argument("--recover-runs", type=int, default=3, help="Consecutive stable runs to unfreeze/ramp")
    ap.add_argument("--recover-scale-start", type=float, default=0.92, help="Scale to jump to on first unfreeze")
    ap.add_argument("--recover-scale-step", type=float, default=0.05, help="Scale increase per stable run")
    ap.add_argument("--canary-share", type=float, default=1.0, help="Fraction 0.0-1.0 to apply")
    ap.add_argument("--canary-salt", type=str, default="", help="Salt for canary hashing (default: from window)")

    # Bundle / Lock args
    ap.add_argument("--bundle-enable", type=int,
                    default=int(os.getenv("CONF_SCORE_GUARD_BUNDLE_ENABLE", "1")))
    ap.add_argument("--bundle-dir",
                    default=os.getenv("CONF_SCORE_GUARD_BUNDLE_DIR", "/var/lib/trade/conf_score_guard_bundles"))
    ap.add_argument("--bundle-retain", type=int, default=60, help="Number of bundles to keep")
    ap.add_argument("--bundle-promote", type=int, default=-1,
                    help="Update current pointer? -1=auto(if apply=1), 0=no, 1=yes")
    ap.add_argument("--bundle-tag", default="v1")
    ap.add_argument("--lock-path",
                    default=os.getenv("CONF_SCORE_GUARD_LOCK_PATH", "/tmp/conf_score_guard.lock"))

    # Stage / Promote args
    ap.add_argument("--stage", type=int, default=0, help="If 1, run in stage mode: write staged.json and staged keys, do NOT touch live")
    ap.add_argument("--bundle-staged-pointer-path", type=str, default="", help="Path to staged.json pointer")
    ap.add_argument("--staged-key-prefix", type=str, default="cfg:crypto_of:overrides_staged:", help="Redis key prefix for staged overrides")

    args = ap.parse_args()

    # 1. Acquire global lock
    with _acquire_lock(args.lock_path):
        report = _load_json(args.drift_report)
        parts_allow = [p.strip() for p in (args.parts or "").split(",") if p.strip()] or None

        # Load state from state-path usually, but if bundles enabled,
        # normally we might want to load from 'current.json'?
        # For now, we stick to state-path for continuity of 'prev_state'.
        # FUTURE: load prev state from current bundle.
        prev_state = _load_state_if_exists(args.state_path)

        # 2. Raw Threshold Decisions
        raw_decisions = decide_actions_thresholds(
            report,
            warn_z=args.warn_z,
            crit_z=args.crit_z,
            min_n=args.min_n,
            parts_allow=parts_allow,
            top_k=args.top_k,
        )

        # Resolve Canary Salt
        c_salt = args.canary_salt
        if not c_salt:
            c_salt = "v1"

        # 3. Stateful Logic
        now_ms = _now_ms()
        final_decisions = apply_hysteresis_and_recovery(
            raw_decisions,
            prev_state=prev_state,
            now_ms=now_ms,
            recover_z=args.recover_z,
            recover_runs=args.recover_runs,
            freeze_hold_sec=args.freeze_hold_sec,
            recover_scale_start=args.recover_scale_start,
            recover_scale_step=args.recover_scale_step,
            scale_bump_min_sec=300,  # lowered default to 5m
            canary_share=args.canary_share,
            canary_salt=c_salt,
        )

        decisions = final_decisions

        summary = {
            "frozen": sum(1 for v in decisions.values() if int(v.get("freeze", 0)) == 1),
            "scaled": sum(1 for v in decisions.values() if float(v.get("scale", 1.0)) < 0.999),
            "latched": sum(1 for v in decisions.values() if float(v.get("latch_remaining_sec", 0) or 0) > 0),
            "canary_skip": sum(1 for v in decisions.values() if int(v.get("canary", 1)) == 0),
            "symbols": len(decisions),
        }

        # Calculate Changed Symbols
        n_changed, changed_list = _compute_changed_symbols(
            decisions,
            prev_state.get("decisions", {}) if isinstance(prev_state.get("decisions"), dict) else {}
        )

        state = {
            "ts_ms": now_ms,
            "inputs": {
                "drift_report": args.drift_report,
                "warn_z": float(args.warn_z),
                "crit_z": float(args.crit_z),
                "min_n": int(args.min_n),
                "parts": parts_allow or [],
                "canary_share": float(args.canary_share),
            },
            "window": report.get("window", {}),
            "group_by": report.get("group_by"),
            "decisions": decisions,
            "summary": summary,
            "bundle": {
                "changed_count": n_changed,
                "changed_symbols": changed_list,
            }
        }

        is_stage_mode = (getattr(args, "stage", 0) == 1)
        target_prefix = args.staged_key_prefix if is_stage_mode else args.key_prefix

        # 3.5 Research guard hard-gate (P5.2).
        # Defense-in-depth: host/container preflight should already block rollout-sensitive jobs,
        # but we still enforce here so that direct python invocation cannot bypass the blocker.
        if int(args.apply) == 1 and os.getenv("ENABLE_STRATEGY_RESEARCH_GUARD_HARD_GATE", "0") == "1":
            assert_research_guard_open(
                args.redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
                purpose="conf_score_guardrails_apply",
                stage_mode=bool(is_stage_mode),
            )

        # 3.5 Research guard blocker (P5). Fail-open while report-only=1.
        if int(args.apply) == 1 and os.getenv("ENABLE_STRATEGY_RESEARCH_GUARD_BLOCKER", "0") == "1":
            blocked, reason, _ = check_research_guard_blocker(
                args.redis_url,
                os.getenv("STRATEGY_RESEARCH_GUARD_BLOCKER_KEY", "cfg:research_guard:blocker:v1"),
            )
            if blocked:
                print(json.dumps({"blocked": True, "reason": reason, "source": "research_guard_blocker"}, ensure_ascii=False))
                return 0

        # 3.7 Strategy research stats gate (P6.1).
        # Uses hard/soft/report_only semantics from STRATEGY_RESEARCH_STATS_GATE_MODE.
        # Default: report_only (fail-open; no blocking until explicitly configured).
        if int(args.apply) == 1 and os.getenv("ENABLE_STRATEGY_RESEARCH_STATS_GATE", "1") in ("1", "true", "True", "yes", "on"):
            gate = evaluate_strategy_research_stats_gate(
                args.redis_url,
                os.getenv("STRATEGY_RESEARCH_STATS_BLOCKER_KEY", "cfg:strategy_research_stats:blocker:v1"),
                os.getenv("STRATEGY_RESEARCH_STATS_SUMMARY_KEY", "metrics:strategy_research_stats:last"),
                max_age_sec=float(os.getenv("STRATEGY_RESEARCH_STATS_MAX_AGE_SEC", "129600") or 129600),
                fail_closed_missing=int(os.getenv("STRATEGY_RESEARCH_STATS_FAIL_CLOSED_MISSING", "0") or 0),
            )
            if (gate.get("status")) == "block":
                print(json.dumps({"blocked": True, "reason": gate.get("reason"), "source": "strategy_research_stats_gate"}, ensure_ascii=False))
                return 0
            if (gate.get("status")) == "invalid" and os.getenv("STRATEGY_RESEARCH_STATS_INVALID_AS_BLOCK", "1") in ("1", "true", "True", "yes", "on"):
                print(json.dumps({"blocked": True, "reason": gate.get("reason"), "source": "strategy_research_stats_gate", "status": "invalid"}, ensure_ascii=False))
                return 0
            if (gate.get("status")) == "soft":
                print(gate_check_message(gate, purpose="conf_score_guardrails_apply"))

        # 4. Redis Apply
        applied_info = None

        if int(args.apply) == 1:
            if not args.redis_url:
                raise SystemExit("--redis-url is required when --apply=1")

            # Additional safety: if stage mode, ensure we don't accidentally write to live prefix
            if is_stage_mode and target_prefix == "cfg:crypto_of:overrides:":
                target_prefix = "cfg:crypto_of:overrides_staged:"

            applied_info = apply_overrides_redis(
                decisions,
                redis_url=args.redis_url,
                key_prefix=target_prefix,
                now_ms=now_ms,
                dry_run=False,
            )
        else:
            applied_info = {"applied": 0, "dry_run": True}

        state["apply"] = applied_info
        if is_stage_mode:
            state["stage_mode"] = True

        # 5. Write Bundle (if enabled)
        if args.bundle_enable:
            should_promote = False
            if is_stage_mode:
                # Stage mode: never promote to current.json
                should_promote = False
            else:
                if args.bundle_promote == -1:
                    should_promote = (int(args.apply) == 1)
                else:
                    should_promote = (int(args.bundle_promote) == 1)

            bundle_info = _write_bundle(
                bundle_dir=args.bundle_dir,
                bundle_retain=args.bundle_retain,
                state=state,
                promote=should_promote,
                tag=args.bundle_tag,
            )

            # If stage mode, we handle "staged.json" pointer manually
            if is_stage_mode:
                staged_ptr_path = args.bundle_staged_pointer_path
                if not staged_ptr_path and args.bundle_dir:
                    staged_ptr_path = os.path.join(args.bundle_dir, "staged.json")

                if staged_ptr_path:
                    ptr_data = {
                        "staged_file": bundle_info["file"],
                        "staged_ts": now_ms,
                        "staged_sha": bundle_info["sha"],
                        "updated_at_iso": datetime.now(UTC).isoformat()
                    }
                    tmp_ptr = staged_ptr_path + ".tmp"
                    with open(tmp_ptr, "w", encoding="utf-8") as f:
                        json.dump(ptr_data, f, indent=2)
                    os.replace(tmp_ptr, staged_ptr_path)
                    bundle_info["staged_pointer"] = staged_ptr_path

            state["bundle"].update(bundle_info)

        # 6. Write State (for exporter)
        _write_state(args.state_path, state)

        # print a compact summary for logs
        print(json.dumps({
            "summary": summary,
            "apply": applied_info,
            "bundle": state.get("bundle")
        }, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
