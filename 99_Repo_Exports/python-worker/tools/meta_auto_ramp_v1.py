#!/usr/bin/env python3
from __future__ import annotations

"""
Meta auto-ramp / freeze controller (P5).

Goal:
  - Read nightly quality report produced by meta_model_quality_report_v1.py
  - Decide whether to ramp up / down meta_enforce_share_* and/or keep SHADOW
  - Optionally write decision into Redis dynamic cfg hash (merged into cfg2 at runtime)

Safety:
  - default is DRY-RUN (no writes) unless --apply=1
  - conservative thresholds + hysteresis (good_streak/bad_streak)

This script is designed to be run from cron/systemd timer.
"""

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _load_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


@dataclass
class RampDecision:
    decision: str  # "ramp_up" | "hold" | "ramp_down" | "freeze"
    reason: str
    new_share: float
    bucket: str
    good_streak: int
    bad_streak: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "reason": self.reason,
            "new_share": float(self.new_share),
            "bucket": str(self.bucket),
            "good_streak": int(self.good_streak),
            "bad_streak": int(self.bad_streak),
        }


def _pick_bucket(args: argparse.Namespace, report: dict[str, Any]) -> str:
    if args.bucket:
        return str(args.bucket)
    # try infer from report meta
    meta = report.get("meta", {}) if isinstance(report.get("meta", {}), dict) else {}
    b = meta.get("bucket") or meta.get("regime_bucket") or meta.get("session") or "global"
    return str(b)


def _extract_metrics(report: dict[str, Any]) -> dict[str, float]:
    # Try "metrics" sub-dict first (P4b), else fallback to top-level (legacy P4)
    metrics = report.get("metrics", {}) if isinstance(report.get("metrics"), dict) else report
    return {
        "ece": _safe_float(metrics.get("ece"), 0.0),
        "brier": _safe_float(metrics.get("brier"), 0.0),
        "pr_auc": _safe_float(metrics.get("pr_auc"), 0.0),
        "precision_at_200": _safe_float(metrics.get("precision_at_200"), -1.0),
        "precision_at_500": _safe_float(metrics.get("precision_at_500"), -1.0),
    }


def _extract_counts(report: dict[str, Any]) -> tuple[int, int, int]:
    # Try "counts" sub-dict first (P4b), else fallback to top-level (legacy P4)
    counts = report.get("counts", {}) if isinstance(report.get("counts"), dict) else report
    n = _safe_int(counts.get("n"), 0)
    pos = _safe_int(counts.get("pos"), 0)
    neg = _safe_int(counts.get("neg"), max(0, n - pos))
    return n, pos, neg


def decide_ramp(
    report: dict[str, Any],
    prev_share: float,
    good_streak: int,
    bad_streak: int,
    args: argparse.Namespace,
) -> RampDecision:
    prev_share = _clamp(float(prev_share), 0.0, 1.0)
    bucket = _pick_bucket(args, report)

    n, pos, _ = _extract_counts(report)
    m = _extract_metrics(report)

    # Dataset sufficiency guard
    if n < args.min_samples or pos < args.min_pos:
        return RampDecision(
            decision="hold",
            reason=f"dataset_too_small n={n} pos={pos} (min_samples={args.min_samples} min_pos={args.min_pos})",
            new_share=prev_share,
            bucket=bucket,
            good_streak=0,
            bad_streak=0,
        )

    # Evaluate "good" conditions (conservative)
    ok_pr = m["pr_auc"] >= args.min_pr_auc
    ok_ece = m["ece"] <= args.max_ece
    ok_brier = (m["brier"] <= args.max_brier) if args.max_brier > 0 else True

    # Precision optional (if present)
    prec_ok = True
    if m["precision_at_200"] >= 0.0:
        prec_ok = m["precision_at_200"] >= args.min_precision_at_200

    is_good = ok_pr and ok_ece and ok_brier and prec_ok
    is_bad = (m["pr_auc"] > 0 and m["pr_auc"] < args.bad_pr_auc) or (m["ece"] > args.bad_ece)

    # Hysteresis
    if is_good:
        good_streak += 1
        bad_streak = 0
    elif is_bad:
        bad_streak += 1
        good_streak = 0
    else:
        # neither good nor bad -> decay streaks slowly
        good_streak = max(0, good_streak - 1)
        bad_streak = max(0, bad_streak - 1)

    # Freeze rule (hard safety)
    if bad_streak >= args.freeze_after_bad:
        new_share = 0.0
        return RampDecision(
            decision="freeze",
            reason=f"bad_streak={bad_streak} (pr_auc={m['pr_auc']:.3f} ece={m['ece']:.3f})",
            new_share=new_share,
            bucket=bucket,
            good_streak=0,
            bad_streak=bad_streak,
        )

    # Ramp-up only after enough consecutive good reports
    if good_streak >= args.ramp_after_good:
        new_share = _clamp(prev_share + args.step_up, 0.0, args.max_share)
        if new_share > prev_share + 1e-9:
            return RampDecision(
                decision="ramp_up",
                reason=f"good_streak={good_streak} (pr_auc={m['pr_auc']:.3f} ece={m['ece']:.3f})",
                new_share=new_share,
                bucket=bucket,
                good_streak=good_streak,
                bad_streak=0,
            )

    # Ramp-down on bad streak (soft)
    if bad_streak >= args.ramp_down_after_bad and prev_share > 0.0:
        new_share = _clamp(prev_share - args.step_down, 0.0, args.max_share)
        return RampDecision(
            decision="ramp_down",
            reason=f"bad_streak={bad_streak} (pr_auc={m['pr_auc']:.3f} ece={m['ece']:.3f})",
            new_share=new_share,
            bucket=bucket,
            good_streak=0,
            bad_streak=bad_streak,
        )

    return RampDecision(
        decision="hold",
        reason=f"hold (pr_auc={m['pr_auc']:.3f} ece={m['ece']:.3f})",
        new_share=prev_share,
        bucket=bucket,
        good_streak=good_streak,
        bad_streak=bad_streak,
    )


def _redis_connect(redis_url: str):
    try:
        import redis  # type: ignore
    except Exception as e:
        raise SystemExit(f"redis-py is required for --apply=1 (pip install redis). Import error: {e}")
    return redis.Redis.from_url(redis_url, decode_responses=True)


def _dyn_read(r, dyn_key: str) -> dict[str, str]:
    try:
        d = r.hgetall(dyn_key) or {}
        if not isinstance(d, dict):
            return {}
        return {str(k): str(v) for k, v in d.items()}
    except Exception:
        return {}


def _dyn_write(r, dyn_key: str, updates: dict[str, Any]) -> None:
    # Store as strings (loader in runtime should parse numbers if needed)
    mapping = {str(k): (json.dumps(v) if isinstance(v, (dict, list)) else str(v)) for k, v in updates.items()}
    r.hset(dyn_key, mapping=mapping)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-json", required=True, help="Path to meta quality report JSON")
    ap.add_argument("--bucket", default="", help="Optional bucket name (default: infer or global)")

    # Thresholds (conservative defaults)
    ap.add_argument("--min-samples", type=int, default=int(os.getenv("META_RAMP_MIN_SAMPLES", "400")))
    ap.add_argument("--min-pos", type=int, default=int(os.getenv("META_RAMP_MIN_POS", "60")))
    ap.add_argument("--min-pr-auc", type=float, default=float(os.getenv("META_RAMP_MIN_PR_AUC", "0.55")))
    ap.add_argument("--max-ece", type=float, default=float(os.getenv("META_RAMP_MAX_ECE", "0.08")))
    ap.add_argument("--max-brier", type=float, default=float(os.getenv("META_RAMP_MAX_BRIER", "0.0")))  # 0 => disabled
    ap.add_argument("--min-precision-at-200", type=float, default=float(os.getenv("META_RAMP_MIN_P200", "0.55")))

    # "Bad" thresholds (trigger ramp down / freeze)
    ap.add_argument("--bad-pr-auc", type=float, default=float(os.getenv("META_RAMP_BAD_PR_AUC", "0.48")))
    ap.add_argument("--bad-ece", type=float, default=float(os.getenv("META_RAMP_BAD_ECE", "0.12")))

    # Ramp knobs
    ap.add_argument("--step-up", type=float, default=float(os.getenv("META_RAMP_STEP_UP", "0.05")))
    ap.add_argument("--step-down", type=float, default=float(os.getenv("META_RAMP_STEP_DOWN", "0.10")))
    ap.add_argument("--max-share", type=float, default=float(os.getenv("META_RAMP_MAX_SHARE", "0.50")))
    ap.add_argument("--ramp-after-good", type=int, default=int(os.getenv("META_RAMP_AFTER_GOOD", "2")))
    ap.add_argument("--ramp-down-after-bad", type=int, default=int(os.getenv("META_RAMP_DOWN_AFTER_BAD", "1")))
    ap.add_argument("--freeze-after-bad", type=int, default=int(os.getenv("META_RAMP_FREEZE_AFTER_BAD", "3")))

    # Redis dynamic cfg IO
    ap.add_argument("--apply", type=int, default=int(os.getenv("META_RAMP_APPLY", "0")), help="1 to write into Redis dynamic cfg")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--dyn-key", default=os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg"), help="Redis hash key for runtime dynamic config")
    ap.add_argument("--share-key", default=os.getenv("META_ENFORCE_SHARE_KEY", "meta_enforce_share"), help="Field name in dynamic cfg")
    ap.add_argument("--mode-key", default=os.getenv("META_MODE_KEY", "meta_model_mode"), help="Field name for mode")

    # State keys (streaks)
    ap.add_argument("--good-key", default=os.getenv("META_RAMP_GOOD_KEY", "meta_ramp_good_streak"))
    ap.add_argument("--bad-key", default=os.getenv("META_RAMP_BAD_KEY", "meta_ramp_bad_streak"))
    ap.add_argument("--status-key", default=os.getenv("META_RAMP_STATUS_KEY", "meta_ramp_status"))

    # Optional Prom textfile output
    ap.add_argument("--prom-textfile", default=os.getenv("META_RAMP_PROM_TEXTFILE", ""))

    args = ap.parse_args()

    report = _load_json(args.report_json)

    prev_share = 0.0
    good_streak = 0
    bad_streak = 0

    # If apply, read previous state from Redis
    dyn: dict[str, str] = {}
    if args.apply:
        r = _redis_connect(args.redis_url)
        dyn = _dyn_read(r, args.dyn_key)
        prev_share = _safe_float(dyn.get(args.share_key), 0.0)
        good_streak = _safe_int(dyn.get(args.good_key), 0)
        bad_streak = _safe_int(dyn.get(args.bad_key), 0)
    else:
        # allow local override for testing
        prev_share = float(os.getenv("META_ENFORCE_SHARE", "0.0"))

    # P11: Check Freeze Latch
    # We check if meta_guard_freeze is set in dyn cfg (populated by meta_guardrails_v1.py)
    guard_freeze = _safe_int(dyn.get("meta_guard_freeze"), 0)
    guard_reason = dyn.get("meta_guard_reason", "unknown")

    # Emergency override
    ignore_guard = int(os.getenv("META_RAMP_IGNORE_GUARD", "0")) or (1 if args.apply and int(dyn.get("meta_ramp_ignore_guard", "0")) else 0)

    if guard_freeze and not ignore_guard:
        # FORCE FREEZE
        dec = RampDecision(
            decision="freeze",
            reason=f"GUARDRAIL_LATCH: {guard_reason}",
            new_share=0.0,
            bucket=report.get("meta", {}).get("bucket", "global"),
            good_streak=0,
            bad_streak=bad_streak
        )
    else:
        dec = decide_ramp(report, prev_share, good_streak, bad_streak, args)

    out = {
        "ts_ms": get_ny_time_millis(),
        "prev_share": float(prev_share),
        "decision": dec.to_dict(),
        "thresholds": {
            "min_samples": args.min_samples,
            "min_pos": args.min_pos,
            "min_pr_auc": args.min_pr_auc,
            "max_ece": args.max_ece,
            "min_precision_at_200": args.min_precision_at_200,
            "step_up": args.step_up,
            "step_down": args.step_down,
            "max_share": args.max_share,
        },
    }

    print(json.dumps(out, ensure_ascii=False, indent=2))

    if args.prom_textfile:
        try:
            lines = []
            lines.append(f"meta_ramp_enforce_share {dec.new_share}\n")
            lines.append(f"meta_ramp_good_streak {dec.good_streak}\n")
            lines.append(f"meta_ramp_bad_streak {dec.bad_streak}\n")
            # decision as labels (0/1)
            for k in ["ramp_up", "hold", "ramp_down", "freeze"]:
                v = 1.0 if dec.decision == k else 0.0
                lines.append(f"meta_ramp_decision{{decision=\"{k}\"}} {v}\n")
            tmp = args.prom_textfile + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.writelines(lines)
            os.replace(tmp, args.prom_textfile)
        except Exception:
            pass

    if args.apply:
        r = _redis_connect(args.redis_url)
        updates = {
            args.share_key: dec.new_share,
            args.good_key: dec.good_streak,
            args.bad_key: dec.bad_streak,
            args.status_key: json.dumps(dec.to_dict(), ensure_ascii=False),
        }
        # If frozen -> set SHADOW explicitly for safety
        if dec.decision == "freeze":
            updates[args.mode_key] = "SHADOW"
        _dyn_write(r, args.dyn_key, updates)


if __name__ == "__main__":
    main()
