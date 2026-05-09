from __future__ import annotations

"""Nightly confidence calibrator bundle (ROI step).

What it does (daily):
  1) Builds a joined JSONL dataset from Redis streams (signals:of:inputs + trades:closed)
  2) Trains a post-hoc calibrator (temp/Platt on logit(confidence))
  3) Computes a calibration report (ECE/Brier + Precision/Expectancy@Top5%)
  4) Applies a simple guard:
       cal_ece <= raw_ece - guard_min_ece_abs
       cal_brier <= raw_brier - guard_min_brier_abs
     If guard passes -> atomically updates conf_cal_latest.json
     Else -> keeps previous calibrator (fail-open)

Outputs:
  - <out_dir>/conf_cal_latest.json
  - <out_dir>/versions/conf_cal_YYYYMMDD_HHMMSS.json
  - <reports_dir>/confidence_calibration_report.json
  - <reports_dir>/confidence_calibration_status.json

Designed to be called from of_timers_worker (single command, deterministic).
"""

import argparse
import json
import math
import os
import subprocess
import sys
from datetime import datetime
from typing import Any

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _run(module: str, args: list[str], *, timeout: int = 3600) -> tuple[bool, str, str]:
    cmd = [sys.executable, "-m", module] + list(args or [])
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    ok = p.returncode == 0
    return ok, (p.stdout or ""), (p.stderr or "")


def _atomic_replace(src: str, dst: str) -> None:
    tmp = dst + ".tmp"
    with open(src, "rb") as r:
        data = r.read()
    with open(tmp, "wb") as w:
        w.write(data)
        w.flush()
        os.fsync(w.fileno())
    os.replace(tmp, dst)


def _read_train_report(cal_json_path: str) -> dict[str, Any] | None:
    try:
        with open(cal_json_path, encoding="utf-8") as f:
            obj = json.load(f)
        rep = obj.get("train_report") or {}
        if not isinstance(rep, dict):
            return None
        return rep
    except Exception:
        return None


def _guard_ok(
    rep: dict[str, Any],
    *,
    min_ece_abs: float,
    min_brier_abs: float,
    max_cal_mce: float = float("inf"),
    min_cal_sharpness_mean: float = -1.0,
    max_cal_prob_mass_near_half: float = float("inf"),
) -> tuple[bool, dict[str, Any]]:
    raw = rep.get("raw") or {}
    cal = rep.get("cal") or {}
    try:
        raw_ece = float(raw.get("ece", 0.0) or 0.0)
        raw_brier = float(raw.get("brier", 0.0) or 0.0)
        cal_ece = float(cal.get("ece", 0.0) or 0.0)
        cal_brier = float(cal.get("brier", 0.0) or 0.0)
        cal_mce = float(cal.get("mce", float("nan")))
        cal_sharp = float(cal.get("sharpness_mean", float("nan")))
        cal_half = float(cal.get("prob_mass_near_half", float("nan")))
        reasons = []
        if not (cal_ece <= (raw_ece - float(min_ece_abs))):
            reasons.append("ece_guard_fail")
        if not (cal_brier <= (raw_brier - float(min_brier_abs))):
            reasons.append("brier_guard_fail")
        if math.isfinite(max_cal_mce) and math.isfinite(cal_mce) and cal_mce > float(max_cal_mce):
            reasons.append("mce_too_high")
        if float(min_cal_sharpness_mean) >= 0.0 and math.isfinite(cal_sharp) and cal_sharp < float(min_cal_sharpness_mean):
            reasons.append("sharpness_too_low")
        if math.isfinite(max_cal_prob_mass_near_half) and math.isfinite(cal_half) and cal_half > float(max_cal_prob_mass_near_half):
            reasons.append("prob_mass_near_half_too_high")
        ok = len(reasons) == 0
        return ok, {
            "raw_ece": raw_ece,
            "raw_brier": raw_brier,
            "cal_ece": cal_ece,
            "cal_brier": cal_brier,
            "cal_mce": cal_mce,
            "cal_sharpness_mean": cal_sharp,
            "cal_prob_mass_near_half": cal_half,
            "min_ece_abs": float(min_ece_abs),
            "min_brier_abs": float(min_brier_abs),
            "max_cal_mce": float(max_cal_mce) if math.isfinite(max_cal_mce) else None,
            "min_cal_sharpness_mean": float(min_cal_sharpness_mean),
            "max_cal_prob_mass_near_half": float(max_cal_prob_mass_near_half) if math.isfinite(max_cal_prob_mass_near_half) else None,
            "reasons": reasons,
            "passed": bool(ok),
        }
    except Exception:
        return False, {"passed": False}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--out_dir", default=os.environ.get("CONF_CAL_OUT_DIR", "/var/lib/trade/of_calibrators"))
    ap.add_argument("--reports_dir", default=os.environ.get("CONF_CAL_REPORTS_DIR", "/var/lib/trade/of_reports/out/confidence_cal"))
    ap.add_argument("--lookback_days", type=int, default=int(os.environ.get("CONF_CAL_LOOKBACK_DAYS", "7")))
    ap.add_argument("--key", default=os.environ.get("CONF_CAL_KEY", "confidence_v1"))
    ap.add_argument("--method", default=os.environ.get("CONF_CAL_METHOD", "auto"), choices=["temp", "platt", "auto", "isotonic", "beta", "identity"])
    ap.add_argument("--min_rows", type=int, default=int(os.environ.get("CONF_CAL_MIN_ROWS", "5000")))
    ap.add_argument("--guard_min_ece_abs", type=float, default=float(os.environ.get("CONF_CAL_GUARD_MIN_ECE_ABS", "0.001")))
    ap.add_argument("--guard_min_brier_abs", type=float, default=float(os.environ.get("CONF_CAL_GUARD_MIN_BRIER_ABS", "0.0005")))
    ap.add_argument("--guard_max_cal_mce", type=float, default=float(os.environ.get("CONF_CAL_GUARD_MAX_CAL_MCE", "inf")))
    ap.add_argument("--guard_min_cal_sharpness_mean", type=float, default=float(os.environ.get("CONF_CAL_GUARD_MIN_CAL_SHARPNESS_MEAN", "-1")))
    ap.add_argument("--guard_max_cal_prob_mass_near_half", type=float, default=float(os.environ.get("CONF_CAL_GUARD_MAX_CAL_PROB_MASS_NEAR_HALF", "inf")))
    ap.add_argument("--y_min_r", type=float, default=float(os.environ.get("Y_MIN_R", "0.10")))
    ap.add_argument("--signals_count", type=int, default=int(os.environ.get("SIGNALS_COUNT", "200000")))
    ap.add_argument("--closes_count", type=int, default=int(os.environ.get("CLOSES_COUNT", "200000")))
    args = ap.parse_args(list(argv) if argv is not None else None)

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(args.reports_dir, exist_ok=True)
    versions_dir = os.path.join(args.out_dir, "versions")
    os.makedirs(versions_dir, exist_ok=True)

    now_ms = _now_ms()
    since_ms = now_ms - int(args.lookback_days) * 86400 * 1000
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Paths
    dataset_jsonl = os.path.join(args.reports_dir, f"edge_train_confcal_{stamp}.jsonl")
    quarantine_jsonl = os.path.join(args.reports_dir, f"edge_quarantine_confcal_{stamp}.jsonl")
    dataset_report = os.path.join(args.reports_dir, f"edge_dataset_report_confcal_{stamp}.json")
    cal_tmp = os.path.join(args.reports_dir, f"conf_cal_{stamp}.json")
    cal_ver = os.path.join(versions_dir, f"conf_cal_{stamp}.json")
    cal_latest = os.path.join(args.out_dir, "conf_cal_latest.json")

    calib_report_json = os.path.join(args.reports_dir, "confidence_calibration_report.json")
    status_json = os.path.join(args.reports_dir, "confidence_calibration_status.json")

    status: dict[str, Any] = {
        "ts_ms": int(now_ms),
        "stamp": stamp,
        "ok": False,
        "deployed": False,
        "paths": {
            "dataset_jsonl": dataset_jsonl,
            "cal_tmp": cal_tmp,
            "cal_latest": cal_latest,
            "cal_version": cal_ver,
            "calib_report_json": calib_report_json,
        },
    }

    # 1) Build dataset
    ok, out, err = _run(
        "ml_analysis.tools.build_edge_stack_dataset_from_redis",
        [
            "--redis_url", args.redis_url,
            "--out_jsonl", dataset_jsonl,
            "--out_quarantine_jsonl", quarantine_jsonl,
            "--out_report_json", dataset_report,
            "--signals_count", str(int(args.signals_count)),
            "--closes_count", str(int(args.closes_count)),
            "--since_ms", str(int(since_ms)),
            "--until_ms", str(int(now_ms)),
            "--y_min_r", str(float(args.y_min_r)),
        ],
        timeout=3600,
    )
    status["build_ok"] = bool(ok)
    if not ok:
        status["error"] = "dataset_build_failed"
        status["stderr"] = err[-2000:]
        with open(status_json, "w", encoding="utf-8") as f:
            json.dump(status, f, ensure_ascii=False, indent=2, sort_keys=True)
        raise SystemExit("dataset_build_failed")

    # 2) Train calibrator (V2)
    # Uses --method auto by default or from env
    method_arg = args.method
    if method_arg not in ("auto", "platt", "isotonic", "beta", "identity"):
        method_arg = "auto"

    ok, out, err = _run(
        "ml_analysis.tools.train_confidence_calibrator_v2",
        [
            "--in_jsonl", dataset_jsonl,
            "--out_bundle", cal_tmp,
            "--key", args.key,
            "--method", method_arg,
            "--min_rows", str(int(args.min_rows)),
            "--hierarchical", "1"
        ],
        timeout=3600,
    )
    status["train_ok"] = bool(ok)
    if not ok:
        status["error"] = "calibrator_train_failed"
        status["stderr"] = err[-2000:]
        with open(status_json, "w", encoding="utf-8") as f:
            json.dump(status, f, ensure_ascii=False, indent=2, sort_keys=True)
        raise SystemExit("calibrator_train_failed")

    rep = _read_train_report(cal_tmp) or {}
    status["train_report"] = rep

    # 3) Calibration report (raw vs calibrated)
    ok, out, err = _run(
        "ml_analysis.tools.calibration_report",
        [
            "--in_jsonl", dataset_jsonl,
            "--out_json", calib_report_json,
        ],
        timeout=1200,
    )
    status["report_ok"] = bool(ok)
    if not ok:
        # non-fatal: still can deploy calibrator if guard passes
        status["report_error"] = err[-2000:]

    # 4) Guard + deploy
    g_ok, g = _guard_ok(rep, min_ece_abs=float(args.guard_min_ece_abs), min_brier_abs=float(args.guard_min_brier_abs), max_cal_mce=float(args.guard_max_cal_mce), min_cal_sharpness_mean=float(args.guard_min_cal_sharpness_mean), max_cal_prob_mass_near_half=float(args.guard_max_cal_prob_mass_near_half))
    status["guard"] = g

    if g_ok:
        # Keep a versioned copy and atomically update latest
        _atomic_replace(cal_tmp, cal_ver)
        _atomic_replace(cal_ver, cal_latest)
        status["deployed"] = True

    status["ok"] = True

    with open(status_json, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2, sort_keys=True)

    # exit code: 0 if ran, even if guard blocked deployment (fail-open)
    return


if __name__ == "__main__":
    main()
