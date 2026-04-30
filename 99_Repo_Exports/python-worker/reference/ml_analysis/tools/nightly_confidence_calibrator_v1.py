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

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time
import subprocess
from datetime import datetime
from typing import Any, Dict, Optional, Tuple


def _now_ms() -> int:
    return get_ny_time_millis()


def _run(module: str, args: list[str], *, timeout: int = 3600) -> Tuple[bool, str, str]:
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


def _read_train_report(cal_json_path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(cal_json_path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        rep = obj.get("train_report") or {}
        if not isinstance(rep, dict):
            return None
        return rep
    except Exception:
        return None


def _guard_ok(rep: Dict[str, Any], *, min_ece_abs: float, min_brier_abs: float) -> Tuple[bool, Dict[str, Any]]:
    raw = rep.get("raw") or {}
    cal = rep.get("cal") or {}
    try:
        raw_ece = float(raw.get("ece", 0.0) or 0.0)
        raw_brier = float(raw.get("brier", 0.0) or 0.0)
        cal_ece = float(cal.get("ece", 0.0) or 0.0)
        cal_brier = float(cal.get("brier", 0.0) or 0.0)
        ok = (cal_ece <= (raw_ece - float(min_ece_abs))) and (cal_brier <= (raw_brier - float(min_brier_abs)))
        return ok, {
            "raw_ece": raw_ece
            "raw_brier": raw_brier
            "cal_ece": cal_ece
            "cal_brier": cal_brier
            "min_ece_abs": float(min_ece_abs)
            "min_brier_abs": float(min_brier_abs)
            "passed": bool(ok)
        }
    except Exception:
        return False, {"passed": False}


def main(argv: Optional[list[str]] = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--out_dir", default=os.environ.get("CONF_CAL_OUT_DIR", "/var/lib/trade/of_calibrators"))
    ap.add_argument("--reports_dir", default=os.environ.get("CONF_CAL_REPORTS_DIR", "/var/lib/trade/of_reports/out/confidence_cal"))
    ap.add_argument("--lookback_days", type=int, default=int(os.environ.get("CONF_CAL_LOOKBACK_DAYS", "7")))
    ap.add_argument("--key", default=os.environ.get("CONF_CAL_KEY", "confidence_v1"))
    ap.add_argument("--method", default=os.environ.get("CONF_CAL_METHOD", "temp"), choices=["temp", "platt"])
    ap.add_argument("--min_rows", type=int, default=int(os.environ.get("CONF_CAL_MIN_ROWS", "5000")))
    ap.add_argument("--guard_min_ece_abs", type=float, default=float(os.environ.get("CONF_CAL_GUARD_MIN_ECE_ABS", "0.001")))
    ap.add_argument("--guard_min_brier_abs", type=float, default=float(os.environ.get("CONF_CAL_GUARD_MIN_BRIER_ABS", "0.0005")))
    ap.add_argument("--y_min_r", type=float, default=float(os.environ.get("Y_MIN_R", "0.10")))
    ap.add_argument("--signals_count", type=int, default=int(os.environ.get("SIGNALS_COUNT", "200000")))
    ap.add_argument("--closes_count", type=int, default=int(os.environ.get("CLOSES_COUNT", "200000")))
    args = ap.parse_args(list(argv) if argv is not None else None)

    os.makedirs(str(args.out_dir), exist_ok=True)
    os.makedirs(str(args.reports_dir), exist_ok=True)
    versions_dir = os.path.join(str(args.out_dir), "versions")
    os.makedirs(versions_dir, exist_ok=True)

    now_ms = _now_ms()
    since_ms = now_ms - int(args.lookback_days) * 86400 * 1000
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Paths
    dataset_jsonl = os.path.join(str(args.reports_dir), f"edge_train_confcal_{stamp}.jsonl")
    quarantine_jsonl = os.path.join(str(args.reports_dir), f"edge_quarantine_confcal_{stamp}.jsonl")
    dataset_report = os.path.join(str(args.reports_dir), f"edge_dataset_report_confcal_{stamp}.json")
    cal_tmp = os.path.join(str(args.reports_dir), f"conf_cal_{stamp}.json")
    cal_ver = os.path.join(versions_dir, f"conf_cal_{stamp}.json")
    cal_latest = os.path.join(str(args.out_dir), "conf_cal_latest.json")

    calib_report_json = os.path.join(str(args.reports_dir), "confidence_calibration_report.json")
    status_json = os.path.join(str(args.reports_dir), "confidence_calibration_status.json")

    status: Dict[str, Any] = {
        "ts_ms": int(now_ms)
        "stamp": stamp
        "ok": False
        "deployed": False
        "paths": {
            "dataset_jsonl": dataset_jsonl
            "cal_tmp": cal_tmp
            "cal_latest": cal_latest
            "cal_version": cal_ver
            "calib_report_json": calib_report_json
        }
    }

    # 1) Build dataset
    ok, out, err = _run(
        "ml_analysis.tools.build_edge_stack_dataset_from_redis"
        [
            "--redis_url", str(args.redis_url)
            "--out_jsonl", dataset_jsonl
            "--out_quarantine_jsonl", quarantine_jsonl
            "--out_report_json", dataset_report
            "--signals_count", str(int(args.signals_count))
            "--closes_count", str(int(args.closes_count))
            "--since_ms", str(int(since_ms))
            "--until_ms", str(int(now_ms))
            "--y_min_r", str(float(args.y_min_r))
        ]
        timeout=3600
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
    method_arg = str(args.method)
    if method_arg not in ("auto", "platt", "isotonic", "beta", "identity"):
        method_arg = "auto"

    ok, out, err = _run(
        "ml_analysis.tools.train_confidence_calibrator_v2"
        [
            "--in_jsonl", dataset_jsonl
            "--out_bundle", cal_tmp
            "--key", str(args.key)
            "--method", method_arg
            "--min_rows", str(int(args.min_rows))
            "--hierarchical", "1"
        ]
        timeout=3600
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
        "ml_analysis.tools.calibration_report"
        [
            "--in_jsonl", dataset_jsonl
            "--out_json", calib_report_json
        ]
        timeout=1200
    )
    status["report_ok"] = bool(ok)
    if not ok:
        # non-fatal: still can deploy calibrator if guard passes
        status["report_error"] = err[-2000:]

    # 4) Guard + deploy
    g_ok, g = _guard_ok(rep, min_ece_abs=float(args.guard_min_ece_abs), min_brier_abs=float(args.guard_min_brier_abs))
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
