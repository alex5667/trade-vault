from __future__ import annotations

"""Live calibration health loop + safe rollback.

Run periodically (hourly recommended) to validate that calibrated confidence
continues to improve (or at least does not materially degrade) calibration
metrics on recent outcomes.

Workflow:
  1) Build a recent joined JSONL dataset from Redis streams:
       - signals:of:inputs (feature snapshots with indicators)
       - trades:closed     (outcomes with pnl/risk)
  2) Compute live metrics for raw vs calibrated confidence:
       - ECE (expected calibration error)
       - Brier score
       - Precision@Top5% (ranking sanity)
       - Expectancy R@Top5% (if r_mult present)
  3) Guard:
       FAIL if calibrated is materially worse than raw:
         cal_ece   > raw_ece   + ece_worse_abs
      OR cal_brier > raw_brier + brier_worse_abs
  4) If FAIL repeats for K consecutive runs (bad streak) -> rollback:
       conf_cal_latest.json <- previous version from <out_dir>/versions/

This tool is fail-open:
  - If not enough data or no calibrated values in joined JSONL, it writes status
    and exits without changing anything.

Env defaults (can be overridden by CLI):
  REDIS_URL=redis://localhost:6379/0
  CONF_CAL_OUT_DIR=/var/lib/trade/of_calibrators
  CONF_CAL_LIVE_REPORTS_DIR=/var/lib/trade/of_reports/out/confidence_cal_live
  CONF_CAL_LIVE_LOOKBACK_HOURS=24
  CONF_CAL_LIVE_MIN_ROWS=800
  CONF_CAL_LIVE_BAD_STREAK=3
  CONF_CAL_LIVE_ECE_WORSE_ABS=0.01
  CONF_CAL_LIVE_BRIER_WORSE_ABS=0.002
  CONF_CAL_LIVE_ROLLBACK_COOLDOWN_MIN=360
  SIGNALS_COUNT=200000
  CLOSES_COUNT=200000
  Y_MIN_R=0.10
"""

import argparse
import hashlib
import json
import math
import os
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis

# Works when executed as either:
#   python -m tools.confidence_cal_live_health_loop_v1
# or:
#   python -m ml_analysis.tools.confidence_cal_live_health_loop_v1
from .build_edge_stack_dataset_from_redis import main as build_dataset_main


def _now_ms() -> int:
    return get_ny_time_millis()


def _as_float(x: Any) -> float | None:
    try:
        f = float(x)
        if math.isfinite(f):
            return f
    except Exception:
        return None
    return None


def _clamp01(p: float) -> float:
    if p <= 0.0:
        return 0.0
    if p >= 1.0:
        return 1.0
    return p


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_replace(src: str, dst: str) -> None:
    # Atomic replace within same filesystem
    tmp = dst + ".tmp"
    with open(src, "rb") as r:
        data = r.read()
    os.makedirs(os.path.dirname(os.path.abspath(dst)) or ".", exist_ok=True)
    with open(tmp, "wb") as w:
        w.write(data)
        w.flush()
        os.fsync(w.fileno())
    os.replace(tmp, dst)


def _ece(y: list[int], p: list[float], bins: int = 20) -> float:
    # Deterministic, no numpy
    n = len(y)
    if n == 0:
        return float("nan")
    counts = [0] * bins
    sum_p = [0.0] * bins
    sum_y = [0.0] * bins
    for yy, pp in zip(y, p):
        pp = _clamp01(pp)
        b = min(bins - 1, int(pp * bins))
        counts[b] += 1
        sum_p[b] += pp
        sum_y[b] += float(yy)
    e = 0.0
    for c, sp, sy in zip(counts, sum_p, sum_y):
        if c <= 0:
            continue
        w = c / n
        conf = sp / c
        acc = sy / c
        e += w * abs(acc - conf)
    return float(e)


def _brier(y: list[int], p: list[float]) -> float:
    n = len(y)
    if n == 0:
        return float("nan")
    s = 0.0
    for yy, pp in zip(y, p):
        pp = _clamp01(pp)
        d = pp - float(yy)
        s += d * d
    return float(s / n)


def _precision_topk(y: list[int], p: list[float], frac: float = 0.05) -> float:
    n = len(y)
    if n == 0:
        return float("nan")
    k = max(1, int(n * frac))
    idx = sorted(range(n), key=lambda i: p[i], reverse=True)[:k]
    return float(sum(y[i] for i in idx) / k)


def _expectancy_topk(r: list[float], p: list[float], frac: float = 0.05) -> float:
    n = len(p)
    if n == 0:
        return float("nan")
    k = max(1, int(n * frac))
    idx = sorted(range(n), key=lambda i: p[i], reverse=True)[:k]
    vals = [r[i] for i in idx if r[i] is not None and math.isfinite(r[i])]
    if not vals:
        return float("nan")
    return float(sum(vals) / len(vals))


def _report(y: list[int], r: list[float], p: list[float]) -> dict[str, float]:
    return {
        "rows": float(len(y)),
        "ece": _ece(y, p),
        "brier": _brier(y, p),
        "precision_top5pct": _precision_topk(y, p, 0.05),
        "expectancy_r_top5pct": _expectancy_topk(r, p, 0.05),
    }


def _get_indicator(ind: dict[str, Any], keys: list[str]) -> float | None:
    for k in keys:
        if k not in ind:
            continue
        f = _as_float(ind.get(k))
        if f is None:
            continue
        if not math.isfinite(f):
            continue
        # Producer writes calibrated value as percentile (0..100) under "*_pct" keys.
        if k.endswith("_pct"):
            f = f / 100.0
        return _clamp01(f)
    return None


def _load_joined_jsonl(
    path: str,
    *,
    raw_keys: list[str],
    cal_keys: list[str],
) -> tuple[list[int], list[float], list[float], list[float]]:
    y: list[int] = []
    r: list[float] = []
    p_raw: list[float] = []
    p_cal: list[float] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                row = json.loads(s)
            except Exception:
                continue
            ind = row.get("indicators") or {}
            if not isinstance(ind, dict):
                continue
            yy = int(row.get("y", 0) or 0)
            rr = _as_float(row.get("r_mult"))
            rr = rr if rr is not None else float("nan")
            pr = _get_indicator(ind, raw_keys)
            if pr is None:
                continue
            pc = _get_indicator(ind, cal_keys)

            y.append(1 if yy else 0)
            r.append(float(rr))
            p_raw.append(float(pr))
            # for cal we still append placeholder NaN to keep alignment (optional)
            p_cal.append(float(pc) if pc is not None else float("nan"))
    return y, r, p_raw, p_cal


def _pick_rollback_version(out_dir: str, latest_path: str) -> str | None:
    versions_dir = os.path.join(out_dir, "versions")
    if not os.path.isdir(versions_dir):
        return None
    files = []
    for name in os.listdir(versions_dir):
        if not name.endswith(".json"):
            continue
        p = os.path.join(versions_dir, name)
        try:
            st = os.stat(p)
        except Exception:
            continue
        files.append((st.st_mtime, p))
    if len(files) < 2:
        return None
    files.sort(key=lambda t: t[0])  # oldest -> newest

    try:
        latest_hash = _sha256_file(latest_path)
    except Exception:
        latest_hash = ""

    idx = None
    if latest_hash:
        for i, (_, p) in enumerate(files):
            try:
                if _sha256_file(p) == latest_hash:
                    idx = i
                    break
            except Exception:
                continue

    # Prefer previous to current if we can locate it
    if idx is not None and idx > 0:
        return files[idx - 1][1]

    # Fallback: second newest
    return files[-2][1]


def _read_json(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _write_json_atomic(path: str, obj: dict[str, Any]) -> None:
    tmp = path + ".tmp"
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--out_dir", default=os.environ.get("CONF_CAL_OUT_DIR", "/var/lib/trade/of_calibrators"))
    ap.add_argument(
        "--reports_dir",
        default=os.environ.get("CONF_CAL_LIVE_REPORTS_DIR", "/var/lib/trade/of_reports/out/confidence_cal_live"),
    )
    ap.add_argument("--lookback_hours", type=int, default=int(os.environ.get("CONF_CAL_LIVE_LOOKBACK_HOURS", "24")))
    ap.add_argument("--min_rows", type=int, default=int(os.environ.get("CONF_CAL_LIVE_MIN_ROWS", "800")))
    ap.add_argument("--bad_streak", type=int, default=int(os.environ.get("CONF_CAL_LIVE_BAD_STREAK", "3")))
    ap.add_argument("--ece_worse_abs", type=float, default=float(os.environ.get("CONF_CAL_LIVE_ECE_WORSE_ABS", "0.01")))
    ap.add_argument(
        "--brier_worse_abs",
        type=float,
        default=float(os.environ.get("CONF_CAL_LIVE_BRIER_WORSE_ABS", "0.002")),
    )
    ap.add_argument(
        "--mce_worse_abs",
        type=float,
        default=float(os.environ.get("CONF_CAL_LIVE_MCE_WORSE_ABS", "inf")),
    )
    ap.add_argument(
        "--sharpness_drop_abs",
        type=float,
        default=float(os.environ.get("CONF_CAL_LIVE_SHARPNESS_DROP_ABS", "inf")),
    )
    ap.add_argument(
        "--rollback_cooldown_min",
        type=int,
        default=int(os.environ.get("CONF_CAL_LIVE_ROLLBACK_COOLDOWN_MIN", "360")),
    )
    ap.add_argument("--signals_count", type=int, default=int(os.environ.get("SIGNALS_COUNT", "200000")))
    ap.add_argument("--closes_count", type=int, default=int(os.environ.get("CLOSES_COUNT", "200000")))
    ap.add_argument("--y_min_r", type=float, default=float(os.environ.get("Y_MIN_R", "0.10")))

    args = ap.parse_args(list(argv) if argv is not None else None)

    now = _now_ms()
    since_ms = now - int(args.lookback_hours) * 3600 * 1000

    os.makedirs(args.reports_dir, exist_ok=True)
    os.makedirs(args.out_dir, exist_ok=True)

    latest_path = os.path.join(args.out_dir, "conf_cal_latest.json")
    status_path = os.path.join(args.reports_dir, "confidence_calibration_live_status.json")

    dataset_path = os.path.join(args.reports_dir, f"edge_live_{now}.jsonl")
    dataset_report = os.path.join(args.reports_dir, f"edge_live_report_{now}.json")
    quarantine_path = os.path.join(args.reports_dir, f"edge_live_quarantine_{now}.jsonl")

    prev = _read_json(status_path)
    prev_streak = int(prev.get("bad_streak", 0) or 0)
    prev_rb_ts = int(prev.get("last_rollback_ts_ms", 0) or 0)

    status: dict[str, Any] = {
        "ts_ms": now,
        "since_ms": int(since_ms),
        "lookback_hours": int(args.lookback_hours),
        "paths": {
            "dataset_jsonl": dataset_path,
            "dataset_report": dataset_report,
            "quarantine_jsonl": quarantine_path,
            "latest_calibrator": latest_path,
        },
        "ok": False,
        "skipped": False,
        "bad_streak": prev_streak,
        "guard_passed": None,
        "rollback": {"performed": False},
    }

    # 1) Build dataset (deterministic join)
    try:
        rc = build_dataset_main(
            [
                "--redis_url",
                str(args.redis_url),
                "--out_jsonl",
                dataset_path,
                "--out_report_json",
                dataset_report,
                "--out_quarantine_jsonl",
                quarantine_path,
                "--signals_count",
                str(int(args.signals_count)),
                "--closes_count",
                str(int(args.closes_count)),
                "--since_ms",
                str(int(since_ms)),
                "--until_ms",
                str(int(now)),
                "--y_min_r",
                str(float(args.y_min_r)),
            ]
        )
    except redis.exceptions.RedisError as e:
        print(f"Redis error during dataset build: {type(e).__name__} - {e}")
        status["error"] = f"redis_error: {type(e).__name__}"
        status["ok"] = False
        _write_json_atomic(status_path, status)
        return 2
    if int(rc) != 0:
        status["error"] = "dataset_build_failed"
        _write_json_atomic(status_path, status)
        return 2

    # 2) Load + compute metrics
    y, r, p_raw, p_cal_all = _load_joined_jsonl(
        dataset_path,
        raw_keys=["confidence_v1", "confidence"],
        cal_keys=["confidence_cal_v1", "confidence_calibrated_pct"],
    )
    status["rows_raw"] = int(len(p_raw))

    # Filter calibrated finite
    y_cal: list[int] = []
    r_cal: list[float] = []
    p_cal: list[float] = []
    for yy, rr, pc in zip(y, r, p_cal_all):
        if pc is None or (isinstance(pc, float) and not math.isfinite(pc)):
            continue
        if not math.isfinite(float(pc)):
            continue
        y_cal.append(int(yy))
        r_cal.append(float(rr))
        p_cal.append(float(pc))

    status["rows_cal"] = int(len(p_cal))

    if len(p_raw) < int(args.min_rows):
        status["skipped"] = True
        status["skip_reason"] = f"insufficient_raw_rows<{int(args.min_rows)}"
        status["ok"] = True
        _write_json_atomic(status_path, status)
        return 0

    if len(p_cal) < int(args.min_rows):
        status["skipped"] = True
        status["skip_reason"] = f"insufficient_cal_rows<{int(args.min_rows)}"
        status["ok"] = True
        _write_json_atomic(status_path, status)
        return 0

    raw_rep = _report(y, r, p_raw)
    cal_rep = _report(y_cal, r_cal, p_cal)

    status["metrics_raw_v1"] = raw_rep
    status["metrics_cal_v1"] = cal_rep

    raw_ece = float(raw_rep.get("ece", 0.0))
    raw_brier = float(raw_rep.get("brier", 0.0))
    raw_mce = float(raw_rep.get("mce", float("nan")))
    raw_sharp = float(raw_rep.get("sharpness_mean", float("nan")))
    cal_ece = float(cal_rep.get("ece", 0.0))
    cal_brier = float(cal_rep.get("brier", 0.0))
    cal_mce = float(cal_rep.get("mce", float("nan")))
    cal_sharp = float(cal_rep.get("sharpness_mean", float("nan")))

    guard_fail = False
    reasons: list[str] = []
    if math.isfinite(raw_ece) and math.isfinite(cal_ece) and cal_ece > raw_ece + float(args.ece_worse_abs):
        guard_fail = True
        reasons.append("ece_worse")
    if math.isfinite(raw_brier) and math.isfinite(cal_brier) and cal_brier > raw_brier + float(args.brier_worse_abs):
        guard_fail = True
        reasons.append("brier_worse")
    if math.isfinite(float(args.mce_worse_abs)) and math.isfinite(raw_mce) and math.isfinite(cal_mce) and cal_mce > raw_mce + float(args.mce_worse_abs):
        guard_fail = True
        reasons.append("mce_worse")
    if math.isfinite(float(args.sharpness_drop_abs)) and math.isfinite(raw_sharp) and math.isfinite(cal_sharp) and cal_sharp < raw_sharp - float(args.sharpness_drop_abs):
        guard_fail = True
        reasons.append("sharpness_drop")

    status["guard"] = {
        "ece_worse_abs": float(args.ece_worse_abs),
        "brier_worse_abs": float(args.brier_worse_abs),
        "mce_worse_abs": float(args.mce_worse_abs) if math.isfinite(float(args.mce_worse_abs)) else None,
        "sharpness_drop_abs": float(args.sharpness_drop_abs) if math.isfinite(float(args.sharpness_drop_abs)) else None,
        "raw_ece": raw_ece,
        "raw_brier": raw_brier,
        "raw_mce": raw_mce,
        "raw_sharpness_mean": raw_sharp,
        "cal_ece": cal_ece,
        "cal_brier": cal_brier,
        "cal_mce": cal_mce,
        "cal_sharpness_mean": cal_sharp,
        "fail": bool(guard_fail),
        "reasons": reasons,
    }

    if guard_fail:
        status["guard_passed"] = False
        status["bad_streak"] = prev_streak + 1
    else:
        status["guard_passed"] = True
        status["bad_streak"] = 0

    # 3) Rollback if bad streak reached, with cooldown
    if status["bad_streak"] >= int(args.bad_streak):
        cooldown_ms = int(args.rollback_cooldown_min) * 60 * 1000
        if prev_rb_ts > 0 and (now - prev_rb_ts) < cooldown_ms:
            status["rollback"] = {"performed": False, "reason": "cooldown_active", "cooldown_ms": cooldown_ms}
        else:
            if os.path.isfile(latest_path):
                cand = _pick_rollback_version(str(args.out_dir), latest_path)
                if cand and os.path.isfile(cand):
                    _atomic_replace(cand, latest_path)
                    status["rollback"] = {"performed": True, "from": latest_path, "to": cand}
                    status["last_rollback_ts_ms"] = int(now)
                    # After rollback, reset streak to avoid rapid oscillation
                    status["bad_streak"] = 0
                else:
                    status["rollback"] = {"performed": False, "reason": "no_candidate"}
            else:
                status["rollback"] = {"performed": False, "reason": "latest_missing"}

    status["ok"] = True
    _write_json_atomic(status_path, status)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
