from __future__ import annotations

import argparse
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2

from common.isotonic_calibration import fit_isotonic_pav, IsotonicCalibrator, _clamp01
from common.kind_normalize import normalize_kind


def _env(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n, "")
        if v:
            return str(v)
    return str(default)


def _now_ts() -> float:
    return float(time.time())


def _finite(x: float) -> bool:
    return bool(math.isfinite(float(x)))


def _label(outcome: str, realized_r: Optional[float]) -> Optional[int]:
    """
    Profit-aware labels:
      - target_hit -> 1
      - stop_hit   -> 0
      - expired_no_entry -> None (не было сделки)
      - manual_exit -> 1 если realized_R > 0 иначе 0
      - expired_no_target -> 0 (вход был, цели не достигли)
      - breakeven -> 0
      - прочее: если realized_R известен -> (realized_R > 0 ? 1 : 0) иначе None
    """
    o = str(outcome or "").strip().lower()
    if o == "target_hit":
        return 1
    if o == "stop_hit":
        return 0
    if o == "expired_no_entry":
        return None
    if o in ("manual_exit", "expired_no_target", "breakeven"):
        if realized_r is not None and _finite(realized_r):
            return 1 if float(realized_r) > 0.0 else 0
        return 0
    if realized_r is not None and _finite(realized_r):
        return 1 if float(realized_r) > 0.0 else 0
    return None


@dataclass
class TrainRow:
    ts_signal: float
    symbol: str
    setup_type: str
    side: str
    final_score: float
    outcome: str
    realized_r: Optional[float]


def _iter_rows(conn, *, since: str, fetch: int = 5000) -> Iterable[TrainRow]:
    sql = """
    SELECT
        EXTRACT(EPOCH FROM p.ts_signal) AS ts_epoch,
        p.symbol,
        p.setup_type,
        p.side,
        s.final_score,
        p.outcome,
        p.realized_R
    FROM signal_performance p
    JOIN signals s ON p.signal_id = s.signal_id
    WHERE p.ts_signal >= %s
      AND s.final_score IS NOT NULL
    ORDER BY p.ts_signal ASC
    """
    with conn.cursor(name="conf_calib_cursor") as cur:
        cur.itersize = int(fetch)
        cur.execute(sql, (since,))
        for r in cur:
            ts_epoch, symbol, setup_type, side, final_score, outcome, realized_r = r
            yield TrainRow(
                ts_signal=float(ts_epoch or 0.0),
                symbol=str(symbol or "*"),
                setup_type=str(setup_type or "*"),
                side=str(side or "*"),
                final_score=float(final_score or 0.0),
                outcome=str(outcome or ""),
                realized_r=(float(realized_r) if realized_r is not None else None),
            )


# _normalize_kind is now imported from common.kind_normalize


def _group_key(kind: str, symbol: str) -> str:
    return f"kind:{kind}|symbol:{symbol}"


def _brier(p: float, y: int) -> float:
    pp = float(_clamp01(p))
    yy = 1.0 if int(y) == 1 else 0.0
    return (pp - yy) ** 2


def _ece(samples: List[Tuple[float, int]], bins: int = 20) -> float:
    # Expected Calibration Error по равным бинам вероятности
    if not samples:
        return 0.0
    bins = max(5, int(bins))
    counts = [0] * bins
    sum_p = [0.0] * bins
    sum_y = [0.0] * bins
    for p, y in samples:
        pp = float(_clamp01(p))
        i = min(bins - 1, int(pp * bins))
        counts[i] += 1
        sum_p[i] += pp
        sum_y[i] += 1.0 if int(y) == 1 else 0.0
    n = float(len(samples))
    e = 0.0
    for i in range(bins):
        if counts[i] <= 0:
            continue
        avg_p = sum_p[i] / counts[i]
        avg_y = sum_y[i] / counts[i]
        e += (counts[i] / n) * abs(avg_p - avg_y)
    return float(e)


def _load_calib_file(path: str) -> Tuple[Dict[str, IsotonicCalibrator], Dict[str, int], int]:
    """
    Возвращает:
      - calibrators: key -> IsotonicCalibrator
      - ns: key -> n
      - trained_at (0 если нет)
    Fail-open: на любой ошибке возвращает пустые dict.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f) or {}
        trained_at = int(obj.get("trained_at", 0) or 0)
        groups = (obj.get("groups", {}) or {})
        out: Dict[str, IsotonicCalibrator] = {}
        ns: Dict[str, int] = {}
        for k, v in groups.items():
            vv = v or {}
            if vv.get("type") != "isotonic":
                continue
            x = list(vv.get("x", []) or [])
            p = list(vv.get("p", []) or [])
            mode = str(vv.get("mode", "linear") or "linear")
            n = int(vv.get("n", 0) or 0)
            cal = IsotonicCalibrator(x=[float(t) for t in x], p=[float(t) for t in p], mode=mode).sanitize()
            if not cal.x or not cal.p or len(cal.x) != len(cal.p):
                continue
            out[str(k)] = cal
            ns[str(k)] = n
        return out, ns, trained_at
    except Exception:
        return {}, {}, 0


def train(
    *,
    dsn: str,
    since: str,
    out_path: str,
    min_samples: int = 300,
    val_days: int = 14,
    mode: str = "linear",
    seed: int = 7,
    baseline_path: Optional[str] = None,
) -> Dict[str, object]:
    """
    Обучает global + kind|symbol из signal_performance.
    Пишет out_path (json) и out_path + '.report.json'.
    Возвращает report dict.
    """
    if not dsn:
        raise RuntimeError("DSN is empty (use PERF_PG_DSN/TRADES_DB_DSN or --dsn)")

    conn = psycopg2.connect(dsn)
    # conn.autocommit = True  # named cursor requires a transaction

    now = _now_ts()
    val_cut = now - float(max(1, int(val_days))) * 86400.0

    # samples dict: key -> list[(x, y, w)] for train
    train_samples: Dict[str, List[Tuple[float, int, float]]] = {"global": []}
    val_samples: Dict[str, List[Tuple[float, int]]] = {"global": []}  # p will be predicted later

    total_rows = 0
    eligible_rows = 0
    max_ts = 0.0

    for row in _iter_rows(conn, since=since):
        total_rows += 1
        max_ts = max(max_ts, float(row.ts_signal))
        kind = normalize_kind(row.setup_type)
        symbol = str(row.symbol or "*")
        y = _label(row.outcome, row.realized_r)
        if y is None:
            continue
        x = abs(float(row.final_score))
        if not _finite(x):
            continue
        eligible_rows += 1

        gk = _group_key(kind, symbol)
        if gk not in train_samples:
            train_samples[gk] = []
            val_samples[gk] = []

        # split train/val by time (последние val_days суток — валидация)
        if float(row.ts_signal) >= val_cut:
            val_samples["global"].append((x, int(y)))
            val_samples[gk].append((x, int(y)))
        else:
            train_samples["global"].append((x, int(y), 1.0))
            train_samples[gk].append((x, int(y), 1.0))

    conn.close()

    # fit calibrators
    groups_out: Dict[str, Dict[str, object]] = {}
    report_groups: Dict[str, Dict[str, object]] = {}          # candidate metrics
    baseline_groups: Dict[str, Dict[str, object]] = {}        # baseline metrics (на том же val)
    delta_groups: Dict[str, Dict[str, object]] = {}           # candidate - baseline

    def _fit_one(key: str, s: List[Tuple[float, int, float]]) -> Optional[IsotonicCalibrator]:
        if not s:
            return None
        cal = fit_isotonic_pav(s)
        cal.mode = mode
        return cal.sanitize()

    # global always
    cal_global = _fit_one("global", train_samples.get("global", []))
    n_global = len(train_samples.get("global", []))
    if cal_global is not None:
        groups_out["global"] = {"type": "isotonic", "x": cal_global.x, "p": cal_global.p, "mode": cal_global.mode, "n": n_global}

    # per kind|symbol
    for k, s in sorted(train_samples.items(), key=lambda t: t[0]):
        if k == "global":
            continue
        n = len(s)
        if n < int(min_samples):
            continue
        cal = _fit_one(k, s)
        if cal is None:
            continue
        groups_out[k] = {"type": "isotonic", "x": cal.x, "p": cal.p, "mode": cal.mode, "n": n}

    # validation metrics (Brier/ECE) for global + each group present
    def _eval(key: str, cal: Optional[IsotonicCalibrator], vals: List[Tuple[float, int]]) -> Dict[str, float]:
        if not vals or cal is None:
            return {"n_val": float(len(vals)), "brier": float("nan"), "ece": float("nan")}
        preds: List[Tuple[float, int]] = []
        bsum = 0.0
        for x, y in vals:
            p = float(_clamp01(cal.predict(float(x))))
            preds.append((p, y))
            bsum += _brier(p, y)
        return {
            "n_val": float(len(vals)),
            "brier": float(bsum / max(1.0, float(len(vals)))),
            "ece": float(_ece(preds, bins=20)),
        }

    # baseline load (optional)
    base_cals: Dict[str, IsotonicCalibrator] = {}
    base_ns: Dict[str, int] = {}
    base_trained_at = 0
    if baseline_path and os.path.exists(str(baseline_path)):
        base_cals, base_ns, base_trained_at = _load_calib_file(str(baseline_path))
        # sanitize safety
        base_cals = {k: v.sanitize() for k, v in (base_cals or {}).items()}

    # build report for keys we output
    trained_at = int(now)
    for key, obj in groups_out.items():
        cal = IsotonicCalibrator(x=list(obj["x"]), p=list(obj["p"]), mode=str(obj.get("mode", "linear"))).sanitize()
        vals = val_samples.get(key, [])
        cand_m = {
            "n_train": float(int(obj.get("n", 0) or 0)),
            **_eval(key, cal, vals),
        }
        report_groups[key] = cand_m

        # baseline metrics for same vals (fallback на global baseline, если key нет)
        base_cal = None
        if base_cals:
            base_cal = base_cals.get(key) or base_cals.get("global")
        base_m = {
            "n_train": float(int(base_ns.get(key, 0) or base_ns.get("global", 0) or 0)),
            **_eval(key, base_cal, vals),
        }
        baseline_groups[key] = base_m

        # delta = cand - base (если base brier/ece nan -> delta nan)
        def _d(a: float, b: float) -> float:
            if not (math.isfinite(float(a)) and math.isfinite(float(b))):
                return float("nan")
            return float(a - b)
        delta_groups[key] = {
            "brier": _d(float(cand_m.get("brier", float("nan"))), float(base_m.get("brier", float("nan")))),
            "ece": _d(float(cand_m.get("ece", float("nan"))), float(base_m.get("ece", float("nan")))),
            "n_val": float(cand_m.get("n_val", 0.0)),
        }

    out_obj = {
        "version": 1,
        "trained_at": trained_at,
        "since": str(since),
        "max_ts_epoch": float(max_ts),
        "groups": groups_out,
    }

    # atomic write + backup (fail-open)
    tmp = out_path + ".tmp"
    bak = out_path + ".bak"
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    if os.path.exists(out_path):
        try:
            with open(out_path, "rb") as fsrc:
                with open(bak, "wb") as fdst:
                    fdst.write(fsrc.read())
        except Exception:
            pass
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out_obj, f, ensure_ascii=False, sort_keys=True, indent=2)
    os.replace(tmp, out_path)

    report = {
        "trained_at": trained_at,
        "baseline_trained_at": int(base_trained_at),
        "since": str(since),
        "val_days": int(val_days),
        "val_cut_epoch": float(val_cut),
        "total_rows": int(total_rows),
        "eligible_rows": int(eligible_rows),
        "max_ts_epoch": float(max_ts),
        "groups_written": int(len(groups_out)),
        "groups": report_groups,
        "baseline_groups": baseline_groups,
        "delta_groups": delta_groups,
    }
    with open(out_path + ".report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, sort_keys=True, indent=2)
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=_env("PERF_PG_DSN", "TRADES_DB_DSN", default=""))
    ap.add_argument("--since", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-samples", type=int, default=int(os.getenv("CONF_CAL_MIN_SAMPLES", "300")))
    ap.add_argument("--val-days", type=int, default=int(os.getenv("CONF_CAL_VAL_DAYS", "14")))
    ap.add_argument("--mode", default=os.getenv("CONF_CAL_ISO_MODE", "linear"))
    ap.add_argument("--baseline", default=os.getenv("CONF_CAL_BASELINE_PATH", ""))
    args = ap.parse_args()

    rep = train(
        dsn=str(args.dsn),
        since=str(args.since),
        out_path=str(args.out),
        min_samples=int(args.min_samples),
        val_days=int(args.val_days),
        mode=str(args.mode),
        baseline_path=(str(args.baseline) if str(args.baseline or "") else None),
    )
    print(json.dumps(rep, ensure_ascii=False))


if __name__ == "__main__":
    main()
