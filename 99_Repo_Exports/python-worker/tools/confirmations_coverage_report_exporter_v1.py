from __future__ import annotations
"""Confirmations coverage report exporter (v1).

Exposes Prometheus metrics from confirmations coverage JSON report.

Env:
  CONFIRMATIONS_COVERAGE_OUT_JSON              report JSON path
  CONFIRMATIONS_COVERAGE_REPORT_STALE_SEC      stale threshold (default 36h)
"""


import argparse
import json
import os
import time
from typing import Any, Dict, Iterable, Optional

from prometheus_client import Gauge, start_http_server  # type: ignore


REPORT_PRESENT = Gauge("confirmations_coverage_report_present", "1 if report file exists")
REPORT_PARSED_OK = Gauge("confirmations_coverage_report_parsed_ok", "1 if report parsed")
REPORT_TS_MS = Gauge("confirmations_coverage_report_ts_ms", "report ts_ms")
REPORT_AGE_SEC = Gauge("confirmations_coverage_report_age_sec", "age of report in seconds")
REPORT_STALE = Gauge("confirmations_coverage_report_stale", "1 if report stale")

N_ROWS = Gauge("confirmations_coverage_n_rows", "rows in dataset")

FEAT_PRESENT = Gauge("confirmations_coverage_feat_present", "1 if feature column present", ["feat"])
FEAT_NONNULL_RATE = Gauge("confirmations_coverage_feat_nonnull_rate", "nonnull rate", ["feat"])
FEAT_NONZERO_RATE = Gauge("confirmations_coverage_feat_nonzero_rate", "nonzero rate", ["feat"])
FEAT_MEAN = Gauge("confirmations_coverage_feat_mean", "mean", ["feat"])

CONF_MIN_NONZERO = Gauge("confirmations_coverage_conf_min_nonzero_rate", "min nonzero rate across conf_*")
CONF_BAD_ALL_ZERO = Gauge("confirmations_coverage_conf_bad_all_zero", "1 if all conf_* are zero")
COVERAGE_REASON = Gauge("confirmations_coverage_reason", "one-hot coverage reason code", ["reason"])


REASON_CODES = (
    "dataset_missing",
    "dataset_load_failed",
    "n_rows_low",
    "conf_cols_missing",
    "conf_all_zero",
    "conf_low_nonzero_rate",
)


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _now_sec() -> float:
    return time.time()


def _clear() -> None:
    REPORT_PRESENT.set(0.0)
    REPORT_PARSED_OK.set(0.0)
    REPORT_TS_MS.set(0.0)
    REPORT_AGE_SEC.set(0.0)
    REPORT_STALE.set(0.0)
    N_ROWS.set(0.0)
    CONF_MIN_NONZERO.set(0.0)
    CONF_BAD_ALL_ZERO.set(0.0)
    for r in REASON_CODES:
        COVERAGE_REASON.labels(reason=r).set(0.0)


def _load_report(path: str) -> Optional[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _set_onehot_reasons(reasons: Iterable[str]) -> None:
    reason_set = set([str(r) for r in reasons if r])
    for r in REASON_CODES:
        COVERAGE_REASON.labels(reason=r).set(1.0 if r in reason_set else 0.0)


def update_metrics(rep: Dict[str, Any], stale_sec: int) -> None:
    REPORT_PRESENT.set(1.0)
    REPORT_PARSED_OK.set(1.0)

    ts_ms = int(rep.get("ts_ms") or 0)
    REPORT_TS_MS.set(float(ts_ms))

    age = 0.0
    if ts_ms > 0:
        age = max(0.0, _now_sec() - (ts_ms / 1000.0))
    REPORT_AGE_SEC.set(age)
    REPORT_STALE.set(1.0 if (stale_sec > 0 and age > stale_sec) else 0.0)

    counts = rep.get("counts") or {}
    N_ROWS.set(_to_float(counts.get("n_rows"), 0.0))

    summ = rep.get("summary") or {}
    CONF_MIN_NONZERO.set(_to_float(summ.get("conf_min_nonzero_rate"), 0.0))
    CONF_BAD_ALL_ZERO.set(1.0 if bool(summ.get("conf_bad_all_zero", False)) else 0.0)

    feats = rep.get("features") or {}
    for feat, st in feats.items():
        FEAT_PRESENT.labels(feat=str(feat)).set(_to_float((st or {}).get("present"), 0.0))
        FEAT_NONNULL_RATE.labels(feat=str(feat)).set(_to_float((st or {}).get("nonnull_rate"), 0.0))
        FEAT_NONZERO_RATE.labels(feat=str(feat)).set(_to_float((st or {}).get("nonzero_rate"), 0.0))
        FEAT_MEAN.labels(feat=str(feat)).set(_to_float((st or {}).get("mean"), 0.0))

    _set_onehot_reasons(rep.get("reasons") or [])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default=os.getenv("CONFIRMATIONS_COVERAGE_OUT_JSON", "/var/lib/trade/of_reports/confirmations_coverage_report.json"))
    ap.add_argument("--port", type=int, default=int(os.getenv("CONFIRMATIONS_COVERAGE_EXPORTER_PORT", "9628")))
    ap.add_argument("--stale-sec", type=int, default=int(os.getenv("CONFIRMATIONS_COVERAGE_REPORT_STALE_SEC", "129600")))  # 36h
    ap.add_argument("--poll-sec", type=int, default=int(os.getenv("CONFIRMATIONS_COVERAGE_EXPORTER_POLL_SEC", "10")))
    args = ap.parse_args()

    start_http_server(args.port)
    last_mtime = 0.0
    while True:
        _clear()
        rep = _load_report(args.report)
        if rep is None:
            REPORT_PRESENT.set(1.0 if os.path.exists(args.report) else 0.0)
            REPORT_PARSED_OK.set(0.0)
        else:
            update_metrics(rep, args.stale_sec)
        time.sleep(args.poll_sec)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
