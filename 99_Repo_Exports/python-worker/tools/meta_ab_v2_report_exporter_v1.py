from __future__ import annotations

"""Meta AB v2 — Prometheus exporter (v1)

Reads the JSON report produced by `tools.meta_ab_v2_nightly_job_v1` and exposes
stable Prometheus gauges.

Report contract (must stay backward compatible):
  ts_ms: int
  winner: champion|challenger|tie
  counts: {n_total:int, n_eligible:int}
  delta: {exp_r_per_candidate:float, tail_rate_per_candidate:float}
  ramp: {share_current:float, share_next:float, action:increase_share|decrease_share|hold}
  reason: str (optional; present => run_ok=0)

Env:
  META_AB_V2_REPORT_JSON            Path to report JSON (default /var/lib/trade/of_reports/meta_ab_v2_report.json)
  META_AB_V2_EXPORTER_PORT          HTTP port (default 9627)
  META_AB_V2_EXPORT_INTERVAL_SEC    Poll interval seconds (default 15)
  META_AB_V2_STALE_AFTER_H          Staleness threshold hours (default 30)
"""

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Any

from prometheus_client import Gauge, start_http_server

from utils.time_utils import get_ny_time_millis

REPORT_JSON = os.getenv("META_AB_V2_REPORT_JSON", "/var/lib/trade/of_reports/meta_ab_v2_report.json")
PORT = int(os.getenv("META_AB_V2_EXPORTER_PORT", "9627") or 9627)
INTERVAL_SEC = int(os.getenv("META_AB_V2_EXPORT_INTERVAL_SEC", "15") or 15)
STALE_AFTER_H = float(os.getenv("META_AB_V2_STALE_AFTER_H", "30") or 30.0)


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def _safe_int(x: Any) -> int | None:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


@dataclass
class MetaAbReport:
    parsed_ok: bool
    run_ok: bool
    ts_ms: int | None = None
    n_total: int | None = None
    n_eligible: int | None = None
    share_current: float | None = None
    share_next: float | None = None
    delta_exp_r_per_candidate: float | None = None
    delta_tail_rate_per_candidate: float | None = None
    p_min: float | None = None
    winner: str | None = None  # champion|challenger|tie
    action: str | None = None  # increase_share|decrease_share|hold
    report_age_sec: float | None = None
    stale_after_h: float = 30.0


def parse_report_obj(obj: dict[str, Any], file_mtime_ms: int | None = None, *, stale_after_h: float = STALE_AFTER_H) -> MetaAbReport:
    """Parse the report dict into a normalized struct. Never raises."""
    ts_ms = _safe_int(obj.get("ts_ms"))
    if ts_ms is None and file_mtime_ms is not None:
        ts_ms = file_mtime_ms
    age_sec = None
    if ts_ms is not None:
        age_sec = max(0.0, (_now_ms() - ts_ms) / 1000.0)

    counts = obj.get("counts") or {}
    delta = obj.get("delta") or {}
    ramp = obj.get("ramp") or {}

    winner = obj.get("winner")
    action = ramp.get("action")
    reason = obj.get("reason")

    cfg = obj.get("cfg") or {}
    p_min_raw = obj.get("p_min")
    if p_min_raw is None:
        p_min_raw = cfg.get("p_min")

    run_ok = reason is None

    return MetaAbReport(
        parsed_ok=True,
        run_ok=bool(run_ok),
        ts_ms=ts_ms,
        n_total=_safe_int(counts.get("n_total")),
        n_eligible=_safe_int(counts.get("n_eligible")),
        share_current=_safe_float(ramp.get("share_current")),
        share_next=_safe_float(ramp.get("share_next")),
        delta_exp_r_per_candidate=_safe_float(delta.get("exp_r_per_candidate")),
        delta_tail_rate_per_candidate=_safe_float(delta.get("tail_rate_per_candidate")),
        p_min=_safe_float(p_min_raw),
        winner=str(winner) if winner is not None else None,
        action=str(action) if action is not None else None,
        report_age_sec=age_sec,
        stale_after_h=float(stale_after_h),
    )


def read_report(path: str) -> tuple[MetaAbReport, str | None]:
    """Return (report, error). Never raises."""
    try:
        st = os.stat(path)
        file_mtime_ms = int(st.st_mtime * 1000)
    except Exception:
        file_mtime_ms = None

    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
        if not isinstance(obj, dict):
            return MetaAbReport(parsed_ok=False, run_ok=False, stale_after_h=STALE_AFTER_H), "report_not_dict"

        # Also store raw obj for policy parsing
        global _LAST_OBJ
        _LAST_OBJ = obj

        rep = parse_report_obj(obj, file_mtime_ms, stale_after_h=STALE_AFTER_H)
        return rep, None
    except FileNotFoundError:
        return MetaAbReport(parsed_ok=False, run_ok=False, stale_after_h=STALE_AFTER_H), "missing"
    except Exception as e:
        return MetaAbReport(parsed_ok=False, run_ok=False, stale_after_h=STALE_AFTER_H), f"parse_error:{type(e).__name__}"


# --- Prometheus metrics ---
G_PARSED_OK = Gauge("meta_ab_v2_report_parsed_ok", "1 if report file parsed OK")
G_RUN_OK = Gauge("meta_ab_v2_run_ok", "1 if report does NOT contain reason")
G_ERROR = Gauge("meta_ab_v2_report_error", "1 if last read had an error", ["error"])  # bounded

G_LAST_TS_MS = Gauge("meta_ab_v2_last_ts_ms", "report ts_ms")
G_AGE_SEC = Gauge("meta_ab_v2_report_age_sec", "seconds since report ts_ms")
G_STALE = Gauge("meta_ab_v2_report_stale", "1 if report_age_sec > stale_after_h", ["threshold_h"])  # bounded

G_N_TOTAL = Gauge("meta_ab_v2_n_total", "dataset rows total")
G_N_ELIGIBLE = Gauge("meta_ab_v2_n_eligible", "eligible rows")

G_SHARE_CUR = Gauge("meta_ab_v2_share_current", "current challenger share")
G_SHARE_NEXT = Gauge("meta_ab_v2_share_next", "recommended challenger share")
G_DELTA_EXP_R = Gauge("meta_ab_v2_delta_exp_r_per_candidate", "delta exp_r_per_candidate (chall - champ)")
G_DELTA_TAIL = Gauge("meta_ab_v2_delta_tail_rate_per_candidate", "delta tail_rate_per_candidate (chall - champ)")
G_P_MIN = Gauge("meta_ab_v2_p_min", "p_min used for eligibility")

G_WINNER = Gauge("meta_ab_v2_winner", "one-hot winner", ["winner"])  # champion|challenger|tie
G_ACTION = Gauge("meta_ab_v2_action", "one-hot action", ["action"])  # increase_share|decrease_share|hold

POLICY_BLOCKED = Gauge("meta_ab_v2_policy_blocked", "1 if policy blocked ramp/apply")
POLICY_ALLOW_APPLY = Gauge("meta_ab_v2_policy_allow_apply", "1 if policy allows apply")
POLICY_REASON = Gauge("meta_ab_v2_policy_blocked_reason", "one-hot policy blocked reason", ["reason"])
ACTION_RAW = Gauge("meta_ab_v2_action_raw", "one-hot raw action before policy", ["action"])
REPORT_SHARE_NEXT_RAW = Gauge("meta_ab_v2_share_next_raw", "raw recommended challenger share before policy")


def _set_one_hot(g: Gauge, candidates: tuple[str, ...], value: str | None) -> None:
    for c in candidates:
        g.labels(c).set(1.0 if value == c else 0.0)


def export_once(path: str) -> None:
    rep, err = read_report(path)

    # parsed/error
    G_PARSED_OK.set(1.0 if rep.parsed_ok else 0.0)
    G_RUN_OK.set(1.0 if rep.run_ok else 0.0)

    for e in ("missing", "report_not_dict", "parse_error"):
        G_ERROR.labels(e).set(0.0)
    if err is not None:
        if err.startswith("parse_error"):
            G_ERROR.labels("parse_error").set(1.0)
        elif err in ("missing", "report_not_dict"):
            G_ERROR.labels(err).set(1.0)

    # numeric fields
    if rep.ts_ms is not None:
        G_LAST_TS_MS.set(float(rep.ts_ms))
    if rep.report_age_sec is not None:
        G_AGE_SEC.set(float(rep.report_age_sec))
        th = f"{rep.stale_after_h:g}"
        G_STALE.labels(th).set(1.0 if rep.report_age_sec > rep.stale_after_h * 3600.0 else 0.0)

    if rep.n_total is not None:
        G_N_TOTAL.set(float(rep.n_total))
    if rep.n_eligible is not None:
        G_N_ELIGIBLE.set(float(rep.n_eligible))

    if rep.share_current is not None:
        G_SHARE_CUR.set(float(rep.share_current))
    if rep.share_next is not None:
        G_SHARE_NEXT.set(float(rep.share_next))
    if rep.delta_exp_r_per_candidate is not None:
        G_DELTA_EXP_R.set(float(rep.delta_exp_r_per_candidate))
    if rep.delta_tail_rate_per_candidate is not None:
        G_DELTA_TAIL.set(float(rep.delta_tail_rate_per_candidate))
    if rep.p_min is not None:
        G_P_MIN.set(float(rep.p_min))

    _set_one_hot(G_WINNER, ("champion", "challenger", "tie"), rep.winner)
    _set_one_hot(G_ACTION, ("increase_share", "decrease_share", "hold"), rep.action)

    # Policy section
    global _LAST_OBJ
    if "_LAST_OBJ" in globals() and _LAST_OBJ and getattr(rep, 'parsed_ok', False):
        pol = _LAST_OBJ.get("policy") or {}
        POLICY_BLOCKED.set(1.0 if bool(pol.get("blocked", False)) else 0.0)
        POLICY_ALLOW_APPLY.set(1.0 if bool(pol.get("allow_apply", False)) else 0.0)

        action_raw = (pol.get("action_raw", "hold") or "hold").strip().lower()
        if action_raw not in ("increase_share", "decrease_share", "hold"):
            action_raw = "hold"
        _set_one_hot(ACTION_RAW, ("increase_share", "decrease_share", "hold"), action_raw)

        ramp_sn = float((_LAST_OBJ.get("ramp") or {}).get("share_next", 0.0))
        REPORT_SHARE_NEXT_RAW.set(_safe_float(pol.get("share_next_raw")) or ramp_sn)

        reasons = pol.get("blocked_reasons") or []
        reason_set = set([str(x).strip().lower() for x in reasons if str(x).strip()])
        for r in ('share_nan', 'share_out_of_bounds', 'eval_reason_present', 'n_eligible_low', 'share_step_too_large', 'share_above_max_share', 'share_above_freeze_max', 'winner_not_challenger', 'delta_exp_r_low', 'tail_worse', 'ci_missing', 'ci_not_positive', 'decrease_disallowed', 'conf_coverage_missing', 'conf_coverage_stale', 'conf_coverage_low'):
            POLICY_REASON.labels(reason=r).set(1.0 if r in reason_set else 0.0)
    else:
        POLICY_BLOCKED.set(0.0)
        POLICY_ALLOW_APPLY.set(0.0)
        REPORT_SHARE_NEXT_RAW.set(0.0)
        _set_one_hot(ACTION_RAW, ("increase_share", "decrease_share", "hold"), "hold")
        for r in ('share_nan', 'share_out_of_bounds', 'eval_reason_present', 'n_eligible_low', 'share_step_too_large', 'share_above_max_share', 'share_above_freeze_max', 'winner_not_challenger', 'delta_exp_r_low', 'tail_worse', 'ci_missing', 'ci_not_positive', 'decrease_disallowed', 'conf_coverage_missing', 'conf_coverage_stale', 'conf_coverage_low'):
            POLICY_REASON.labels(reason=r).set(0.0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--report", type=str, default=REPORT_JSON)
    ap.add_argument("--interval", type=int, default=INTERVAL_SEC)
    args = ap.parse_args()

    start_http_server(args.port)
    print(f"meta_ab_v2_report_exporter_v1: serving :{args.port}, report={args.report}", flush=True)
    while True:
        export_once(args.report)
        time.sleep(max(1, int(args.interval)))


if __name__ == "__main__":
    main()
