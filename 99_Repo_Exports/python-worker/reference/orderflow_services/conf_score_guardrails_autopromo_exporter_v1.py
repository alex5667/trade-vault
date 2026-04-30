# -*- coding: utf-8 -*-
"""conf_score_guardrails_autopromo_exporter_v1.py

Prometheus exporter for the autopromo controller state.

This is intentionally small and robust:
- reads a JSON state file written by conf_score_guardrails_autopromo_controller_v1.py
- exports low-cardinality gauges for dashboards & alerts

Env:
  CONF_SCORE_GUARD_AUTOPROMO_STATE_PATH
  CONF_SCORE_GUARD_AUTOPROMO_EXPORTER_PORT
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from typing import Any, Dict, Optional

from prometheus_client import Gauge, start_http_server  # type: ignore


def _now_ms() -> int:
    return get_ny_time_millis()


def _load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return 1.0 if x else 0.0
        return float(x)
    except Exception:
        return None


PHASES = {
    "idle": 0
    "baseline": 1
    "canary_promote": 2
    "observing": 3
    "evaluate": 4
    "promote_full": 5
    "rollback": 6
    "promoted": 7
    "rolled_back": 8
    "blocked": 9
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-path", default=os.getenv("CONF_SCORE_GUARD_AUTOPROMO_STATE_PATH", "/tmp/conf_score_guard_autopromo_state.json"))
    ap.add_argument("--port", type=int, default=int(os.getenv("CONF_SCORE_GUARD_AUTOPROMO_EXPORTER_PORT", "9117")))
    ap.add_argument("--poll-sec", type=float, default=2.0)
    args = ap.parse_args()

    # core gauges
    g_phase = Gauge("conf_score_guard_autopromo_phase", "Autopromo phase code (see runbook)", ["phase"])
    g_last_ok = Gauge("conf_score_guard_autopromo_last_eval_ok", "Last canary eval verdict (1 ok, 0 fail)")
    g_blocked = Gauge("conf_score_guard_autopromo_blocked", "Autopromo blocked state (1 blocked)")
    g_state_age = Gauge("conf_score_guard_autopromo_state_age_sec", "Age of autopromo state file in seconds")
    g_observe_rem = Gauge("conf_score_guard_autopromo_observing_remaining_sec", "Remaining observe window in seconds (if observing)")
    g_candidate = Gauge("conf_score_guard_autopromo_candidate_active", "Active candidate marker", ["version"])
    g_delta_ece = Gauge("conf_score_guard_autopromo_delta_ece_cal", "Delta ece_cal vs baseline (canary eval)")
    g_delta_brier = Gauge("conf_score_guard_autopromo_delta_brier_cal", "Delta brier_cal vs baseline (canary eval)")
    g_arm_delta_ece = Gauge("conf_score_guard_autopromo_arm_delta_ece_cal", "Paired delta ece_cal (challenger - champion)")
    g_arm_delta_brier = Gauge("conf_score_guard_autopromo_arm_delta_brier_cal", "Paired delta brier_cal (challenger - champion)")
    g_cohort_wmean_ece = Gauge("conf_score_guard_autopromo_cohort_delta_ece_cal_wmean", "Matched-cohort weighted mean delta ece_cal")
    g_cohort_wmean_brier = Gauge("conf_score_guard_autopromo_cohort_delta_brier_cal_wmean", "Matched-cohort weighted mean delta brier_cal")
    g_cohort_max_ece = Gauge("conf_score_guard_autopromo_cohort_delta_ece_cal_max", "Matched-cohort worst delta ece_cal")
    g_cohort_max_brier = Gauge("conf_score_guard_autopromo_cohort_delta_brier_cal_max", "Matched-cohort worst delta brier_cal")

    start_http_server(args.port)

    last_mtime = None
    while True:
        st = _load_json(args.state_path)
        # phase
        phase = str(st.get("phase") or "idle")
        # publish one-hot phase gauges
        for p in PHASES.keys():
            g_phase.labels(phase=p).set(1.0 if p == phase else 0.0)

        g_blocked.set(1.0 if phase == "blocked" else 0.0)

        # state age
        try:
            mtime = os.path.getmtime(args.state_path)
            g_state_age.set(max(0.0, time.time() - float(mtime)))
            last_mtime = mtime
        except Exception:
            g_state_age.set(float("nan"))

        # observing remaining
        rem = _safe_float(st.get("observing_remaining_sec"))
        if rem is None:
            rem = 0.0
        g_observe_rem.set(float(rem))

        # candidate version (low churn)
        cand = st.get("candidate") or {}
        ver = str(cand.get("version") or "none")
        # wipe previous labels if version changes (simple approach)
        g_candidate.labels(version=ver).set(1.0)

        # last eval
        cycle = st.get("cycle") or {}
        last_eval = cycle.get("last_eval") or {}
        ok = last_eval.get("ok")
        g_last_ok.set(1.0 if ok is True else 0.0 if ok is False else float("nan"))

        delta = last_eval.get("delta") or {}
        de = _safe_float(delta.get("ece_cal"))
        db = _safe_float(delta.get("brier_cal"))
        g_delta_ece.set(de if de is not None else float("nan"))
        g_delta_brier.set(db if db is not None else float("nan"))

        ade = _safe_float(delta.get("arm_delta_ece_cal"))
        adb = _safe_float(delta.get("arm_delta_brier_cal"))
        cwe = _safe_float(delta.get("cohort_delta_ece_cal_wmean"))
        cwb = _safe_float(delta.get("cohort_delta_brier_cal_wmean"))
        cme = _safe_float(delta.get("cohort_delta_ece_cal_max"))
        cmb = _safe_float(delta.get("cohort_delta_brier_cal_max"))
        g_arm_delta_ece.set(ade if ade is not None else float("nan"))
        g_arm_delta_brier.set(adb if adb is not None else float("nan"))
        g_cohort_wmean_ece.set(cwe if cwe is not None else float("nan"))
        g_cohort_wmean_brier.set(cwb if cwb is not None else float("nan"))
        g_cohort_max_ece.set(cme if cme is not None else float("nan"))
        g_cohort_max_brier.set(cmb if cmb is not None else float("nan"))

        time.sleep(float(args.poll_sec))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
