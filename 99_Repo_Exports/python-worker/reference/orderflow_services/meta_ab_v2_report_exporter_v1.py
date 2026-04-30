"""
Prometheus exporter for the latest Meta AB-winner v2 report JSON.

Exports only low-cardinality, alert-friendly gauges.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, start_http_server


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except Exception:
        return default


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{ts}] meta_ab_v2_exporter: {msg}", flush=True)


REPORT_PRESENT = Gauge("meta_ab_v2_report_present", "1 if report file parsed OK")
REPORT_PARSE_ERRORS = Counter("meta_ab_v2_report_parse_errors_total", "JSON parse/read errors")

REPORT_LAST_TS_MS = Gauge("meta_ab_v2_last_ts_ms", "report ts_ms")
REPORT_N_TOTAL = Gauge("meta_ab_v2_n_total", "dataset rows total")
REPORT_N_ELIGIBLE = Gauge("meta_ab_v2_n_eligible", "eligible rows")
REPORT_P_MIN = Gauge("meta_ab_v2_p_min", "p_min threshold")

REPORT_SHARE_CUR = Gauge("meta_ab_v2_share_current", "current challenger share")
REPORT_SHARE_NEXT = Gauge("meta_ab_v2_share_next", "recommended challenger share")

REPORT_DELTA_EXP_R = Gauge("meta_ab_v2_delta_exp_r_per_candidate", "delta exp_r_per_candidate (chall - champ)")
REPORT_DELTA_TAIL = Gauge("meta_ab_v2_delta_tail_rate_per_candidate", "delta tail_rate_per_candidate (chall - champ)")

WINNER = Gauge("meta_ab_v2_winner", "one-hot winner", ["winner"])  # champion|challenger|tie
ACTION = Gauge("meta_ab_v2_action", "one-hot action", ["action"])  # increase_share|decrease_share|hold


def _to_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _set_onehot(g: Gauge, label_name: str, keys: tuple[str, ...], value: str) -> None:
    v = (value or "").strip().lower()
    for k in keys:
        g.labels(**{label_name: k}).set(1.0 if k == v else 0.0)


def _apply_report(rep: dict) -> None:
    REPORT_PRESENT.set(1.0)
    REPORT_LAST_TS_MS.set(_to_float(rep.get("ts_ms", 0.0)))

    counts = rep.get("counts") or {}
    REPORT_N_TOTAL.set(_to_float(counts.get("n_total", 0)))
    REPORT_N_ELIGIBLE.set(_to_float(counts.get("n_eligible", 0)))

    cfg = rep.get("config") or {}
    REPORT_P_MIN.set(_to_float(cfg.get("p_min", rep.get("p_min", 0.0))))

    ramp = rep.get("ramp") or {}
    REPORT_SHARE_CUR.set(_to_float(ramp.get("share_current", 0.0)))
    REPORT_SHARE_NEXT.set(_to_float(ramp.get("share_next", 0.0)))

    delta = rep.get("delta") or {}
    REPORT_DELTA_EXP_R.set(_to_float(delta.get("exp_r_per_candidate", 0.0)))
    REPORT_DELTA_TAIL.set(_to_float(delta.get("tail_rate_per_candidate", 0.0)))

    winner = str(rep.get("winner", "tie") or "tie").strip().lower()
    if winner not in ("champion", "challenger", "tie"):
        winner = "tie"
    _set_onehot(WINNER, "winner", ("champion", "challenger", "tie"), winner)

    action = str((rep.get("ramp") or {}).get("action", "hold") or "hold").strip().lower()
    if action not in ("increase_share", "decrease_share", "hold"):
        action = "hold"
    _set_onehot(ACTION, "action", ("increase_share", "decrease_share", "hold"), action)


def _clear() -> None:
    REPORT_PRESENT.set(0.0)
    REPORT_LAST_TS_MS.set(0.0)
    REPORT_N_TOTAL.set(0.0)
    REPORT_N_ELIGIBLE.set(0.0)
    REPORT_P_MIN.set(0.0)
    REPORT_SHARE_CUR.set(0.0)
    REPORT_SHARE_NEXT.set(0.0)
    REPORT_DELTA_EXP_R.set(0.0)
    REPORT_DELTA_TAIL.set(0.0)
    _set_onehot(WINNER, "winner", ("champion", "challenger", "tie"), "tie")
    _set_onehot(ACTION, "action", ("increase_share", "decrease_share", "hold"), "hold")


def main() -> int:
    port = _env_int("META_AB_V2_EXPORTER_PORT", 9634)
    report_path = os.getenv(
        "META_AB_V2_OUT_JSON"
        "/var/lib/trade/of_reports/out/meta_ab_v2/ab_v2_report.json"
    )
    poll_s = _env_int("META_AB_V2_EXPORTER_POLL_S", 10)

    start_http_server(port)
    _log(f"listening :{port}; report={report_path}; poll={poll_s}s")

    _clear()
    last_mtime = 0.0

    while True:
        try:
            st = os.stat(report_path)
            if st.st_mtime != last_mtime:
                last_mtime = st.st_mtime
                with open(report_path, "r", encoding="utf-8") as f:
                    rep = json.load(f)
                _apply_report(rep)
                _log("report reloaded")
        except FileNotFoundError:
            _clear()
        except Exception as e:
            REPORT_PARSE_ERRORS.inc()
            _log(f"ERROR: {type(e).__name__}: {e}")
        time.sleep(max(1, poll_s))


if __name__ == "__main__":
    raise SystemExit(main())
