#!/usr/bin/env python3
"""conf_cal_live_status_exporter_v1.py

Prometheus exporter for confidence calibration live-health status.

Reads a JSON status file produced by ml_analysis.tools.confidence_cal_live_health_loop_v1
and exports gauges/counters so degradation/rollbacks are observable.

Robust to multiple schema variants and filename variants.

ENV
  CONF_CAL_LIVE_REPORTS_DIR (default /var/lib/trade/of_reports/out/conf_cal/live)
  CONF_CAL_LIVE_STATUS_PATH (optional explicit)
  CONF_CAL_LIVE_EXPORTER_STATE_PATH (default ${REPORTS_DIR}/conf_cal_live_exporter_state.json)
  CONF_CAL_LIVE_EXPORTER_PORT (default 9134)
  CONF_CAL_LIVE_EXPORTER_REFRESH_SEC (default 5)

Requested gauges:
  live_ece_raw, live_ece_cal, live_brier_raw, live_brier_cal, bad_streak, rollback_total
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import signal
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from prometheus_client import Counter, Gauge, start_http_server  # type: ignore


def _now_ms() -> int:
    return get_ny_time_millis()


def _as_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or isinstance(x, bool):
            return int(default)
        if isinstance(x, (int, float)):
            return int(x)
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "ignore")
        s = str(x).strip()
        return int(float(s)) if s else int(default)
    except Exception:
        return int(default)


def _as_float(x: Any, default: float = float("nan")) -> float:
    try:
        if x is None or isinstance(x, bool):
            return float(default)
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "ignore")
        s = str(x).strip()
        return float(s) if s else float(default)
    except Exception:
        return float(default)


def _load_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _write_json_atomic(path: str, d: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _nested(d: Dict[str, Any], *keys: str) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _probe_status_path(reports_dir: str) -> Tuple[str, str]:
    env_path = os.getenv("CONF_CAL_LIVE_STATUS_PATH", "").strip()
    if env_path:
        return env_path, "env"
    candidates = [
        "conf_cal_live_status.json",
        "confidence_calibration_live_status.json",
        "confidence_cal_live_status.json",
        "live_status.json",
    ]
    for name in candidates:
        p = os.path.join(reports_dir, name)
        if os.path.isfile(p):
            return p, f"found:{name}"
    return os.path.join(reports_dir, candidates[0]), "default"


# Requested gauges
live_ece_raw = Gauge("live_ece_raw", "live ECE on raw confidence")
live_ece_cal = Gauge("live_ece_cal", "live ECE on calibrated confidence")
live_brier_raw = Gauge("live_brier_raw", "live Brier on raw confidence")
live_brier_cal = Gauge("live_brier_cal", "live Brier on calibrated confidence")
bad_streak = Gauge("bad_streak", "consecutive degradation streak counter")
rollback_total = Gauge("rollback_total", "total rollbacks observed by exporter (persisted)")

# Ops/diagnostics
conf_cal_live_exporter_up = Gauge("conf_cal_live_exporter_up", "exporter loop running (1/0)")
conf_cal_live_status_ts_ms = Gauge("conf_cal_live_status_ts_ms", "status ts_ms from live loop")
conf_cal_live_status_age_sec = Gauge("conf_cal_live_status_age_sec", "age (now - status.ts_ms) seconds")
conf_cal_live_ok = Gauge("conf_cal_live_ok", "status ok flag (1/0)")
conf_cal_live_degrade = Gauge("conf_cal_live_degrade", "degrade decision (1/0)")
conf_cal_live_rows = Gauge("conf_cal_live_rows", "rows in live eval window (raw)")
conf_cal_live_rows_cal = Gauge("conf_cal_live_rows_cal", "rows with calibrated values")
conf_cal_live_last_rollback_ts_ms = Gauge("conf_cal_live_last_rollback_ts_ms", "last rollback timestamp")
conf_cal_live_exporter_read_ok = Gauge("conf_cal_live_exporter_read_ok", "exporter read succeeded (1/0)")
conf_cal_live_skip = Gauge("conf_cal_live_skip", "skip_reason present (1/0)")

conf_cal_live_exporter_read_errors_total = Counter(
    "conf_cal_live_exporter_read_errors_total", "exporter read errors"
)
conf_cal_live_exporter_parse_errors_total = Counter(
    "conf_cal_live_exporter_parse_errors_total", "exporter parse errors"
)
conf_cal_live_rollback_events_total = Counter(
    "conf_cal_live_rollback_events_total", "rollback events observed"
)
conf_cal_live_degrade_events_total = Counter("conf_cal_live_degrade_events_total", "degrade decisions observed")
conf_cal_live_degrade_reason_total = Counter(
    "conf_cal_live_degrade_reason_total", "degrade reason breakdown", ["reason"]
)
conf_cal_live_skip_reason_total = Counter(
    "conf_cal_live_skip_reason_total", "skip reason breakdown", ["reason"]
)


# Proof-state (cal_after_proof) observability
conf_cal_proof_read_ok = Gauge("conf_cal_proof_read_ok", "proof state read succeeded (1/0)")
conf_cal_proof_ts_sec = Gauge("conf_cal_proof_ts_sec", "proof controller update ts (seconds)")
conf_cal_proof_age_sec = Gauge("conf_cal_proof_age_sec", "age (now - proof.ts) seconds")
conf_cal_proof_evidence_ts_sec = Gauge("conf_cal_proof_evidence_ts_sec", "evidence ts used for freshness (seconds)")
conf_cal_proof_evidence_age_sec = Gauge("conf_cal_proof_evidence_age_sec", "age (now - proof.evidence_ts) seconds")
conf_cal_proof_valid = Gauge("conf_cal_proof_valid", "proof valid flag (1/0)")
conf_cal_proof_canary_share = Gauge("conf_cal_proof_canary_share", "canary share from proof controller (0..1)")
conf_cal_proof_status_age_sec = Gauge("conf_cal_proof_status_age_sec", "status age seconds reported in proof.source")

conf_cal_proof_read_errors_total = Counter("conf_cal_proof_read_errors_total", "proof state read errors")
conf_cal_proof_parse_errors_total = Counter("conf_cal_proof_parse_errors_total", "proof state parse/shape errors")


@dataclass
class _State:
    rollback_total: int = 0
    last_rb_event_ts_ms: int = 0


class Exporter:
    def __init__(self, reports_dir: str) -> None:
        self.reports_dir = reports_dir
        self.status_path, self.status_path_reason = _probe_status_path(reports_dir)
        self.state_path = os.getenv(
            "CONF_CAL_LIVE_EXPORTER_STATE_PATH",
            os.path.join(str(reports_dir), "conf_cal_live_exporter_state.json"),
        )
        self.proof_path = os.getenv(
            "CONF_CAL_PROOF_STATE_PATH",
            "/var/lib/trade/of_calibrators/conf_cal_proof_state.json",
        )
        self.running = True
        self.state = _State()
        self._last_state_write_ms = 0

        self._load_state()

        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, signum, frame) -> None:
        self.running = False

    def _load_state(self) -> None:
        st = _load_json(self.state_path)
        if isinstance(st, dict):
            self.state.rollback_total = _as_int(st.get("rollback_total", 0), 0)
            self.state.last_rb_event_ts_ms = _as_int(st.get("last_rb_event_ts_ms", 0), 0)
        rollback_total.set(float(self.state.rollback_total))

    def _save_state_if_needed(self, now_ms: int, force: bool = False) -> None:
        if not force and (now_ms - self._last_state_write_ms) < 5000:
            return
        self._last_state_write_ms = now_ms
        try:
            _write_json_atomic(
                self.state_path,
                {
                    "ts_ms": int(now_ms),
                    "rollback_total": int(self.state.rollback_total),
                    "last_rb_event_ts_ms": int(self.state.last_rb_event_ts_ms),
                },
            )
        except Exception:
            pass

    def _extract_metrics(self, status: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        raw = status.get("raw") if isinstance(status.get("raw"), dict) else None
        cal = status.get("cal") if isinstance(status.get("cal"), dict) else None
        if raw is None and isinstance(status.get("metrics_raw_v1"), dict):
            raw = status.get("metrics_raw_v1")
        if cal is None and isinstance(status.get("metrics_cal_v1"), dict):
            cal = status.get("metrics_cal_v1")
        if raw is None and isinstance(status.get("metrics_raw"), dict):
            raw = status.get("metrics_raw")
        if cal is None and isinstance(status.get("metrics_cal"), dict):
            cal = status.get("metrics_cal")
        return (raw or {}), (cal or {})

    def _extract_degrade(self, status: Dict[str, Any]) -> bool:
        if "degrade" in status:
            return bool(status.get("degrade", False))
        guard = status.get("guard") if isinstance(status.get("guard"), dict) else {}
        if isinstance(guard, dict) and "fail" in guard:
            return bool(guard.get("fail", False))
        if status.get("guard_passed") is False:
            return True
        return False

    def _extract_rollback_event_ts(self, status: Dict[str, Any]) -> Tuple[bool, int]:
        rb = status.get("rollback") if isinstance(status.get("rollback"), dict) else {}
        performed = bool(rb.get("performed", False)) if isinstance(rb, dict) else False
        ev_ts = _as_int(status.get("ts_ms", 0), 0)
        return performed, ev_ts

    def _step_proof(self, now_ms: int) -> None:
        # Update proof-state metrics independently of live status read.
        # Proof is produced by conf_cal_proof_state_controller_v1.py
        try:
            if not self.proof_path or not os.path.isfile(self.proof_path):
                conf_cal_proof_read_ok.set(0.0)
                # Do not increment error counters on "missing" to avoid noise.
                return

            proof = _load_json(self.proof_path)
            if not isinstance(proof, dict):
                conf_cal_proof_read_ok.set(0.0)
                conf_cal_proof_read_errors_total.inc()
                return

            now_sec = int(now_ms // 1000)
            ts_sec = _as_int(proof.get("ts", 0), 0)
            evidence_ts_sec = _as_int(proof.get("evidence_ts", 0), 0)

            conf_cal_proof_read_ok.set(1.0)
            conf_cal_proof_ts_sec.set(float(ts_sec))
            conf_cal_proof_evidence_ts_sec.set(float(evidence_ts_sec))

            age = float(max(0, now_sec - ts_sec)) if ts_sec > 0 else float("inf")
            e_age = float(max(0, now_sec - evidence_ts_sec)) if evidence_ts_sec > 0 else float("inf")
            conf_cal_proof_age_sec.set(float(age))
            conf_cal_proof_evidence_age_sec.set(float(e_age))

            conf_cal_proof_valid.set(1.0 if bool(proof.get("valid", False)) else 0.0)
            conf_cal_proof_canary_share.set(float(_as_float(proof.get("canary_share", 0.0), 0.0)))

            src = proof.get("source") if isinstance(proof.get("source"), dict) else {}
            if isinstance(src, dict) and "status_age_sec" in src:
                conf_cal_proof_status_age_sec.set(float(_as_float(src.get("status_age_sec"), float("nan"))))
        except Exception:
            try:
                conf_cal_proof_parse_errors_total.inc()
            except Exception:
                pass

    def step(self) -> None:
        now_ms = _now_ms()

        if not os.path.isfile(self.status_path):
            self.status_path, self.status_path_reason = _probe_status_path(self.reports_dir)

        # proof metrics are updated even if live status cannot be read
        self._step_proof(now_ms)

        status = _load_json(self.status_path)
        if not isinstance(status, dict):
            conf_cal_live_exporter_read_ok.set(0.0)
            try:
                conf_cal_live_exporter_read_errors_total.inc()
            except Exception:
                pass
            return

        conf_cal_live_exporter_read_ok.set(1.0)

        try:
            ts_ms = _as_int(status.get("ts_ms", 0), 0)
            if ts_ms > 0:
                age = max(0.0, (now_ms - ts_ms) / 1000.0)
            else:
                try:
                    age = max(0.0, time.time() - os.path.getmtime(self.status_path))
                except Exception:
                    age = float("inf")

            conf_cal_live_status_ts_ms.set(float(ts_ms))
            conf_cal_live_status_age_sec.set(float(age))

            conf_cal_live_ok.set(1.0 if bool(status.get("ok", False)) else 0.0)
            conf_cal_live_rows.set(float(_as_int(status.get("rows_raw", status.get("rows", 0)), 0)))
            conf_cal_live_rows_cal.set(float(_as_int(status.get("rows_cal", 0), 0)))

            skip_reason = str(status.get("skip_reason", "") or "").strip()
            conf_cal_live_skip.set(1.0 if skip_reason else 0.0)
            if skip_reason:
                conf_cal_live_skip_reason_total.labels(reason=skip_reason).inc()

            degrade = self._extract_degrade(status)
            conf_cal_live_degrade.set(1.0 if degrade else 0.0)
            if degrade:
                conf_cal_live_degrade_events_total.inc()
                reason = str(status.get("degrade_reason") or status.get("reason") or "unknown")
                conf_cal_live_degrade_reason_total.labels(reason=reason).inc()

            bs = _as_int(status.get("bad_streak", 0), 0)
            bad_streak.set(float(bs))

            conf_cal_live_last_rollback_ts_ms.set(float(_as_int(status.get("last_rollback_ts_ms", 0), 0)))

            raw, cal = self._extract_metrics(status)
            live_ece_raw.set(_as_float(_nested(raw, "ece")))
            live_ece_cal.set(_as_float(_nested(cal, "ece")))
            live_brier_raw.set(_as_float(_nested(raw, "brier")))
            live_brier_cal.set(_as_float(_nested(cal, "brier")))

            performed, ev_ts = self._extract_rollback_event_ts(status)
            if performed and ev_ts > 0 and ev_ts != self.state.last_rb_event_ts_ms:
                self.state.last_rb_event_ts_ms = ev_ts
                self.state.rollback_total += 1
                rollback_total.set(float(self.state.rollback_total))
                conf_cal_live_rollback_events_total.inc()
                self._save_state_if_needed(now_ms, force=True)
            else:
                self._save_state_if_needed(now_ms, force=False)

        except Exception:
            try:
                conf_cal_live_exporter_parse_errors_total.inc()
            except Exception:
                pass


def main() -> int:
    reports_dir = os.getenv("CONF_CAL_LIVE_REPORTS_DIR", "/var/lib/trade/of_reports/out/conf_cal/live")
    port = int(os.getenv("CONF_CAL_LIVE_EXPORTER_PORT", "9134") or 9134)
    refresh_sec = float(os.getenv("CONF_CAL_LIVE_EXPORTER_REFRESH_SEC", "5") or 5)

    exp = Exporter(reports_dir=str(reports_dir))
    start_http_server(port)
    conf_cal_live_exporter_up.set(1.0)
    print(json.dumps({"ok": True, "port": port, "reports_dir": reports_dir, "status_path": exp.status_path}, ensure_ascii=False))

    while exp.running:
        exp.step()
        time.sleep(max(0.5, float(refresh_sec)))

    conf_cal_live_exporter_up.set(0.0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
