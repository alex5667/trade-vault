#!/usr/bin/env python3
from __future__ import annotations
"""conf_cal_proof_state_controller_v1.py

Proof-state controller for calibrated confidence gating (P75+).

Produces proof JSON consumed by services/orderflow_strategy.py when
confidence_cal_gating_mode=cal_after_proof.

Key properties:
- Anti-flap: N GOOD runs -> valid=true; M BAD runs -> valid=false
- Canary ramp: canary_share grows stepwise when valid=true
- evidence_ts is used for freshness (controller ts does NOT refresh evidence)

ENV:
  CONF_CAL_LIVE_REPORTS_DIR               default /var/lib/trade/of_reports/out/confidence_cal_live
  CONF_CAL_LIVE_STATUS_PATH              optional explicit
  CONF_CAL_PROOF_STATE_PATH              default /tmp/conf_cal_proof_state.json
  CONF_CAL_PROOF_CONTROLLER_STATE_PATH   default <dir(proof)>/conf_cal_proof_controller_state.json
  CONF_CAL_PROOF_MIN_GOOD_RUNS           default 2
  CONF_CAL_PROOF_MIN_BAD_RUNS            default 2
  CONF_CAL_PROOF_MAX_LIVE_STATUS_AGE_SEC default 21600
  CONF_CAL_PROOF_CANARY_ENABLE           default 1
  CONF_CAL_PROOF_CANARY_START            default 0.10
  CONF_CAL_PROOF_CANARY_STEP             default 0.10
  CONF_CAL_PROOF_CANARY_MAX              default 1.00
  CONF_CAL_PROOF_CANARY_BUMP_MIN_SEC     default 1800
"""

from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

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
        "confidence_calibration_live_status.json",
        "conf_cal_live_status.json",
        "confidence_cal_live_status.json",
        "live_status.json"]
    for name in candidates:
        p = os.path.join(reports_dir, name)
        if os.path.isfile(p):
            return p, f"found:{name}"
    return os.path.join(reports_dir, candidates[0]), "default"

def _clamp01(x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    return float(x)

@dataclass
class _CtrlState:
    good_streak: int = 0
    bad_streak: int = 0
    valid: bool = False
    evidence_ts: int = 0
    canary_share: float = 0.0
    ramp_started_ts: int = 0
    last_bump_ts: int = 0

class ProofStateController:
    def __init__(self, *, reports_dir: str, proof_path: str, state_path: str,
                 min_good_runs: int, min_bad_runs: int, max_live_age_sec: int,
                 canary_enable: bool, canary_start: float, canary_step: float,
                 canary_max: float, canary_bump_min_sec: int) -> None:
        self.reports_dir = str(reports_dir)
        self.proof_path = str(proof_path)
        self.state_path = str(state_path)
        self.status_path, self.status_path_reason = _probe_status_path(self.reports_dir)
        self.min_good_runs = max(1, int(min_good_runs))
        self.min_bad_runs = max(1, int(min_bad_runs))
        self.max_live_age_sec = max(60, int(max_live_age_sec))
        self.canary_enable = bool(canary_enable)
        self.canary_start = _clamp01(float(canary_start))
        self.canary_step = _clamp01(float(canary_step))
        self.canary_max = _clamp01(float(canary_max))
        self.canary_bump_min_sec = max(60, int(canary_bump_min_sec))
        self.state = _CtrlState()
        self._load_state()

    def _load_state(self) -> None:
        st = _load_json(self.state_path)
        if not isinstance(st, dict):
            return
        self.state.good_streak = _as_int(st.get("good_streak", 0), 0)
        self.state.bad_streak = _as_int(st.get("bad_streak", 0), 0)
        self.state.valid = bool(st.get("valid", False))
        self.state.evidence_ts = _as_int(st.get("evidence_ts", 0), 0)
        self.state.canary_share = float(_as_float(st.get("canary_share", 0.0), 0.0))
        self.state.ramp_started_ts = _as_int(st.get("ramp_started_ts", 0), 0)
        self.state.last_bump_ts = _as_int(st.get("last_bump_ts", 0), 0)

    def _save_state(self, now_sec: int) -> None:
        _write_json_atomic(self.state_path, {
            "ts": int(now_sec),
            "good_streak": int(self.state.good_streak),
            "bad_streak": int(self.state.bad_streak),
            "valid": bool(self.state.valid),
            "evidence_ts": int(self.state.evidence_ts),
            "canary_share": float(self.state.canary_share),
            "ramp_started_ts": int(self.state.ramp_started_ts),
            "last_bump_ts": int(self.state.last_bump_ts),
            "status_path": str(self.status_path),
            "status_path_reason": str(self.status_path_reason),
        })

    def _extract_guard_passed(self, status: Dict[str, Any]) -> Optional[bool]:
        gp = status.get("guard_passed", None)
        if isinstance(gp, bool):
            return gp
        gp2 = _nested(status, "guard", "passed")
        if isinstance(gp2, bool):
            return gp2
        gf = _nested(status, "guard", "fail")
        if isinstance(gf, bool):
            return not gf
        return None

    def _extract_guard_fail(self, status: Dict[str, Any]) -> Optional[bool]:
        gf = _nested(status, "guard", "fail")
        if isinstance(gf, bool):
            return gf
        gp = self._extract_guard_passed(status)
        if gp is True:
            return False
        if gp is False:
            return True
        return None

    def _status_age_sec(self, status: Dict[str, Any], now_ms: int) -> float:
        ts_ms = _as_int(status.get("ts_ms", 0), 0)
        if ts_ms > 0:
            return max(0.0, float(now_ms - ts_ms) / 1000.0)
        try:
            return max(0.0, time.time() - os.path.getmtime(self.status_path))
        except Exception:
            return float("inf")

    def _maybe_bump_canary(self, now_sec: int) -> None:
        if not self.canary_enable:
            self.state.canary_share = 1.0
            return
        if self.state.canary_share <= 0.0:
            self.state.canary_share = float(self.canary_start)
            self.state.ramp_started_ts = int(now_sec)
            self.state.last_bump_ts = int(now_sec)
            return
        if self.state.canary_share >= self.canary_max:
            self.state.canary_share = float(self.canary_max)
            return
        if (now_sec - int(self.state.last_bump_ts)) < int(self.canary_bump_min_sec):
            return
        nxt = min(float(self.canary_max), float(self.state.canary_share) + float(self.canary_step))
        self.state.canary_share = float(nxt)
        self.state.last_bump_ts = int(now_sec)

    def step(self, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
        now_ms = int(now_ms) if now_ms is not None else _now_ms()
        now_sec = int(now_ms // 1000)
        if not os.path.isfile(self.status_path):
            self.status_path, self.status_path_reason = _probe_status_path(self.reports_dir)
        status = _load_json(self.status_path)
        if not isinstance(status, dict):
            proof = self._emit_proof(now_sec, reason="status_unreadable", status=None)
            self._save_state(now_sec)
            return proof
        age_sec = self._status_age_sec(status, now_ms)
        if age_sec > float(self.max_live_age_sec):
            proof = self._emit_proof(now_sec, reason=f"status_stale>{int(self.max_live_age_sec)}s",
                                     status=status, status_age_sec=age_sec)
            self._save_state(now_sec)
            return proof
        skipped = bool(status.get("skipped", False))
        skip_reason = str(status.get("skip_reason", "") or "").strip()
        guard_fail = self._extract_guard_fail(status)
        guard_passed = self._extract_guard_passed(status)
        outcome = "neutral"
        if skipped:
            outcome = "neutral"
        elif guard_fail is True or guard_passed is False:
            outcome = "bad"
        elif guard_fail is False and guard_passed is True:
            outcome = "good"
        prev_valid = bool(self.state.valid)
        if outcome == "good":
            self.state.good_streak += 1
            self.state.bad_streak = 0
            if self.state.good_streak >= self.min_good_runs:
                if not prev_valid:
                    self.state.valid = True
                    self.state.evidence_ts = int(now_sec)
                    self.state.canary_share = 0.0
                    self.state.ramp_started_ts = int(now_sec)
                    self.state.last_bump_ts = int(now_sec)
                self._maybe_bump_canary(now_sec)
        elif outcome == "bad":
            self.state.bad_streak += 1
            self.state.good_streak = 0
            if self.state.bad_streak >= self.min_bad_runs:
                self.state.valid = False
                self.state.canary_share = 0.0
                self.state.ramp_started_ts = 0
                self.state.last_bump_ts = 0
        reason = outcome
        if outcome == "neutral" and skipped:
            reason = f"neutral_skip:{skip_reason or 'skipped'}"
        elif outcome == "neutral" and (guard_passed is None and guard_fail is None):
            reason = "neutral_unknown_guard"
        elif outcome == "bad":
            g = status.get("guard") if isinstance(status.get("guard"), dict) else {}
            reasons = g.get("reasons") if isinstance(g, dict) else None
            if isinstance(reasons, list) and reasons:
                reason = "bad:" + ",".join(str(x) for x in reasons[:3])
        proof = self._emit_proof(now_sec, reason=reason, status=status, status_age_sec=age_sec,
                                 guard_passed=guard_passed, skipped=skipped, skip_reason=skip_reason)
        self._save_state(now_sec),
        return proof,

    def _emit_proof(self, now_sec: int, *, reason: str, status: Optional[Dict[str, Any]],
                    status_age_sec: Optional[float] = None, guard_passed: Optional[bool] = None,
                    skipped: bool = False, skip_reason: str = "") -> Dict[str, Any]:
        status_ts_ms = _as_int(status.get("ts_ms"), 0) if isinstance(status, dict) else 0,
        if status_age_sec is None:
            try:
                status_age_sec = float(self._status_age_sec(status or {}, int(now_sec * 1000))) if isinstance(status, dict) else float("inf"),
            except Exception:
                status_age_sec = float("inf"),
        proof: Dict[str, Any] = {
            "ts": int(now_sec),
            "evidence_ts": int(self.state.evidence_ts),
            "valid": bool(self.state.valid),
            "reason": str(reason),
            "canary_share": float(_clamp01(float(self.state.canary_share))) if self.state.valid else 0.0,
            "source": {
                "status_path": str(self.status_path),
                "status_path_reason": str(self.status_path_reason),
                "status_ts_ms": int(status_ts_ms),
                "status_age_sec": float(status_age_sec),
            },
            "streaks": {"good": int(self.state.good_streak), "bad": int(self.state.bad_streak)},
            "last": {"guard_passed": guard_passed, "skipped": bool(skipped), "skip_reason": str(skip_reason)},
        }
        _write_json_atomic(self.proof_path, proof)
        return proof

def main(argv: Optional[list[str]] = None) -> int:
    reports_dir = os.getenv("CONF_CAL_LIVE_REPORTS_DIR", "/var/lib/trade/of_reports/out/confidence_cal_live")
    proof_path = os.getenv("CONF_CAL_PROOF_STATE_PATH", "/tmp/conf_cal_proof_state.json")
    default_state_path = os.path.join(os.path.dirname(os.path.abspath(proof_path)) or ".", "conf_cal_proof_controller_state.json")
    state_path = os.getenv("CONF_CAL_PROOF_CONTROLLER_STATE_PATH", default_state_path)
    min_good = int(os.getenv("CONF_CAL_PROOF_MIN_GOOD_RUNS", "2") or 2)
    min_bad = int(os.getenv("CONF_CAL_PROOF_MIN_BAD_RUNS", "2") or 2)
    max_live_age_sec = int(os.getenv("CONF_CAL_PROOF_MAX_LIVE_STATUS_AGE_SEC", "21600") or 21600)
    canary_enable = os.getenv("CONF_CAL_PROOF_CANARY_ENABLE", "1").strip().lower() in ("1", "true", "yes", "on")
    canary_start = float(os.getenv("CONF_CAL_PROOF_CANARY_START", "0.10") or 0.10)
    canary_step = float(os.getenv("CONF_CAL_PROOF_CANARY_STEP", "0.10") or 0.10)
    canary_max = float(os.getenv("CONF_CAL_PROOF_CANARY_MAX", "1.0") or 1.0)
    canary_bump = int(os.getenv("CONF_CAL_PROOF_CANARY_BUMP_MIN_SEC", "1800") or 1800)
    ctl = ProofStateController(
        reports_dir=str(reports_dir), proof_path=str(proof_path), state_path=str(state_path),
        min_good_runs=min_good, min_bad_runs=min_bad, max_live_age_sec=max_live_age_sec,
        canary_enable=canary_enable, canary_start=canary_start, canary_step=canary_step,
        canary_max=canary_max, canary_bump_min_sec=canary_bump,
    )
    proof = ctl.step()
    print(json.dumps({"ok": True, "proof_path": proof_path, "valid": bool(proof.get("valid")), "reason": proof.get("reason")}, ensure_ascii=False))
    return 0

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
