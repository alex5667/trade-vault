from __future__ import annotations

"""
Unit tests — burst_gate_calibrator_service.py

Проверяем:
  1. evaluate_burst_gate_calibration — FSM penalty→shadow→enforce_request / rollback
  2. _apply_dynamic_cfg — пишет burst_gate_mode в config:orderflow:* хеши (fakeredis)
  3. _load_state / _save_state — round-trip через fakeredis
  4. handle_enforce_approve / handle_enforce_reject — меняют state и pending
  5. Throttle-логика
"""

import json
import time
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

from services.burst_gate_calibrator_service import (
    BurstCalibState,
    BurstCalibResult,
    evaluate_burst_gate_calibration,
    handle_enforce_approve,
    handle_enforce_reject,
    _apply_dynamic_cfg,
    _load_state,
    _save_state,
    _throttle_ok,
    _record_throttle,
    STATE_KEY,
    THROTTLE_KEY,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _state(mode: str = "penalty", proof: int = 0, rollback: int = 0) -> BurstCalibState:
    return BurstCalibState(mode=mode, proof_streak=proof, rollback_streak=rollback)


def _shares(n: int = 3, v: float = 0.10) -> dict[str, float]:
    return {f"SYM{i}": v for i in range(n)}


# ---------------------------------------------------------------------------
# 1. Pure calibration FSM
# ---------------------------------------------------------------------------

class TestEvaluateBurstGateCalibration:

    def _eval(self, shares, prev, *, min_share=0.05, max_share=0.20,
              min_streak=7, enforce_streak=7, rollback_streak=3):
        return evaluate_burst_gate_calibration(
            shares, prev,
            min_share=min_share, max_share=max_share,
            min_streak=min_streak, enforce_streak=enforce_streak,
            rollback_streak_required=rollback_streak,
        )

    def test_hold_insufficient_streak(self):
        r = self._eval(_shares(v=0.10), _state("penalty", proof=3))
        assert r.recommend == "hold"
        assert r.effective_mode == "penalty"
        assert r.proof_streak == 4

    def test_promote_penalty_to_shadow(self):
        r = self._eval(_shares(v=0.10), _state("penalty", proof=6), min_streak=7)
        assert r.recommend == "promote"
        assert r.effective_mode == "shadow"
        assert r.proof_streak == 7

    def test_shadow_not_auto_enforce(self):
        # streak ≥ min+enforce → recommend=promote but mode stays shadow (human ACK needed)
        r = self._eval(_shares(v=0.10), _state("shadow", proof=13), min_streak=7, enforce_streak=7)
        assert r.recommend == "promote"
        assert r.effective_mode == "shadow"  # no auto-enforce

    def test_rollback_from_shadow(self):
        r = self._eval(_shares(v=0.30), _state("shadow", rollback=2), max_share=0.20, rollback_streak=3)
        assert r.rollback_streak == 3
        assert r.recommend == "rollback"
        assert r.effective_mode == "penalty"
        assert r.proof_streak == 0

    def test_rollback_from_enforce(self):
        r = self._eval(_shares(v=0.30), _state("enforce", rollback=2), max_share=0.20, rollback_streak=3)
        assert r.recommend == "rollback"
        assert r.effective_mode == "penalty"

    def test_share_too_low_no_promote(self):
        r = self._eval(_shares(v=0.01), _state("penalty", proof=10), min_share=0.05, min_streak=7)
        assert r.recommend == "hold"
        assert r.effective_mode == "penalty"
        assert "share_too_low" in r.reason

    def test_share_too_high_increments_rollback(self):
        r = self._eval(_shares(v=0.25), _state("penalty", rollback=1), max_share=0.20)
        assert r.rollback_streak == 2
        assert "share_too_high" in r.reason

    def test_empty_shares_returns_hold(self):
        r = self._eval({}, _state("penalty"))
        assert r.recommend == "hold"
        assert r.avg_share == 0.0
        assert r.n_symbols == 0

    def test_avg_share_computed_correctly(self):
        shares = {"A": 0.10, "B": 0.20}
        r = self._eval(shares, _state("penalty", proof=10), min_streak=7)
        assert abs(r.avg_share - 0.15) < 1e-9

    def test_rollback_wins_over_promote(self):
        # Even if streak is high, rollback condition fires first
        r = self._eval(
            _shares(v=0.30),
            _state("shadow", proof=20, rollback=2),
            max_share=0.20, rollback_streak=3,
        )
        assert r.recommend == "rollback"


# ---------------------------------------------------------------------------
# 2. _apply_dynamic_cfg — fakeredis
# ---------------------------------------------------------------------------

class TestApplyDynamicCfg:

    def test_writes_burst_gate_mode_to_all_3part_keys(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        r.hset("config:orderflow:BTCUSDT", "some_key", "val")
        r.hset("config:orderflow:ETHUSDT", "some_key", "val")
        # 4-part key should be skipped
        r.hset("config:orderflow:BTCUSDT:obi_stable", "x", "1")

        from services.burst_gate_calibrator_service import _apply_dynamic_cfg
        n = _apply_dynamic_cfg(r, "shadow")

        assert n == 2
        assert r.hget("config:orderflow:BTCUSDT", "burst_gate_mode") == "shadow"
        assert r.hget("config:orderflow:ETHUSDT", "burst_gate_mode") == "shadow"
        # 4-part key NOT written
        assert r.hget("config:orderflow:BTCUSDT:obi_stable", "burst_gate_mode") is None

    def test_writes_enforce(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        r.hset("config:orderflow:SOLUSDT", "x", "1")
        from services.burst_gate_calibrator_service import _apply_dynamic_cfg
        _apply_dynamic_cfg(r, "enforce")
        assert r.hget("config:orderflow:SOLUSDT", "burst_gate_mode") == "enforce"

    def test_empty_returns_zero(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        from services.burst_gate_calibrator_service import _apply_dynamic_cfg
        assert _apply_dynamic_cfg(r, "shadow") == 0


# ---------------------------------------------------------------------------
# 3. State persistence
# ---------------------------------------------------------------------------

class TestStatePersistence:

    def test_round_trip(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        result = BurstCalibResult(
            prev_mode="penalty", effective_mode="shadow",
            recommend="promote", reason="streak:7>=7|share:10.0%",
            proof_streak=7, rollback_streak=0,
            avg_share=0.10, n_symbols=5,
        )
        _save_state(r, result)
        loaded = _load_state(r)
        assert loaded.mode == "shadow"
        assert loaded.proof_streak == 7
        assert loaded.rollback_streak == 0
        assert abs(loaded.last_share - 0.10) < 1e-6

    def test_load_state_missing_returns_default(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        s = _load_state(r)
        assert s.mode == "penalty"
        assert s.proof_streak == 0

    def test_load_state_corrupted_returns_default(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        r.set(STATE_KEY, "not-json")
        s = _load_state(r)
        assert s.mode == "penalty"


# ---------------------------------------------------------------------------
# 4. Telegram callback handlers
# ---------------------------------------------------------------------------

class TestHandleEnforceApprove:

    def _setup(self, r, run_id: str, status: str = "PENDING") -> None:
        r.hset("config:orderflow:BTCUSDT", "x", "1")
        r.hset("config:orderflow:ETHUSDT", "x", "1")
        pending = {
            "run_id": run_id, "status": status,
            "action": "burst_gate_enforce",
            "avg_share": 0.10, "proof_streak": 14, "n_symbols": 2,
            "created_at_ms": int(time.time() * 1000),
        }
        r.set(f"burst_gate_calib:pending:{run_id}", json.dumps(pending), ex=86400)

    def test_approve_writes_enforce_and_updates_state(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        self._setup(r, "run-001")
        msg = handle_enforce_approve(r, "run-001")

        assert "enforce" in msg
        assert r.hget("config:orderflow:BTCUSDT", "burst_gate_mode") == "enforce"
        state = json.loads(r.get(STATE_KEY))
        assert state["mode"] == "enforce"

    def test_approve_marks_pending_approved(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        self._setup(r, "run-002")
        handle_enforce_approve(r, "run-002")
        raw = r.get("burst_gate_calib:pending:run-002")
        assert json.loads(raw)["status"] == "APPROVED"

    def test_approve_missing_run_id_returns_error(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        msg = handle_enforce_approve(r, "no-such-run")
        assert "not found" in msg.lower() or "expired" in msg.lower()

    def test_approve_already_handled_returns_warning(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        self._setup(r, "run-003", status="APPROVED")
        msg = handle_enforce_approve(r, "run-003")
        assert "already" in msg.lower() or "APPROVED" in msg


class TestHandleEnforceReject:

    def _setup(self, r, run_id: str) -> None:
        r.hset("config:orderflow:BTCUSDT", "burst_gate_mode", "shadow")
        pending = {
            "run_id": run_id, "status": "PENDING",
            "avg_share": 0.10, "proof_streak": 14,
        }
        r.set(f"burst_gate_calib:pending:{run_id}", json.dumps(pending), ex=86400)

    def test_reject_marks_pending_rejected(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        self._setup(r, "run-rej-001")
        msg = handle_enforce_reject(r, "run-rej-001")
        raw = r.get("burst_gate_calib:pending:run-rej-001")
        assert json.loads(raw)["status"] == "REJECTED"
        assert "shadow" in msg.lower() or "reject" in msg.lower()

    def test_reject_keeps_shadow_in_config(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        self._setup(r, "run-rej-002")
        handle_enforce_reject(r, "run-rej-002")
        # mode should NOT have changed to enforce
        mode = r.hget("config:orderflow:BTCUSDT", "burst_gate_mode")
        assert mode == "shadow"

    def test_reject_missing_run_id_safe(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        msg = handle_enforce_reject(r, "nonexistent")
        assert isinstance(msg, str)


# ---------------------------------------------------------------------------
# 5. Throttle
# ---------------------------------------------------------------------------

class TestThrottle:

    def test_first_call_ok(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        assert _throttle_ok(r, 3600) is True

    def test_second_call_within_interval_blocked(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        _record_throttle(r, 3600)
        assert _throttle_ok(r, 3600) is False

    def test_after_interval_ok(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        # Simulate last_ts far in the past
        r.set(THROTTLE_KEY, str(time.time() - 7200))
        assert _throttle_ok(r, 3600) is True
