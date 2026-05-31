"""
WR stop-bleed 2026-05-27 — regression for virtual signal hard-drop.

Trade report (5/27 17:04 UTC, 60min window):
  198 signals → 147 bypassed → 140 virtual trades → 120/122 INITIAL_SL hits
  → WR=1.6%. Virtual signals with `validation_status=bypassed/failed` were
  flowing all the way to trade_monitor and creating positions.

Fix: pure helper `should_drop_virtual_signal` decides per payload.
Caller in publish_signal gates on `VIRTUAL_GATE_HARD_DROP_ENABLED` and
`VIRTUAL_GATE_HARD_DROP_SHADOW` envs.
"""
from __future__ import annotations

from services.orderflow.signal_pipeline import should_drop_virtual_signal


def test_real_signal_never_dropped():
    """is_virtual=0 → never dropped regardless of validation_status."""
    sig = {"is_virtual": 0, "validation_status": "failed", "v_gate_reason": "VETO_FOO"}
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is False
    assert reason == ""


def test_virtual_failed_validation_dropped():
    sig = {"is_virtual": 1, "validation_status": "failed"}
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is True
    assert reason == "validation_failed"


def test_virtual_bypassed_validation_dropped():
    """The dominant case — OFConfirm not evaluated for virtual."""
    sig = {"is_virtual": 1, "validation_status": "bypassed"}
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is True
    assert reason == "validation_bypassed"


def test_virtual_passed_validation_kept():
    """Virtual + passed → keep (legitimate shadow sample)."""
    sig = {"is_virtual": 1, "validation_status": "passed"}
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is False
    assert reason == ""


def test_virtual_with_hard_veto_reason_dropped():
    """validation_status=passed but v_gate_reason has VETO_ → hard veto."""
    sig = {
        "is_virtual": 1,
        "validation_status": "passed",
        "v_gate_reason": "VETO_BURST_SOFT",
    }
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is True
    assert reason == "hard_veto"


def test_virtual_with_hard_veto_in_validation_reason():
    """validation_reason field with VETO_ also triggers."""
    sig = {
        "is_virtual": 1,
        "validation_status": "passed",
        "validation_reason": "score_veto: VETO_OF_SCORE_MIN",
    }
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is True
    assert reason == "hard_veto"


def test_virtual_with_hard_veto_keyword():
    """HARD_VETO substring also matches."""
    sig = {
        "is_virtual": 1,
        "validation_status": "passed",
        "v_gate_reason": "HARD_VETO_burst",
    }
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is True
    assert reason == "hard_veto"


def test_virtual_with_benign_reason_kept():
    """No VETO_ in reasons → keep."""
    sig = {
        "is_virtual": 1,
        "validation_status": "passed",
        "v_gate_reason": "OFConfirm passed",
        "validation_reason": "ok",
    }
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is False
    assert reason == ""


def test_virtual_no_status_no_reason_kept():
    """Minimal virtual signal — no drop signal."""
    sig = {"is_virtual": 1}
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is False
    assert reason == ""


def test_failed_takes_priority_over_hard_veto():
    """validation_status=failed wins over hard_veto label."""
    sig = {
        "is_virtual": 1,
        "validation_status": "failed",
        "v_gate_reason": "VETO_SOMETHING",
    }
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is True
    assert reason == "validation_failed"


def test_handles_is_virtual_as_string():
    """is_virtual could come as string from JSON parsing."""
    sig = {"is_virtual": "1", "validation_status": "bypassed"}
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is True
    assert reason == "validation_bypassed"


def test_handles_invalid_is_virtual_gracefully():
    """Garbage is_virtual → treat as not virtual (fail-safe)."""
    sig = {"is_virtual": "not_a_number", "validation_status": "bypassed"}
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is False
    assert reason == ""


def test_case_insensitive_validation_status():
    """validation_status='FAILED' (upper) — lowercased before compare."""
    sig = {"is_virtual": 1, "validation_status": "FAILED"}
    drop, reason = should_drop_virtual_signal(sig)
    assert drop is True
    assert reason == "validation_failed"
