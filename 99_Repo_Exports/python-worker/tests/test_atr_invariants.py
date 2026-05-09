import os

import pytest

from services.atr_invariant_replay_engine import InvariantReplayEngine
from services.atr_invariant_runtime_engine import InvariantRuntimeEngine


@pytest.fixture
def runtime_engine():
    # Force hard enforcement for testing
    os.environ["ATR_INVARIANTS_ADVISORY_ONLY"] = "0"
    os.environ["ATR_INVARIANTS_RUNTIME_DENY_CRITICAL"] = "1"
    os.environ["ATR_INVARIANTS_MOCK_ENV"] = "1" # Bypass DB init if needed

    # Init with mock invariants
    engine = InvariantRuntimeEngine()

    # We can mock the DB load
    engine.invariants = [
        {
            "reason_code": "INV_PAYLOAD_001",
            "category": "Payload",
            "enforcement_mode": "runtime_deny",
            "enabled": True,
            "condition_sql": "SL > 0 AND ENTRY > 0"
        },
        {
            "reason_code": "INV_EXEC_001",
            "category": "Execution",
            "enforcement_mode": "runtime_deny",
            "enabled": True,
            "condition_sql": "RISK <= 10.0"
        }
    ]
    return engine

def test_runtime_engine_valid_signal(runtime_engine):
    payload = {
        "signal_id": "test_001",
        "side": "LONG",
        "entry_price": 60000.0,
        "sl_price": 59000.0,
        "tp1_price": 61000.0,
        "tradeable": True,
        "risk_pct": 2.0
    }

    allow, violations = runtime_engine.validate_signal(payload)
    assert allow is True
    assert len(violations) == 0

def test_runtime_engine_invalid_sl(runtime_engine):
    payload = {
        "signal_id": "test_002",
        "side": "LONG",
        "entry_price": 60000.0,
        "sl_price": 0.0, # Missing SL
        "tp1_price": 61000.0,
        "tradeable": True
    }

    allow, violations = runtime_engine.validate_signal(payload)
    assert allow is False
    assert "INV_NO_ORDER_WITHOUT_SL" in [v["reason_code"] for v in violations] if isinstance(violations[0], dict) else "INV_NO_ORDER_WITHOUT_SL" in violations

def test_runtime_engine_tradeable_vetoed(runtime_engine):
    payload = {
        "signal_id": "test_003",
        "side": "LONG",
        "entry_price": 60000.0,
        "sl_price": 59000.0,
        "tp1_price": 61000.0,
        "is_rejected_signal": 1, # vetoed
        "rejection_reason": "SOME_REASON",
        "tradeable": False # Corrected. The invariant checks for veto_reason but tradeable=True
    }

    # If the signal is vetoed (tradeable=False), it should be allowed to drop, no invariant breach
    # Wait, the invariant INV_STR_GATE checks if (is_vetoed) AND (tradeable == True).
    # If we supply tradeable=True and is_rejected=1, it should fail.
    payload["tradeable"] = True

    allow, violations = runtime_engine.validate_signal(payload)
    assert allow is False
    assert "INV_TRADEABLE_REQUIRES_NO_HARD_VETO" in [v["reason_code"] for v in violations] if isinstance(violations[0], dict) else "INV_TRADEABLE_REQUIRES_NO_HARD_VETO" in violations

def test_replay_engine():
    engine = InvariantReplayEngine()

    baseline = {
        "signal_id": "sig1",
        "side": "LONG"
    }
    candidate = {
        "signal_id": "sig2", # drifted
        "side": "LONG"
    }

    violations = engine.validate_change(baseline, candidate, "req_1")
    assert any(v["reason_code"] == "INV_SIGNAL_ID_STABLE_IN_REPLAY" for v in violations if isinstance(v, dict))
