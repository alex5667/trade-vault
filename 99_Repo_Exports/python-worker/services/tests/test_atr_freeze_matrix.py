import pytest
from datetime import datetime, timezone, timedelta
from services.atr_freeze_matrix_service import ATRFreezeMatrixService
from services.atr_unfreeze_hysteresis_service import ATRUnfreezeHysteresisService

# Mock policies
POLICIES = [
    {
        "policy_id": "p1",
        "trigger_kind": "runtime_budget_exhausted",
        "scope_kind": "symbol",
        "severity": "critical",
        "freeze_state": "scope_frozen",
        "ttl_sec": 3600,
        "is_enabled": True
    },
    {
        "policy_id": "p2",
        "trigger_kind": "protective_breach",
        "scope_kind": "global",
        "severity": "critical",
        "freeze_state": "hard_freeze",
        "ttl_sec": 86400,
        "is_enabled": True
    }
]

def test_freeze_escalation():
    svc = ATRFreezeMatrixService(advisory_only=True)
    
    # 1. Trigger scope_frozen
    trigger1 = {
        "trigger_kind": "runtime_budget_exhausted",
        "scope_kind": "symbol",
        "scope_value": "BTCUSDT",
        "severity": "critical"
    }

    res1 = svc.evaluate_trigger(trigger1, [], POLICIES)
    assert res1["status"] == "created"
    assert res1["freeze_state"] == "scope_frozen"

    # Assume it's now active
    active_freezes = [{
        "freeze_id": res1["freeze_id"],
        "scope_kind": "symbol",
        "scope_value": "BTCUSDT",
        "freeze_state": "scope_frozen",
        "status": "active",
    }]

    # 2. Trigger hard_freeze on global
    trigger2 = {
        "trigger_kind": "protective_breach",
        "scope_kind": "global",
        "scope_value": "all",
        "severity": "critical"
    }
    
    res2 = svc.evaluate_trigger(trigger2, active_freezes, POLICIES)
    assert res2["status"] == "created"
    assert res2["freeze_state"] == "hard_freeze"

def test_unfreeze_hysteresis():
    svc = ATRUnfreezeHysteresisService(require_cert=False)
    now_utc = datetime.now(timezone.utc)
    
    # Active freeze with dwell time in the past
    active_freezes = [{
        "freeze_id": "f1",
        "status": "active",
        "freeze_state": "scope_frozen",
        "started_at": (now_utc - timedelta(hours=2)).isoformat(),
        "recovery_not_before": (now_utc - timedelta(hours=1)).isoformat()
    }]

    # Unhealthy context
    health_bad = {
        "burn_rate_healthy": False,
        "allocator_fresh": True,
        "open_critical_incidents": 0,
        "recent_violations": 0
    }
    
    transitions = svc.evaluate_unfreeze_candidates(active_freezes, health_bad)
    # Should not transition because health is bad
    assert len(transitions) == 0

    # Healthy context
    health_good = {
        "burn_rate_healthy": True,
        "allocator_fresh": True,
        "open_critical_incidents": 0,
        "recent_violations": 0
    }

    transitions = svc.evaluate_unfreeze_candidates(active_freezes, health_good)
    assert len(transitions) == 1
    assert transitions[0]["new_status"] == "recovering"

    # Now simulate recovering state with cert required but not present
    svc_cert = ATRUnfreezeHysteresisService(require_cert=True)
    recovering_freezes = [{
        "freeze_id": "f1",
        "status": "recovering",
        "freeze_state": "scope_frozen",
        "started_at": (now_utc - timedelta(hours=2)).isoformat(),
        "recovery_not_before": (now_utc - timedelta(hours=1)).isoformat()
    }]
    
    transitions2 = svc_cert.evaluate_unfreeze_candidates(recovering_freezes, health_good)
    assert len(transitions2) == 1
    # Missing cert, should stay recovering
    assert transitions2[0]["new_status"] == "recovering"
    assert transitions2[0]["reason_code"] == "HYSTERESIS_PENDING_CERT"

    # Provide cert
    health_good["cert_passed_f1"] = True
    transitions3 = svc_cert.evaluate_unfreeze_candidates(recovering_freezes, health_good)
    assert len(transitions3) == 1
    # From scope_frozen -> clip
    assert transitions3[0]["update_payload"]["freeze_state"] == "clip"
    assert transitions3[0]["reason_code"] == "HYSTERESIS_STAGED_TO_CLIP"

