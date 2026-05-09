from services.orderflow.policy.circuit_breaker_v1 import (
    CircuitBreakerDecision,
    _norm_state,
    apply_circuit_breaker_overrides,
    decide_circuit_breaker,
)


def test_norm_state():
    assert _norm_state("ok") == "ok"
    assert _norm_state("OK ") == "ok"
    assert _norm_state(0) == "ok"
    assert _norm_state(False) == "ok"

    assert _norm_state("warn") == "warn"
    assert _norm_state(1) == "warn"

    assert _norm_state("block") == "block"
    assert _norm_state("BAD") == "block"
    assert _norm_state(2) == "block"
    assert _norm_state(True) == "block"

    assert _norm_state(None) == "unknown"
    assert _norm_state("weird") == "unknown"

def test_decide_circuit_breaker_base():
    cfg = {"cb_enable": True}

    # OK
    d = decide_circuit_breaker(cfg=cfg, dq_state="ok", drift_state="ok")
    assert d.regime == "ok"
    assert not d.force_rule_strong_only
    assert not d.disable_ml_enforce

    # Block by DQ
    d = decide_circuit_breaker(cfg=cfg, dq_state="block", drift_state="ok")
    assert d.regime == "block"
    assert d.force_rule_strong_only
    assert d.disable_ml_enforce

    # Block by Drift
    d = decide_circuit_breaker(cfg=cfg, dq_state="ok", drift_state="block")
    assert d.regime == "block"

def test_decide_circuit_breaker_quality_escalation():
    cfg = {
        "cb_enable": True,
        "cb_quality_min_n_24h": 10,
        "signal_quality_n_24h": 20,
        "signal_quality_ece_24h": 0.25, # High ECE -> should block
    }

    # DQ/Drift are OK, but quality is bad
    d = decide_circuit_breaker(cfg=cfg, dq_state="ok", drift_state="ok")
    assert d.regime == "block"
    assert "quality:block" in d.reason

def test_apply_overrides():
    cfg = {"cb_enable": True}
    decision = CircuitBreakerDecision(
        ver="v1",
        regime="block",
        reason="test",
        force_rule_strong_only=True,
        disable_ml_enforce=True,
        dq_state="ok",
        drift_state="ok",
        ece_24h=0.0,
        expectancy_r_24h=0.0,
        precision_top5p_24h=0.0
    )

    overrides, fields = apply_circuit_breaker_overrides(cfg=cfg, decision=decision)

    assert overrides["require_strong_confirmation"] == 1
    assert overrides["ml_enforce_disable"] == 1
    assert fields["policy_regime"] == "block"
    assert fields["policy_force_rule_strong_only"] == 1
