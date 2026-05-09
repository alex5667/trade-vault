from __future__ import annotations

"""
test_exec_health_slo_autoguard_v1.py
Unit tests for the pure evaluate_autoguard() function of P5 AutoGuard.
No Redis I/O required — all tests use the deterministic EvalResult path.
"""

from orderflow_services.exec_health_slo_autoguard_v1 import GuardCfg, evaluate_autoguard


def _cfg() -> GuardCfg:
    return GuardCfg(
        redis_url='redis://x',
        summary_key='k1',
        state_key='k2',
        freeze_key='k3',
        notify_stream='k4',
        loop_s=30,
        mode_mismatch_minutes=5,
        drift_minutes=10,
        drift_instances_min=2,
        freeze_minutes=30,
        cooldown_minutes=30,
        rollback_enable=True,
        rollback_on_mode_mismatch=True,
        rollback_on_drift=False,
        enabled=True,
    )


def test_autoguard_triggers_on_sustained_mode_mismatch() -> None:
    """Mode mismatch held for > mode_mismatch_minutes should produce a trigger."""
    cfg = _cfg()
    now = 1_000_000
    prev = {"mode_mismatch_since_ts_ms": str(now - 6 * 60 * 1000), "rollout_drift_since_ts_ms": "0"}
    summary = {"cross_scope_mode_distinct": "2", "rollout_drift_instances_total": "0"}
    ev = evaluate_autoguard(summary=summary, prev_state=prev, cfg=cfg, now_ms=now)
    assert ev.should_trigger is True
    assert "cross_scope_mode_mismatch" in ev.trigger_reasons
    assert ev.rollout_drift_active is False


def test_autoguard_triggers_on_sustained_rollout_drift() -> None:
    """Rollout drift above threshold held for > drift_minutes should produce a trigger."""
    cfg = _cfg()
    now = 2_000_000
    prev = {"mode_mismatch_since_ts_ms": "0", "rollout_drift_since_ts_ms": str(now - 11 * 60 * 1000)}
    summary = {"cross_scope_mode_distinct": "1", "rollout_drift_instances_total": "3"}
    ev = evaluate_autoguard(summary=summary, prev_state=prev, cfg=cfg, now_ms=now)
    assert ev.should_trigger is True
    assert "rollout_drift" in ev.trigger_reasons


def test_autoguard_resets_since_when_condition_clears() -> None:
    """When conditions are no longer active, since timestamps must reset to 0."""
    cfg = _cfg()
    prev = {"mode_mismatch_since_ts_ms": "123", "rollout_drift_since_ts_ms": "456"}
    summary = {"cross_scope_mode_distinct": "1", "rollout_drift_instances_total": "0"}
    ev = evaluate_autoguard(summary=summary, prev_state=prev, cfg=cfg, now_ms=999)
    assert ev.mode_mismatch_since_ts_ms == 0
    assert ev.rollout_drift_since_ts_ms == 0
    assert ev.should_trigger is False


def test_autoguard_no_trigger_before_sustained_duration() -> None:
    """Condition active but not yet sustained long enough — must NOT trigger."""
    cfg = _cfg()
    now = 1_000_000
    # Only 2 minutes since mismatch start, threshold is 5 minutes
    prev = {"mode_mismatch_since_ts_ms": str(now - 2 * 60 * 1000), "rollout_drift_since_ts_ms": "0"}
    summary = {"cross_scope_mode_distinct": "3", "rollout_drift_instances_total": "0"}
    ev = evaluate_autoguard(summary=summary, prev_state=prev, cfg=cfg, now_ms=now)
    assert ev.should_trigger is False
    assert ev.mode_mismatch_active is True


def test_autoguard_drift_below_min_instances_no_trigger() -> None:
    """Drift instance count below drift_instances_min — must NOT trigger."""
    cfg = _cfg()
    now = 5_000_000
    prev = {"mode_mismatch_since_ts_ms": "0", "rollout_drift_since_ts_ms": str(now - 11 * 60 * 1000)}
    # drift_instances_min=2, only 1 instance
    summary = {"cross_scope_mode_distinct": "1", "rollout_drift_instances_total": "1"}
    ev = evaluate_autoguard(summary=summary, prev_state=prev, cfg=cfg, now_ms=now)
    assert ev.should_trigger is False
    assert ev.rollout_drift_active is False


def test_autoguard_latches_since_timestamp() -> None:
    """When condition is already active and since_ts is set, it must be preserved."""
    cfg = _cfg()
    now = 9_000_000
    original_since = now - 3 * 60 * 1000
    prev = {"mode_mismatch_since_ts_ms": str(original_since), "rollout_drift_since_ts_ms": "0"}
    summary = {"cross_scope_mode_distinct": "2", "rollout_drift_instances_total": "0"}
    ev = evaluate_autoguard(summary=summary, prev_state=prev, cfg=cfg, now_ms=now)
    # since must be latched at the original value, not reset to now
    assert ev.mode_mismatch_since_ts_ms == original_since
