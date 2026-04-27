from orderflow_services.ofc_contextual_rollout_controller_v1 import (
    RolloutInputs,
    Thresholds,
    compute_rollout_decision,
)


def _th() -> Thresholds:
    return Thresholds(
        min_observations=300,
        max_shadow_disagree_rate=0.15,
        max_fail_open_rate=0.02,
        max_fallback_rate=0.25,
        max_bundle_age_seconds=21600.0,
        max_writer_lag_seconds=120.0,
        min_hold_sec=1800,
    )


def test_rollout_advances_shadow_to_tighten_only_when_healthy_and_hold_elapsed():
    inp = RolloutInputs(
        observations=500,
        shadow_disagree_rate=0.05,
        fail_open_rate=0.0,
        fallback_rate=0.10,
        bundle_age_seconds=100.0,
        writer_lag_seconds=5.0,
        nightly_success=1,
        rollback_requested=False,
    )
    d = compute_rollout_decision(
        current_mode="shadow",
        last_change_ms=0,
        inputs=inp,
        thresholds=_th(),
        now_ms=3_600_000,
    )
    assert d.desired_mode == "tighten_only"
    assert d.blocked is False


def test_rollout_reverts_to_shadow_on_health_breach():
    inp = RolloutInputs(
        observations=500,
        shadow_disagree_rate=0.30,
        fail_open_rate=0.0,
        fallback_rate=0.10,
        bundle_age_seconds=100.0,
        writer_lag_seconds=5.0,
        nightly_success=1,
        rollback_requested=False,
    )
    d = compute_rollout_decision(
        current_mode="replace_score_veto",
        last_change_ms=0,
        inputs=inp,
        thresholds=_th(),
        now_ms=3_600_000,
    )
    assert d.desired_mode == "shadow"
    assert d.should_set_rollback_flag is True
    assert d.blocked is True


def test_rollout_min_hold_blocks_promotion():
    inp = RolloutInputs(
        observations=500,
        shadow_disagree_rate=0.01,
        fail_open_rate=0.0,
        fallback_rate=0.01,
        bundle_age_seconds=100.0,
        writer_lag_seconds=5.0,
        nightly_success=1,
        rollback_requested=False,
    )
    d = compute_rollout_decision(
        current_mode="shadow",
        last_change_ms=1000,
        inputs=inp,
        thresholds=_th(),
        now_ms=1000 + 60_000,
    )
    assert d.desired_mode == "shadow"
    assert d.blocked_reason == "min_hold"
