from orderflow_services.operator_rca_winner_routing_apply_controller_v2_5 import (
    WinnerDecision,
    build_policy_update,
    compute_apply_decision,
)


def _wd(**kw):
    base = dict(
        experiment_id="exp-1",
        winner_arm="challenger",
        provider="vertex",
        model_name="gemini-2.5-flash-lite",
        prompt_version="ml_triage_v1",
        policy_version="policy_v1",
        sample_n=24,
        winner_score=0.82,
        control_score=0.70,
        confidence=0.88,
        ts_ms=1,
        reason_codes=[],
        raw={},
    )
    base.update(kw)
    return WinnerDecision(**base)


def test_compute_apply_decision_dry_run_promote():
    wd = _wd()
    decision, reasons = compute_apply_decision(
        wd,
        advisory_only=True,
        kill_switch=False,
        min_sample=12,
        min_uplift=0.05,
        min_confidence=0.60,
        cooldown_active=False,
        allowed_providers={"vertex"},
        allowed_models={"gemini-2.5-flash-lite"},
        allowed_prompts={"ml_triage_v1"},
    )
    assert decision == "PROMOTE_DRY_RUN"
    assert "DRY_RUN" in reasons


def test_compute_apply_decision_hold_on_low_sample():
    wd = _wd(sample_n=3)
    decision, reasons = compute_apply_decision(
        wd,
        advisory_only=False,
        kill_switch=False,
        min_sample=12,
        min_uplift=0.05,
        min_confidence=0.60,
        cooldown_active=False,
        allowed_providers={"vertex"},
        allowed_models={"gemini-2.5-flash-lite"},
        allowed_prompts={"ml_triage_v1"},
    )
    assert decision == "HOLD"
    assert "INSUFFICIENT_SAMPLE" in reasons


def test_compute_apply_decision_suppress_on_model_not_allowed():
    wd = _wd(model_name="other-model")
    decision, reasons = compute_apply_decision(
        wd,
        advisory_only=False,
        kill_switch=False,
        min_sample=12,
        min_uplift=0.05,
        min_confidence=0.60,
        cooldown_active=False,
        allowed_providers={"vertex"},
        allowed_models={"gemini-2.5-flash-lite"},
        allowed_prompts={"ml_triage_v1"},
    )
    assert decision == "SUPPRESS"
    assert "MODEL_NOT_ALLOWED" in reasons


def test_build_policy_update_captures_previous_values():
    wd = _wd(prompt_version="ml_triage_v2")
    upd = build_policy_update(wd, {
        "provider": "vertex",
        "model_name": "gemini-2.5-flash-lite",
        "prompt_version": "ml_triage_v1",
        "policy_version": "policy_v1",
    })
    assert upd["prompt_version"] == "ml_triage_v2"
    assert upd["previous_prompt_version"] == "ml_triage_v1"
    assert upd["winner_experiment_id"] == "exp-1"
