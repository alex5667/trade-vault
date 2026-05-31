from orderflow_services.operator_rca_usefulness_governor_v2_2 import (
    GovernorRow,
    build_action_pattern_decisions,
    build_provider_version_decisions,
)


def test_action_pattern_suppressed_when_low_quality_and_low_usefulness():
    rows = [
        GovernorRow(
            recommendation_id=f"r{i}",
            analysis_run_id="a1",
            provider="vertex",
            model_name="gemini-2.5-flash-lite",
            prompt_version="ml_triage_v1",
            policy_version="policy_v1",
            action_type="propose_threshold_canary",
            quality_score=0.20,
            usefulness_score=0.10,
            feedback_n=8,
            ts_ms=1,
        )
        for i in range(12)
    ]
    decisions = build_action_pattern_decisions(rows)
    assert len(decisions) == 1
    assert decisions[0]["decision"] == "SUPPRESS"


def test_action_pattern_promoted_when_high_quality_and_useful():
    rows = [
        GovernorRow(
            recommendation_id=f"r{i}",
            analysis_run_id="a1",
            provider="vertex",
            model_name="gemini-2.5-flash-lite",
            prompt_version="ml_triage_v1",
            policy_version="policy_v1",
            action_type="freeze_candidate",
            quality_score=0.90,
            usefulness_score=1.0,
            feedback_n=10,
            ts_ms=1,
        )
        for i in range(12)
    ]
    decisions = build_action_pattern_decisions(rows)
    assert decisions[0]["decision"] == "PROMOTE"


def test_provider_prompt_hold_on_low_sample():
    rows = [
        GovernorRow(
            recommendation_id=f"r{i}",
            analysis_run_id="a1",
            provider="vertex",
            model_name="gemini-2.5-flash-lite",
            prompt_version="ml_triage_v1",
            policy_version="policy_v1",
            action_type="open_incident",
            quality_score=0.99,
            usefulness_score=1.0,
            feedback_n=2,
            ts_ms=1,
        )
        for i in range(4)
    ]
    decisions = build_provider_version_decisions(rows)
    assert decisions[0]["decision"] == "HOLD"


def test_provider_prompt_can_be_suppressed():
    rows = [
        GovernorRow(
            recommendation_id=f"r{i}",
            analysis_run_id="a1",
            provider="vertex",
            model_name="gemini-2.5-flash-lite",
            prompt_version="ml_triage_v1",
            policy_version="policy_v1",
            action_type="draft_postmortem",
            quality_score=0.20,
            usefulness_score=0.0,
            feedback_n=9,
            ts_ms=1,
        )
        for i in range(15)
    ]
    decisions = build_provider_version_decisions(rows)
    assert decisions[0]["decision"] == "SUPPRESS"
