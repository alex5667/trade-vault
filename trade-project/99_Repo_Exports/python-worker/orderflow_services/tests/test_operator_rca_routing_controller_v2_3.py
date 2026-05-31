from orderflow_services.operator_rca_routing_controller_v2_3 import (
    GovernorDecision,
    choose_route,
)


def test_choose_route_promote_beats_current_when_not_suppressed():
    current = {
        "provider": "vertex",
        "model_name": "gemini-2.5-flash-lite",
        "prompt_version": "ml_triage_v1",
        "policy_version": "policy_v1",
    }
    decisions = [
        GovernorDecision(
            scope="provider_prompt",
            decision="PROMOTE",
            action_type="*",
            provider="vertex",
            model_name="gemini-2.5-flash",
            prompt_version="ml_triage_v2",
            policy_version="policy_v2",
            score=0.91,
            ts_ms=1,
            reason_codes=["PROMOTE_HIGH_USEFULNESS"],
        )
    ]
    route, audit = choose_route(
        current=current,
        decisions=decisions,
        allow_promote=True,
        allow_suppress=True,
        fallback_provider="vertex",
        fallback_model="gemini-2.5-flash-lite",
        fallback_prompt_version="ml_triage_v1",
    )
    assert route["model_name"] == "gemini-2.5-flash"
    assert route["prompt_version"] == "ml_triage_v2"
    assert any(x["event"] == "PROMOTE_SELECTED" for x in audit)


def test_choose_route_falls_back_when_active_route_suppressed():
    current = {
        "provider": "vertex",
        "model_name": "gemini-2.5-flash-lite",
        "prompt_version": "ml_triage_v1",
        "policy_version": "policy_v1",
    }
    decisions = [
        GovernorDecision(
            scope="provider_prompt",
            decision="SUPPRESS",
            action_type="*",
            provider="vertex",
            model_name="gemini-2.5-flash-lite",
            prompt_version="ml_triage_v1",
            policy_version="policy_v1",
            score=0.22,
            ts_ms=1,
            reason_codes=["SUPPRESS_LOW_USEFULNESS"],
        )
    ]
    route, audit = choose_route(
        current=current,
        decisions=decisions,
        allow_promote=True,
        allow_suppress=True,
        fallback_provider="vertex",
        fallback_model="gemini-2.5-flash-lite",
        fallback_prompt_version="ml_triage_v1",
    )
    assert route["provider"] == "vertex"
    assert any(x["event"] == "FALLBACK_SELECTED" for x in audit)

