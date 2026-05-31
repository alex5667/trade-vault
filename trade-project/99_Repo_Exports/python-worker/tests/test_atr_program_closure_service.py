from unittest.mock import patch

import pytest

from services.atr_program_closure_service import (
    ATRProgramClosureService,
    DomainHandoffStatus,
    ProgramClosureVerdict,
    ResidualBacklogClass,
)


@pytest.fixture
def service():
    return ATRProgramClosureService()

@pytest.fixture
def valid_criteria():
    return dict(
        charter_active=True,
        enforcement_map_active=True,
        critical_coverage_gaps=0,
        e2e_acceptance_passed=True,
        go_live_signed=True,
        critical_quarantine_active=False
    )

@pytest.fixture
def valid_handoffs():
    domains = [
        "signal_and_gates",
        "dispatch_and_runtime",
        "execution",
        "protective_lifecycle",
        "control_plane_governance",
        "dr_refresh", # This should fail since we need all 6 exact domains
        "dr_replay_archive",
    ]
    return [
        {
            "domain": d,
            "primary_owner": "owner@trade",
            "oncall_route": "#alerts-trade",
            "status": DomainHandoffStatus.ACCEPTED
        } for d in domains
    ]

def test_closure_criteria(service, valid_criteria):
    assert service.evaluate_closure_criteria(**valid_criteria) is True

    invalid_criteria = valid_criteria.copy()
    invalid_criteria["critical_coverage_gaps"] = 2
    assert service.evaluate_closure_criteria(**invalid_criteria) is False

    invalid_criteria = valid_criteria.copy()
    invalid_criteria["critical_quarantine_active"] = True
    assert service.evaluate_closure_criteria(**invalid_criteria) is False

def test_backlog_classification(service):
    backlog_input = [
        {
            "domain": "observability",
            "priority": "P2",
            "backlog_class": ResidualBacklogClass.NON_BLOCKING,
            "title": "Add dashboard"
        },
        {
            "domain": "protective_lifecycle",
            "priority": "P0",
            "backlog_class": ResidualBacklogClass.NON_BLOCKING, # Should be forced to BLOCKING
            "title": "Critical gap in protective"
        }
    ]

    classified = service.classify_residual_backlog("pkg_1", backlog_input)
    assert classified[0]["backlog_class"] == ResidualBacklogClass.NON_BLOCKING
    assert classified[1]["backlog_class"] == ResidualBacklogClass.BLOCKING

def test_verdict_aggregation_program_closed(service, valid_criteria, valid_handoffs):
    criteria_pass = service.evaluate_closure_criteria(**valid_criteria)
    handoffs = service.build_handoff_matrix("pkg_1", valid_handoffs)
    backlog = []

    verdict = service.compute_program_closure_verdict(criteria_pass, handoffs, backlog)
    assert verdict == ProgramClosureVerdict.PROGRAM_CLOSED

def test_verdict_aggregation_with_residual_backlog(service, valid_criteria, valid_handoffs):
    criteria_pass = service.evaluate_closure_criteria(**valid_criteria)
    handoffs = service.build_handoff_matrix("pkg_1", valid_handoffs)
    backlog = service.classify_residual_backlog("pkg_1", [
        {"domain": "ui", "priority": "P3", "backlog_class": ResidualBacklogClass.NON_BLOCKING}
    ])

    verdict = service.compute_program_closure_verdict(criteria_pass, handoffs, backlog)
    assert verdict == ProgramClosureVerdict.CLOSED_WITH_RESIDUAL_BACKLOG

def test_verdict_aggregation_hold_open(service, valid_criteria, valid_handoffs):
    criteria_pass = service.evaluate_closure_criteria(**valid_criteria)
    handoffs = service.build_handoff_matrix("pkg_1", valid_handoffs)
    backlog = service.classify_residual_backlog("pkg_1", [
        {"domain": "execution", "priority": "P0", "backlog_class": ResidualBacklogClass.NON_BLOCKING} # Elevates to BLOCKING
    ])

    verdict = service.compute_program_closure_verdict(criteria_pass, handoffs, backlog)
    assert verdict == ProgramClosureVerdict.HOLD_OPEN

def test_verdict_aggregation_reject_missing_handoff(service, valid_criteria, valid_handoffs):
    criteria_pass = service.evaluate_closure_criteria(**valid_criteria)
    # Remove one domain
    handoffs_input = [h for h in valid_handoffs if h["domain"] != "execution"]
    handoffs = service.build_handoff_matrix("pkg_1", handoffs_input)
    backlog = []

    verdict = service.compute_program_closure_verdict(criteria_pass, handoffs, backlog)
    assert verdict == ProgramClosureVerdict.REJECT_CLOSE

@patch.object(ATRProgramClosureService, '_save_package')
def test_build_program_closure_package(mock_save, service, valid_criteria, valid_handoffs):
    package = service.build_program_closure_package(
        "pkg_test_1",
        "1.0.0",
        "v1.0-scope",
        valid_criteria,
        valid_handoffs,
        []
    )

    assert package["verdict"] == ProgramClosureVerdict.PROGRAM_CLOSED
    assert package["status"] == "ready"
    mock_save.assert_called_once()
