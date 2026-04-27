import pytest
from datetime import datetime, timezone, timedelta
import json
from unittest.mock import MagicMock

from services.atr_weekly_operating_scorecard_service import ATRWeeklyScorecardService, DOMAINS

@pytest.fixture
def service():
    return ATRWeeklyScorecardService(enable=True, enforce=True)

# ---------------------------------------------------------
# Unit tests
# ---------------------------------------------------------

def test_domain_scoring(service):
    # graph cert fail => control-plane graph RED
    metrics_cpg = {"graph_consistency_cert": "failed"}
    assert service.derive_domain_status("control_plane_graph", metrics_cpg) == "RED"

    # protective critical drift => protective RED
    metrics_pl = {"protective_critical_drifts": 1}
    assert service.derive_domain_status("protective_lifecycle", metrics_pl) == "RED"
    
    metrics_pl_ok = {"protective_critical_drifts": 0, "be_before_tp1_violations": 0}
    assert service.derive_domain_status("protective_lifecycle", metrics_pl_ok) == "GREEN"

    # overdue P1 => audit hygiene YELLOW/RED per policy
    metrics_ah_yellow = {"overdue_actions_p1": 1}
    assert service.derive_domain_status("audit_hygiene", metrics_ah_yellow) == "YELLOW"
    
    metrics_ah_red = {"expired_overrides_active": 1}
    assert service.derive_domain_status("audit_hygiene", metrics_ah_red) == "RED"

def test_decision_proposal(service):
    # all green => GO
    statuses_all_green = {d: "GREEN" for d in DOMAINS}
    assert service.propose_weekly_decision(statuses_all_green, {}) == "GO"

    # one yellow => GO_WITH_CONSTRAINTS
    statuses_one_yellow = {d: "GREEN" for d in DOMAINS}
    statuses_one_yellow["execution"] = "YELLOW"
    assert service.propose_weekly_decision(statuses_one_yellow, {}) == "GO_WITH_CONSTRAINTS"

    # critical drift => HOLD
    domain_metrics = {
        "dispatch_runtime": {"runtime_critical_drifts": 1}
    }
    assert service.propose_weekly_decision({"dispatch_runtime": "RED"}, domain_metrics) == "HOLD"

    cpg_metrics = {
        "control_plane_graph": {"graph_consistency_cert": "failed"}
    }
    assert service.propose_weekly_decision({"control_plane_graph": "RED"}, cpg_metrics) == "HOLD"

    # repeated runtime/protective failures => FREEZE_ESCALATION
    pl_metrics = {
        "protective_lifecycle": {"protective_critical_drifts": 1}
    }
    assert service.propose_weekly_decision({"protective_lifecycle": "RED"}, pl_metrics) == "FREEZE_ESCALATION"

def test_action_item_generation(service):
    # P0/P1 items generated for RED domains
    statuses = {d: "GREEN" for d in DOMAINS}
    statuses["audit_hygiene"] = "RED"
    
    domain_metrics = {
        "audit_hygiene": {"expired_overrides_active": 1}
    }
    
    actions = service.suggest_action_items("wk_test", statuses, domain_metrics)
    assert len(actions) == 1
    assert actions[0]["domain"] == "audit_hygiene"
    assert actions[0]["priority"] == "P1"
    assert actions[0]["status"] == "open"

# ---------------------------------------------------------
# Integration test mocking the DB
# ---------------------------------------------------------

class MockCursor:
    def __init__(self):
        self.queries = []
    def execute(self, query, vars=None):
        self.queries.append((query, vars))
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class MockConnection:
    def __init__(self):
        self.mock_cursor = MockCursor()
        self.commits = 0
    def cursor(self):
        return self.mock_cursor
    def commit(self):
        self.commits += 1

def test_integration_weekly_builder(service):
    conn = MockConnection()
    r = MagicMock()
    
    week_start = datetime.now(timezone.utc)
    week_end = week_start + timedelta(days=6)
    
    # 1. Provide custom metrics with some conditions
    custom_metrics = {
        d: service._get_metrics_for_domain(d, None, None) for d in DOMAINS
    }
    # Injec one execution yellow condition and one audit hygiene overdue P1
    custom_metrics["execution"]["mt5_requotes_total"] = 16
    custom_metrics["audit_hygiene"]["overdue_actions_p1"] = 1
    
    scorecard_id = service.build_weekly_scorecard(conn, r, week_start, week_end, custom_metrics)
    
    assert scorecard_id is not None
    assert conn.commits == 1
    
    # Check that GO_WITH_CONSTRAINTS was decided
    inserts = conn.mock_cursor.queries
    scorecard_insert = [q for q in inserts if "INSERT INTO atr_weekly_operating_scorecards" in q[0]]
    assert len(scorecard_insert) == 1
    query, vars = scorecard_insert[0]
    # vars layout: scorecard_id, week_start, week_end, overall_status, domains_json, summary_json
    assert vars[3] == "GO_WITH_CONSTRAINTS"
    
    # Check action items created
    action_inserts = [q for q in inserts if "INSERT INTO atr_weekly_action_items" in q[0]]
    assert len(action_inserts) == 2 # One for execution (YELLOW), one for hygiene (YELLOW)
    
    # 4. Inject graph cert failure
    custom_metrics["control_plane_graph"]["graph_consistency_cert"] = "failed"
    conn_fail = MockConnection()
    service.build_weekly_scorecard(conn_fail, r, week_start, week_end, custom_metrics)
    
    scorecard_insert_fail = [q for q in conn_fail.mock_cursor.queries if "INSERT" in q[0] and "atr_weekly_operating_scorecards" in q[0]][0]
    assert scorecard_insert_fail[1][3] == "HOLD"
    
    # 6. Inject protective critical drift
    custom_metrics["control_plane_graph"]["graph_consistency_cert"] = "passed"
    custom_metrics["protective_lifecycle"]["protective_critical_drifts"] = 1
    conn_prot = MockConnection()
    service.build_weekly_scorecard(conn_prot, r, week_start, week_end, custom_metrics)
    
    scorecard_insert_prot = [q for q in conn_prot.mock_cursor.queries if "INSERT" in q[0] and "atr_weekly_operating_scorecards" in q[0]][0]
    assert scorecard_insert_prot[1][3] == "FREEZE_ESCALATION"
