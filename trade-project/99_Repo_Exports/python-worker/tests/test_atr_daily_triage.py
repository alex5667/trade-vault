import unittest
from datetime import UTC, datetime, timedelta


# Mock the database connection
class MockCursor:
    def __init__(self):
        self.queries = []
    def execute(self, query, params=None):
        self.queries.append((query, params))
    def fetchone(self):
        return None
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class MockConnection:
    def __init__(self):
        self.committed = False
        self.rolled_back = False
        self.cur = MockCursor()
    def cursor(self):
        return self.cur
    def commit(self):
        self.committed = True
    def rollback(self):
        self.rolled_back = True

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.atr_daily_triage_service import ATRDailyTriageService


class TestATRDailyTriageService(unittest.TestCase):
    def setUp(self):
        self.service = ATRDailyTriageService(enable=True, enforce=False)
        # Mock logging to avoid clutter
        import logging
        logging.getLogger('atr_daily_triage_service').setLevel(logging.CRITICAL)

    def test_section_scoring_dq_spike(self):
        metrics = {
            "book_stale": 51,
            "negative_ev": 10
        }
        status = self.service.derive_section_status("signal_gates", {"veto_top": metrics})
        self.assertEqual(status, "RED")

    def test_section_scoring_mt5_requote(self):
        status = self.service.derive_section_status("execution", {"mt5_requotes_total": 12, "connection_bursts": 0})
        self.assertEqual(status, "RED")

        status_burst = self.service.derive_section_status("execution", {"mt5_requotes_total": 0, "connection_bursts": 5})
        self.assertEqual(status_burst, "BLACK")

    def test_section_scoring_protective_mismatch(self):
        status = self.service.derive_section_status("protective", {"sl_ratchet_backwards": 1})
        self.assertEqual(status, "BLACK")

    def test_section_scoring_stale_graph_cert(self):
        status = self.service.derive_section_status("control_plane", {"graph_cert_status": "failed"})
        self.assertEqual(status, "BLACK")

    def test_decision_proposal_one_red(self):
        section_statuses = {"signal_gates": "RED", "dispatch_runtime": "GREEN", "execution": "YELLOW", "protective": "GREEN", "control_plane": "GREEN"}
        decisions = self.service.propose_daily_decision(section_statuses, {})
        self.assertIn("SAME_DAY_FIX", decisions)
        self.assertEqual(len(decisions), 1)

    def test_decision_proposal_one_black(self):
        section_statuses = {"signal_gates": "GREEN", "dispatch_runtime": "GREEN", "execution": "BLACK", "protective": "GREEN", "control_plane": "GREEN"}
        decisions = self.service.propose_daily_decision(section_statuses, {})
        self.assertIn("INCIDENT_OPEN", decisions)

    def test_decision_proposal_control_plane_repeated(self):
        section_statuses = {"control_plane": "RED"}
        decisions = self.service.propose_daily_decision(section_statuses, {})
        self.assertIn("FREEZE_RELEASES", decisions)

    def test_action_generation_black(self):
        section_statuses = {"protective": "BLACK"}
        board_id = "test_board_1"
        actions = self.service.suggest_daily_actions(board_id, section_statuses, {})
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["priority"], "P0")
        self.assertEqual(actions[0]["owner"], "protective_owner")
        self.assertEqual(actions[0]["reason_code"], "protective_invariant_violation")
        # due within ~1 hour
        expected_due = datetime.now(UTC) + timedelta(hours=1)
        # allow some seconds drift
        self.assertTrue(expected_due - timedelta(seconds=10) < actions[0]["due_at"] < expected_due + timedelta(seconds=10))

    def test_action_generation_red(self):
        section_statuses = {"execution": "RED"}
        board_id = "test_board_2"
        actions = self.service.suggest_daily_actions(board_id, section_statuses, {})
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["priority"], "P1")
        self.assertEqual(actions[0]["owner"], "execution_owner")

    def test_board_builder_integration(self):
        # Override Telegram emit to avoid prints
        self.service.emit_telegram_digest = lambda *args: None

        conn = MockConnection()
        r = None # mock redis

        # Seed healthy dispatch, protective, but MT5 error
        custom_metrics = {
             "dispatch_runtime": {"runtime_critical_drifts": 0, "order_queue_publish_ok_rate": 1.0},
             "dispatch_runtime_status": "GREEN",
             "protective": {"be_before_tp1": 0, "sl_ratchet_backwards": 0, "unresolved_protective_drifts": 0},
             "protective_status": "GREEN",
             "execution": {"mt5_requotes_total": 0, "connection_bursts": 3},
             "execution_status": "RED",
             "control_plane": {"open_overrides": 1},
             "control_plane_status": "YELLOW",
             "signal_gates": {},
             "signal_gates_status": "GREEN"
        }

        day_start = datetime.now(UTC)
        board_data = self.service.build_daily_triage_board(conn, r, day_start, custom_metrics=custom_metrics)

        self.assertEqual(board_data["overall_status"], "RED")
        self.assertEqual(board_data["summary"]["primary_decision"], "SAME_DAY_FIX")
        self.assertTrue(conn.committed)

        # Inject protective black
        custom_metrics["protective_status"] = "BLACK"
        custom_metrics["protective"] = {"sl_ratchet_backwards": 1}
        board_data_black = self.service.build_daily_triage_board(conn, r, day_start, custom_metrics=custom_metrics)

        self.assertEqual(board_data_black["overall_status"], "BLACK")
        self.assertIn("INCIDENT_OPEN", board_data_black["summary"]["all_decisions"])
        self.assertIn("FREEZE_SCOPE", board_data_black["summary"]["all_decisions"])

if __name__ == '__main__':
    unittest.main()
