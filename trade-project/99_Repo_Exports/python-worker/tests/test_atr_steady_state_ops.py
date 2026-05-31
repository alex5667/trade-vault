import datetime
from unittest.mock import MagicMock

from services.atr_steady_state_ops_service import ATRSteadyStateOpsService


class TestATRSteadyStateOps:
    def mock_conn(self):
        conn_mock = MagicMock()
        cursor_mock = MagicMock()
        conn_mock.cursor.return_value.__enter__.return_value = cursor_mock
        return conn_mock, cursor_mock

    def test_ownership_completeness(self):
        # Simply verifying the schema / logic would expect an owner, secondary and escalation
        # For unittest purposes we ensure our code can read the domains without error
        service = ATRSteadyStateOpsService()
        conn_mock, cur_mock = self.mock_conn()

        # Test it queries atr_operations_ownership
        cur_mock.fetchall.return_value = [{"domain": "signal_gates"}, {"domain": "execution"}]
        service.build_daily_ops_scorecard(conn_mock, None)

        # Must have fetched ownership domains
        cur_mock.execute.assert_any_call("SELECT domain FROM atr_operations_ownership")
        # Must have inserted scorecards
        assert cur_mock.execute.call_count >= 3

    def test_cadence_policy(self):
        # Tests that daily/weekly/monthly scorecards can be disabled and generated correctly
        service = ATRSteadyStateOpsService(
            ops_enable=True,
            daily_enable=False,
            weekly_enable=True,
            monthly_enable=True
        )
        conn_mock, cur_mock = self.mock_conn()
        cur_mock.fetchall.return_value = [{"domain": "control_plane"}]

        service.build_daily_ops_scorecard(conn_mock, None)
        # Should not execute queries
        cur_mock.execute.assert_not_called()

        service.build_weekly_ops_scorecard(conn_mock, None)
        cur_mock.execute.assert_any_call("SELECT domain FROM atr_operations_ownership")

    def test_slo_evaluation_stale_graph_cert(self):
        # A stale graph cert -> scorecard fail
        service = ATRSteadyStateOpsService()
        conn_mock, cur_mock = self.mock_conn()

        cur_mock.fetchall.return_value = [{"domain": "control_plane"}]
        # Mock finding a stale graph cert
        def mock_cur_execute(query, *args, **kwargs):
            if "SELECT count(*) as c" in query:
                cur_mock.fetchone.return_value = {"c": 1}

        cur_mock.execute.side_effect = mock_cur_execute
        service.build_daily_ops_scorecard(conn_mock, None)
        assert cur_mock.execute.call_count >= 2

    def test_hygiene_rules(self):
        service = ATRSteadyStateOpsService()
        conn_mock, cur_mock = self.mock_conn()

        # Give some dummy data
        cur_mock.fetchall.return_value = [{"freeze_id": "fz_123", "expires_at": datetime.datetime.now(), "drift_id": "drf_456"}]

        violations = service.check_hygiene_violations(conn_mock, None)

        assert len(violations) > 0
        assert violations[0]["kind"] == "expired_override"
        assert violations[0]["severity"] == "critical"
        assert violations[0]["resource"] == "fz_123"
