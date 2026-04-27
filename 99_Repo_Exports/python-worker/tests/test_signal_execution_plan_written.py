import json
import datetime
from unittest.mock import MagicMock

from orderflow.base_handler_legacy import BaseOrderFlowHandler
from signal_exec.repository import SignalRepository
from signal_exec.models import ExecutionPlan, Side


def _make_plan() -> ExecutionPlan:
    return ExecutionPlan(
        signal_id="test_signal_123",
        symbol="BTCUSDT",
        side=Side.LONG,
        setup_type="breakout",
        ts_signal=datetime.datetime(2026, 4, 26, 12, 0, 0, tzinfo=datetime.timezone.utc),
        price_at_signal=10050.0,
        entry_zone_low=10000.0,
        entry_zone_high=10100.0,
        stop_price=9900.0,
        tp_levels=[10500.0],
        partials=[1.0],
        pos_risk_R=1.0,
        risk_usd=100.0,
        position_size=0.1,
        expiry_bars=5,
    )


def test_signal_execution_plan_written():
    # Test that _save_execution_plan delegates to repo.insert_execution_plan.
    # Use MagicMock as self to avoid constructing the full handler.
    handler = MagicMock()
    repo_mock = MagicMock(spec=SignalRepository)
    handler._execution_repo = repo_mock

    plan = _make_plan()
    BaseOrderFlowHandler._save_execution_plan(handler, plan)

    repo_mock.insert_execution_plan.assert_called_once_with(plan)


def test_signal_execution_plan_exception_is_logged():
    # If repo raises, the exception must be logged and not propagate.
    handler = MagicMock()
    repo_mock = MagicMock(spec=SignalRepository)
    repo_mock.insert_execution_plan.side_effect = RuntimeError("db down")
    handler._execution_repo = repo_mock

    plan = _make_plan()
    BaseOrderFlowHandler._save_execution_plan(handler, plan)  # must not raise

    handler.logger.exception.assert_called_once()


def test_entry_candidate_published():
    # Test _publish_entry_candidate directly (extracted helper).
    redis_mock = MagicMock()
    handler = MagicMock()
    handler.redis = redis_mock
    handler.symbol = "BTCUSDT"
    handler._get_strategy_key.return_value = "test_strategy"

    BaseOrderFlowHandler._publish_entry_candidate(
        handler, "long", "breakout", {"regime": "trend", "ab_arm": "B", "ab_group": "test"}
    )

    redis_mock.xadd.assert_called_once()
    call_args = redis_mock.xadd.call_args
    stream_name = call_args[0][0]
    assert stream_name == "stream:trade:entry_candidate"

    payload = json.loads(call_args[0][1]["payload"])
    assert payload["schema_version"] == 1
    assert payload["type"] == "entry_candidate"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["side"] == "long"
    assert payload["kind"] == "breakout"
    assert payload["regime"] == "trend"
    assert payload["ab_arm"] == "B"
    assert payload["leader"] == "test_strategy"


def test_entry_candidate_schema_version_present():
    # Regression: schema_version must always be 1 in every published payload.
    redis_mock = MagicMock()
    handler = MagicMock()
    handler.redis = redis_mock
    handler.symbol = "ETHUSDT"
    handler._get_strategy_key.return_value = "strat"

    BaseOrderFlowHandler._publish_entry_candidate(handler, "short", "fade", {})

    payload = json.loads(redis_mock.xadd.call_args[0][1]["payload"])
    assert payload.get("schema_version") == 1, "schema_version must be 1 in entry_candidate payload"
