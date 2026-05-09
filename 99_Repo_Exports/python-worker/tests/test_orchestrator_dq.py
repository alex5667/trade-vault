from unittest.mock import patch

from handlers.crypto_orderflow.pipeline.orchestrator import _emit_dq_flag


class DummyCtx:
    def __init__(self):
        self.dq_flags = set()
        self.symbol = "BTCUSDT"

@patch("handlers.crypto_orderflow.pipeline.orchestrator._DQ_FLAG_TOTAL")
def test_emit_dq_flag_dedup(mock_metrics):
    ctx = DummyCtx()

    # First emit -> metric increased
    _emit_dq_flag(ctx, "test_flag", symbol="BTCUSDT")
    from common.dq_flags import ensure_dq_flags
    assert "test_flag" in ensure_dq_flags(ctx)
    mock_metrics.labels.assert_called_with(flag="test_flag", symbol="BTCUSDT")
    mock_metrics.labels.return_value.inc.assert_called_once()

    mock_metrics.labels.return_value.inc.reset_mock()

    # Second emit of same flag -> no metric inc
    _emit_dq_flag(ctx, "test_flag", symbol="BTCUSDT")
    mock_metrics.labels.return_value.inc.assert_not_called()

@patch("handlers.crypto_orderflow.pipeline.orchestrator._DQ_FLAG_TOTAL")
def test_emit_dq_flag_fail_open(mock_metrics):
    # Metric raises exception, but execution shouldn't fail
    mock_metrics.labels.side_effect = Exception("Prometheus error")
    ctx = DummyCtx()
    _emit_dq_flag(ctx, "test_flag_2", symbol="ETHUSDT")
    from common.dq_flags import ensure_dq_flags
    assert "test_flag_2" in ensure_dq_flags(ctx)
