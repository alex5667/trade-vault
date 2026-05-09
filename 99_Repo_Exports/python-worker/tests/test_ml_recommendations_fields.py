from unittest.mock import MagicMock

from services.ml_confirm_gate import MLConfirmDecision
from services.orderflow.decision_record_v1 import build_decision_record_v1


def test_ml_confirm_decision_fields():
    """Verify that MLConfirmDecision dataclass has the new expert recommendation fields."""
    d = MLConfirmDecision(
        p_edge=0.6,
        p_min=0.4,
        score=0.2,
        exec_risk_ref_bps=1.5,
        exec_risk_bps=2.0,
        exec_risk_norm=0.75,
        exec_pen=0.1,
        score_breakdown_small={"risk": -0.1, "base": 0.3},
        score_breakdown_json='{"risk": -0.1, "base": 0.3}',
        latency_us=5000,
        cfg_key_used="test_cfg"
    )

    # Check fields
    assert d.exec_risk_ref_bps == 1.5
    assert d.exec_risk_bps == 2.0
    assert d.exec_risk_norm == 0.75
    assert d.exec_pen == 0.1
    assert d.score_breakdown_small == {"risk": -0.1, "base": 0.3}
    assert d.score_breakdown_json == '{"risk": -0.1, "base": 0.3}'

    # Check to_dict
    data = d.to_dict()
    assert data["exec_risk_ref_bps"] == 1.5
    assert data["exec_risk_bps"] == 2.0
    assert data["exec_risk_norm"] == 0.75
    assert data["exec_pen"] == 0.1
    assert data["score_breakdown_small"] == {"risk": -0.1, "base": 0.3}

def test_decision_record_v1_ml_integration():
    """Verify that build_decision_record_v1 correctly extracts new ML fields."""
    runtime = MagicMock()
    runtime.l3_stats = None

    # Mock indicators with nested ML decision structure expected by build_decision_record_v1
    ml_decision = {
        "exec_risk_ref_bps": 1.2,
        "exec_risk_norm": 0.6,
        "exec_pen": 0.05,
        "score_breakdown_small": {"exec": -0.05, "base": 0.25},
        "p_edge": 0.55,
        "score": 0.2
    }

    indicators = {
        "tick_ts": 1234567890000,
        "price": 50000.0,
        "of_confirm": {
            "evidence": {
                "exec_risk_bps": 2.5,
                "ml": ml_decision
            }
        }
    }

    signal = {
        "sid": "test_sid",
        "symbol": "BTCUSDT",
        "direction": "BUY",
        "indicators": indicators
    }

    record = build_decision_record_v1(
        runtime=runtime,
        signal=signal,
        stage="final",
        final_actual="emit"
    )

    # Verify ML section
    assert record["ml"]["exec_risk_ref_bps"] == 1.2
    assert record["ml"]["exec_risk_norm"] == 0.6
    assert record["ml"]["exec_pen"] == 0.05
    assert record["ml"]["score_breakdown_small"] == {"exec": -0.05, "base": 0.25}

    # Verify inputs section
    assert record["inputs"]["exec_risk_bps"] == 2.5

def test_trade_intensity_indicator():
    """Verify that trade_intensity is correctly added to indicators."""
    from services.orderflow.strategy import OrderFlowStrategy

    # Mock dependencies for Strategy
    strategy = OrderFlowStrategy(
        redis=MagicMock(),
        ticks=MagicMock(),
        publisher=MagicMock(),
        of_engine=MagicMock()
    )
    runtime = MagicMock()
    runtime.l3_stats = MagicMock()
    runtime.l3_stats.taker_buy_rate_ema = 100.0
    runtime.l3_stats.taker_sell_rate_ema = 50.0

    indicators = {}
    strategy.add_trade_intensity(runtime, indicators)

    assert indicators["trade_intensity"] == 150.0
