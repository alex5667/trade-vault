from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from core.of_confirm_engine import OFConfirmEngine
from services.ml_confirm_gate import MLConfirmDecision, MLConfirmGate


def test_score_breakdown_generation_and_enrichment():
    """Test that OFConfirmEngine produces score_breakdown and enriches indicators for ML."""
    eng = OFConfirmEngine(version=2)

    # Mock ML gate to verify indicators passed to decision
    eng._ml_gate = MagicMock()
    eng._ml_gate.decide.return_value = MLConfirmDecision(mode="SHADOW", allow=True)

    indicators = {"delta_z": 3.0} # Strong Z score
    cfg = {
        "require_strong_confirmation": False,
        "w_z": 0.3, "w_wp": 0.0, "w_reclaim": 0.0, "w_obi": 0.0, "w_ice": 0.0, "w_abs": 0.0,
        "score_z_ref": 3.0,

        # Configure exec penalty to be exactly 0.05
        # exec_risk_bps = 1.0 + 0.5 = 1.5
        # dist_bp_threshold = 30.0 => ref = 30.0
        # norm = 1.5 / 30.0 = 0.05
        # w_exec_risk = 1.0 => pen = 0.05 * 1.0 = 0.05
        "w_exec_risk": 1.0,
        "dist_bp_threshold": 30.0,
        "spread_bps_missing_default": 0.0,

        "of_score_agg": "weighted_mean",
        "ml_confirm_enabled": True
    }

    # Simple runtime stub
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=False),
        last_obi_event=None,
        last_iceberg_event=None,
        last_sweep=None,
        last_reclaim=None,
        last_div=None,
        cont_ctx_ts_ms=0,
    )

    # Input values for exec risk calculation
    indicators["spread_bps"] = 1.0
    indicators["expected_slippage_bps"] = 0.5

    ofc, _ = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=10000,
        price=100.0,
        delta_z=3.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )

    assert ofc is not None

    # 1. Verify score_breakdown in input indicators (in-place modification)
    sb = indicators.get("score_breakdown")
    assert sb is not None
    assert sb["base_score"] == 1.0
    assert sb["exec_pen"] == 0.05
    assert sb["final_score_raw"] == 0.95
    assert sb["final_score"] == 0.95

    # 2. Verify score_breakdown_small in indicators passed to ML gate
    assert eng._ml_gate.decide.called
    _, kwargs = eng._ml_gate.decide.call_args
    inds_passed = kwargs['indicators']

    assert "score_breakdown_small" in inds_passed
    sb_small = inds_passed["score_breakdown_small"]

    assert sb_small["base_score"] == 1.0
    assert sb_small["exec_pen"] == 0.05
    assert sb_small["final_score_raw"] == 0.95
    assert sb_small["final_score_01"] == 0.95

    # Verify enrichment
    assert inds_passed["of_base_score"] == 1.0
    assert inds_passed["of_score_final_raw"] == 0.95
    assert inds_passed["of_score_final"] == 0.95


def test_ml_gate_emit_metrics_rule_fields():
    """Test that MLConfirmGate._emit_metrics includes rule score breakdown."""

    # Mock Redis
    mock_redis = MagicMock()

    # Initialize with required args
    gate = MLConfirmGate(
        r=mock_redis,
        mode="SHADOW",
        fail_policy="allow",
        champion_key="test_champ",
        challenger_key="test_chall"
    )
    gate._metrics_enable = True
    gate._metrics_stream = "metrics:ml_confirm"
    gate._metrics_sample = 1.0

    # Create indicators with score_breakdown_small (as produced by OFConfirmEngine)
    indicators = {
        "sid": "test_sid",
        "score_breakdown_small": {
            "base_score": 0.8,
            "exec_pen": 0.1,
            "final_score_raw": 0.7,
            "final_score_01": 0.7,
            "raw_sum": 0.24,
            "w_sum": 0.3,
            "agg": "weighted_mean"
        }
    }

    dec = MLConfirmDecision(mode="SHADOW", allow=True)
    dec.p_edge = 0.6

    gate._emit_metrics(
        dec,
        symbol="BTCUSDT",
        ts_ms=10000,
        direction="LONG",
        scenario="trend",
        rule_score=0.7,
        rule_have=1,
        rule_need=1,
        cancel_spike_veto=0,
        ok_rule=1,
        indicators=indicators
    )

    # Verify redis call
    assert mock_redis.xadd.called
    args, kwargs = mock_redis.xadd.call_args
    stream, payload = args[0], args[1]

    assert stream == "metrics:ml_confirm"
    # Expect strings with 6 decimal places
    assert payload["rule_base_score"] == "0.800000"
    assert payload["rule_exec_pen"] == "0.100000"
    assert payload["rule_score_raw"] == "0.700000"
    assert payload["rule_score_01"] == "0.700000"
    assert payload["score_raw_sum"] == "0.240000"
    assert payload["score_w_sum"] == "0.300000"
    assert payload["score_agg"] == "weighted_mean"
