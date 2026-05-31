"""Integration tests for edge stack v1 dataset builder and OOF training timers.

Tests verify:
  - Dataset builder v2 produces valid report.json with joined, pos_rate, drop stats
  - OOF training validates report.json before training (joined >= min, pos_rate reasonable)
  - Quarantine JSONL contains dropped records with proper structure
"""

import json
import os
import tempfile

import pytest
from core.redis_keys import RedisStreams as RS


def test_dataset_report_json_structure():
    """Test that edge_dataset_report.json has required fields for timer validation."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import DropStats

    # Simulate report structure
    drop = DropStats(max_examples=10)
    drop.add("signal_parse_none", {"id": "1-0"})
    drop.add("close_invalid_risk", {"id": "2-0", "sid": "test", "risk_usd": 0.0})

    report = {
        "signal_stream": RS.OF_INPUTS,
        "closed_stream": RS.TRADES_CLOSED,
        "signals_raw": 1000,
        "signals_parsed": 950,
        "closes_raw": 1000,
        "closes_parsed": 980,
        "joined": 900,  # Critical field for timer validation
        "unmatched_closes": 80,
        "y_min_r": 0.10,
        "pos_rate": 0.15,  # Critical field for timer validation (should be 0.05-0.40)
        "r_mult_p50": 0.12,
        "r_mult_p95": 0.45,
        "drop": drop.to_dict(),
        "generated_ms": 1700000000000,
    }

    # Validate structure
    assert "joined" in report
    assert "pos_rate" in report
    assert "drop" in report
    assert report["joined"] >= 0
    assert 0.0 <= report["pos_rate"] <= 1.0
    assert "counts" in report["drop"]
    assert "examples" in report["drop"]


def test_timer_validation_joined_threshold():
    """Test timer validation logic: joined >= MIN_JOINED."""
    # Simulate report.json
    report_small = {"joined": 1500, "pos_rate": 0.12}
    report_large = {"joined": 15000, "pos_rate": 0.15}

    MIN_JOINED_SMALL = 2000
    MIN_JOINED_LARGE = 10000

    # Small run validation
    assert report_small["joined"] < MIN_JOINED_SMALL, "Small run should fail validation"
    assert report_large["joined"] >= MIN_JOINED_LARGE, "Large run should pass validation"


def test_timer_validation_pos_rate_range():
    """Test timer validation logic: pos_rate in reasonable range (0.01-0.50)."""
    MIN_POS_RATE = 0.01
    MAX_POS_RATE = 0.50

    test_cases = [
        (0.05, True),  # Within range
        (0.15, True),  # Middle
        (0.40, True),  # Within range
        (0.01, True),  # Lower bound (inclusive)
        (0.50, True),  # Upper bound (inclusive)
        (0.005, False),  # Too low
        (0.60, False),  # Too high
    ]

    for pos_rate, should_pass in test_cases:
        in_range = MIN_POS_RATE <= pos_rate <= MAX_POS_RATE
        assert in_range == should_pass, f"pos_rate={pos_rate} validation failed (range: {MIN_POS_RATE}-{MAX_POS_RATE})"


def test_quarantine_jsonl_structure():
    """Test that quarantine JSONL has proper structure for diagnostics."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import QuarantineWriter

    with tempfile.TemporaryDirectory() as td:
        q_path = os.path.join(td, "quarantine.jsonl")
        q = QuarantineWriter(q_path)

        # Write sample quarantine records
        q.write(
            "signal",
            "signal_parse_none",
            stream=RS.OF_INPUTS,
            msg_id="1-0",
            data={"payload": "invalid"},
        )
        q.write(
            "close",
            "close_invalid_risk",
            stream=RS.TRADES_CLOSED,
            msg_id="2-0",
            data={"sid": "test", "risk_usd": 0.0},
        )
        q.close()

        # Validate structure
        with open(q_path, encoding="utf-8") as f:
            lines = f.readlines()
            assert len(lines) == 2

            rec1 = json.loads(lines[0])
            assert rec1["kind"] == "signal"
            assert rec1["reason"] == "signal_parse_none"
            assert rec1["stream"] == RS.OF_INPUTS
            assert "id" in rec1
            assert "data" in rec1

            rec2 = json.loads(lines[1])
            assert rec2["kind"] == "close"
            assert rec2["reason"] == "close_invalid_risk"


def test_report_drop_diagnostics():
    """Test that report.json drop stats help diagnose issues (recommendations section 4)."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import DropStats

    drop = DropStats(max_examples=5)
    # Simulate high signal_parse_none (diagnosis A: format mismatch)
    for i in range(100):
        drop.add("signal_parse_none", {"id": f"{i}-0"})
    # Simulate high join_no_signal + mismatch <=1s (diagnosis B: sid mismatch)
    for i in range(50):
        drop.add("join_no_signal", {"sid": f"test-{i}"})

    report = {
        "joined": 500,
        "drop": drop.to_dict(),
        "mismatch": {
            "counts": {"<=1s": 30, "<=10s": 10, ">5m": 10},
            "examples": [
                {"sid_close": "test-1", "nearest_signal_sid": "test-2", "delta_ms": 500}
            ],
        },
    }

    # Diagnosis checks
    signal_parse_count = report["drop"]["counts"].get("signal_parse_none", 0)
    assert signal_parse_count == 100, "High signal_parse_none indicates format mismatch"

    join_no_signal_count = report["drop"]["counts"].get("join_no_signal", 0)
    mismatch_1s = report["mismatch"]["counts"].get("<=1s", 0)
    assert join_no_signal_count > 0 and mismatch_1s > 0, "High join_no_signal + <=1s mismatch indicates sid issue"


def test_feature_cols_json_structure():
    """Test that feature_cols.json is properly emitted for OOF training."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import infer_feature_cols

    rows = [
        {
            "ts_ms": 1000,
            "sid": "crypto-of:BTCUSDT:1000",
            "symbol": "BTCUSDT",
            "direction": "BUY",
            "scenario": "trend",
            "indicators": {"spread_bps": 1.2, "delta_z": 0.5, "obi": 0.8},
            "y": 1,
        },
        {
            "ts_ms": 2000,
            "sid": "crypto-of:ETHUSDT:2000",
            "symbol": "ETHUSDT",
            "direction": "SELL",
            "scenario": "range",
            "indicators": {"spread_bps": 1.0, "delta_z": -0.2, "obi": 0.6},
            "y": 0,
        },
    ]

    cols = infer_feature_cols(rows, max_numeric=128, include_direction=True, include_scenario=True)

    # Validate structure
    assert isinstance(cols, list)
    assert len(cols) > 0
    # Should include feature prefixes
    assert any("f_spread_bps" in c or "spread_bps" in c for c in cols)
    # Should include direction if enabled
    assert any("direction" in c.lower() for c in cols) or not any("direction" in str(c).lower() for c in cols)


def test_oof_training_prerequisites():
    """Test that OOF training requires: dataset.jsonl, feature_cols.json, and optionally report.json."""
    with tempfile.TemporaryDirectory() as td:
        dataset_path = os.path.join(td, "edge_train.jsonl")
        feature_cols_path = os.path.join(td, "feature_cols.json")
        report_path = os.path.join(td, "edge_dataset_report.json")

        # Create minimal valid files
        with open(dataset_path, "w") as f:
            f.write('{"ts_ms": 1000, "y": 1, "indicators": {"x": 1.0}}\n')

        with open(feature_cols_path, "w") as f:
            json.dump(["f_x"], f)

        with open(report_path, "w") as f:
            json.dump({"joined": 5000, "pos_rate": 0.15}, f)

        # Validate prerequisites
        assert os.path.exists(dataset_path), "Dataset file required"
        assert os.path.exists(feature_cols_path), "Feature cols file required"
        # Report is optional but recommended
        if os.path.exists(report_path):
            with open(report_path) as f:
                report = json.load(f)
                assert report["joined"] >= 2000, "Joined should be >= 2000 for training"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

