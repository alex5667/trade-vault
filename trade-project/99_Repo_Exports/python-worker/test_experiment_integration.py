from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Test script for experiment layer integration.

Tests the experiment assignment, filtering, and persistence.
"""

import os
import sys
import time
import json
from unittest.mock import Mock, patch
from dataclasses import dataclass, field
from typing import Dict, Any

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

from handlers.experiment_manager import ExperimentManager, ExperimentSpec
from handlers.experiment_metrics import calculate_experiment_metrics

# Simplified SignalContext for testing
@dataclass
class TestSignalContext:
    ts: int
    price: float
    z_delta: float
    weak_progress: bool
    obi: float
    obi_avg: float
    obi_sustained: bool
    atr: float
    symbol: str = "BTCUSDT"
    family: str = "orderflow"
    direction: int = 1

    # Experiment fields
    experiment_id: str | None = None
    experiment_variant: str | None = None
    experiment_config: Dict[str, Any] = field(default_factory=dict)
    filter_flags: Dict[str, bool] = field(default_factory=dict)


def test_experiment_manager():
    """Test ExperimentManager functionality"""
    import os
    os.environ["PG_DSN"] = "postgresql://user:pass@localhost:5432/db"
    print("Testing ExperimentManager...")

    # Mock database connection
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    # Mock experiment data
    mock_cursor.fetchall.return_value = [
        {
            "experiment_id": "test_exp_001",
            "filter_name": "confidence_boost",
            "signal_family": "orderflow",
            "direction": 0,
            "status": "running",
            "start_at_ms": get_ny_time_millis() - 3600000,  # 1 hour ago
            "end_at_ms": None,
            "target_metric": "expectancy_r",
            "config": {"confidence_threshold": 60.0}
        }
    ]

    with patch('psycopg2.connect', return_value=mock_conn):
        manager = ExperimentManager()

        # Test variant assignment
        exp_info = manager.assign_variant(
            now_ms=get_ny_time_millis(),
            symbol="BTCUSDT",
            signal_family="orderflow",
            direction=1,
            signal_id="test_signal_001"
        )

        assert exp_info is not None
        assert exp_info["experiment_id"] == "test_exp_001"
        assert exp_info["variant"] in ["control", "treatment"]
        assert exp_info["filter_name"] == "confidence_boost"

        print("✓ ExperimentManager test passed")


def test_signal_context_with_experiments():
    """Test SignalContext with experiment fields"""
    print("Testing SignalContext with experiment fields...")

    ctx = TestSignalContext(
        ts=get_ny_time_millis(),
        price=50000.0,
        z_delta=2.5,
        weak_progress=False,
        obi=0.8,
        obi_avg=0.6,
        obi_sustained=True,
        atr=50.0,
        symbol="BTCUSDT",
        family="orderflow",
        direction=1
    )

    # Test experiment fields
    ctx.experiment_id = "test_exp_001"
    ctx.experiment_variant = "treatment"
    ctx.experiment_config = {"confidence_threshold": 60.0}
    ctx.filter_flags = {"baseline_passed": True, "confidence_boost_passed": False}

    assert ctx.experiment_id == "test_exp_001"
    assert ctx.experiment_variant == "treatment"
    assert ctx.experiment_config["confidence_threshold"] == 60.0
    assert ctx.filter_flags["baseline_passed"] is True

    print("✓ SignalContext experiment fields test passed")


def test_metrics_calculation():
    """Test experiment metrics calculation"""
    print("Testing experiment metrics calculation...")

    # Sample R results (positive = profit, negative = loss)
    pnl_rs = [0.5, -0.2, 0.8, -0.1, 0.3, -0.4, 1.0, -0.3]  # 8 trades

    metrics = calculate_experiment_metrics(pnl_rs, success_threshold_r=0.2)

    assert metrics["signals_total"] == 8
    assert metrics["traded_total"] == 8
    assert metrics["winners_total"] == 4  # 0.5, 0.8, 0.3, 1.0 >= 0.2
    assert metrics["losers_total"] == 4   # -0.2, -0.1, -0.4, -0.3 < 0.2

    # Check expectancy (should be positive)
    assert metrics["expectancy_r"] > 0

    # Check winrate
    assert metrics["winrate"] == 0.5  # 4/8

    print("✓ Metrics calculation test passed")


def test_experiment_assignment_determinism():
    """Test that experiment assignment is deterministic for same inputs"""
    import os
    os.environ["PG_DSN"] = "postgresql://user:pass@localhost:5432/db"
    print("Testing experiment assignment determinism...")

    # Mock database connection
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    mock_cursor.fetchall.return_value = [
        {
            "experiment_id": "test_deterministic",
            "filter_name": "test_filter",
            "signal_family": "orderflow",
            "direction": 0,
            "status": "running",
            "start_at_ms": get_ny_time_millis() - 3600000,
            "end_at_ms": None,
            "target_metric": "expectancy_r",
            "config": {}
        }
    ]

    with patch('psycopg2.connect', return_value=mock_conn):
        manager = ExperimentManager()

        # Test same inputs multiple times - should get same variant
        results = []
        for i in range(10):
            exp_info = manager.assign_variant(
                now_ms=get_ny_time_millis(),
                symbol="BTCUSDT",
                signal_family="orderflow",
                direction=1,
                signal_id="deterministic_test"
            )
            results.append(exp_info["variant"])

        # All results should be the same (deterministic)
        assert all(variant == results[0] for variant in results), f"Non-deterministic results: {results}"

        print("✓ Experiment assignment determinism test passed")


def main():
    """Run all tests"""
    print("Running experiment integration tests...\n")

    try:
        test_experiment_manager()
        test_signal_context_with_experiments()
        test_metrics_calculation()
        test_experiment_assignment_determinism()

        print("\n✅ All experiment integration tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
