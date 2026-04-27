import pytest
from ml_analysis.reality_check import evaluate_strategy

def test_evaluate_strategy():
    # Provide a mini dummy dataset
    dataset = [
        {"return": 0.05, "cost_bps": 10, "score": 0.9},
        {"return": -0.01, "cost_bps": 10, "score": 0.4},
        {"return": 0.02, "cost_bps": 20, "score": 0.8},
    ]
    
    metrics = evaluate_strategy(dataset)
    
    assert "net_expectancy" in metrics
    assert "precision_at_top_x" in metrics
    assert "mean_r" in metrics
    assert "downside_adjusted_return" in metrics
    assert "hit_rate_conditioned_on_cost" in metrics
    assert "avg_cost_bps" in metrics
    
    # Net expectancy should be around ( (0.05 - 0.001) + (-0.01 - 0.001) + (0.02 - 0.002) ) / 3
    # = (0.049 - 0.011 + 0.018) / 3 = 0.056 / 3 = 0.018666...
    assert abs(metrics["net_expectancy"] - 0.01866) < 0.0001
    
    # Empty dataset fallback
    empty_metrics = evaluate_strategy([])
    assert empty_metrics["net_expectancy"] == 0.0
