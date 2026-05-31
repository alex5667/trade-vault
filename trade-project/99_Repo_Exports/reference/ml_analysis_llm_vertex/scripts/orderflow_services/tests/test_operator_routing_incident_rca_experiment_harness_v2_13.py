import pytest
import asyncio

from orderflow_services.operator_routing_incident_rca_experiment_router_v2_13 import select_bucket
from orderflow_services.operator_routing_incident_rca_experiment_winner_selector_v2_13 import (
    calculate_combined_score,
    select_winner,
)

def test_select_bucket():
    # Hash behavior depends on env, but we can verify it returns one of the two
    b1 = select_bucket("route_1")
    b2 = select_bucket("route_2")
    assert b1 in ["control", "challenger"]
    assert b2 in ["control", "challenger"]

def test_calculate_combined_score():
    # q=0.5, u=1.0 => 0.5*0.4 + 1.0*0.6 = 0.2 + 0.6 = 0.8
    s = calculate_combined_score(0.5, 1.0)
    assert abs(s - 0.8) < 1e-5

def test_select_winner():
    stats = [
        {"bucket": "control", "sample_n": 10, "avg_quality": 0.5, "avg_usefulness": 0.5},
        {"bucket": "challenger", "sample_n": 10, "avg_quality": 0.8, "avg_usefulness": 0.8},
    ]
    # Combined:
    # control = 0.5*0.4 + 0.5*0.6 = 0.5
    # challenger = 0.8*0.4 + 0.8*0.6 = 0.8
    winner, score = select_winner(stats)
    assert winner == "challenger"
    assert score > 0.79

    # Test MIN_SAMPLE
    stats[1]["sample_n"] = 2
    # challenger has too few samples now
    winner2, score2 = select_winner(stats)
    assert winner2 == "control"
