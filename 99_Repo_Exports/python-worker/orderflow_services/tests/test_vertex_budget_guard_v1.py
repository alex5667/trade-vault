from orderflow_services.vertex_budget_guard_v1 import estimate_vertex_triage_cost_usd


def test_estimate_cost_is_positive_for_nonempty_io():
    cost = estimate_vertex_triage_cost_usd(10000, 2000)
    assert cost > 0.0

