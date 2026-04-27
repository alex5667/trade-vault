from orderflow_services.vertex_cost_accounting_v1 import estimate_cost_usd


def test_estimate_cost_increases_with_io():
    a = estimate_cost_usd(model_name="gemini-2.5-flash-lite", input_chars=1000, output_chars=100)
    b = estimate_cost_usd(model_name="gemini-2.5-flash-lite", input_chars=2000, output_chars=500)
    assert b > a
