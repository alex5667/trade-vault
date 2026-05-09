def test_edge_cost_gate_session_from_ts_ms_is_canonical():
    """
    Regression guard:
      - edge_cost_gate MUST NOT have its own session_from_ts_ms implementation.
      - It must re-export domain.time_utils.session_from_ts_ms.
    If someone re-introduces a local def session_from_ts_ms, this test will fail.
    """
    from domain import time_utils
    from handlers.crypto_orderflow.utils import edge_cost_gate

    assert edge_cost_gate.session_from_ts_ms is time_utils.session_from_ts_ms
