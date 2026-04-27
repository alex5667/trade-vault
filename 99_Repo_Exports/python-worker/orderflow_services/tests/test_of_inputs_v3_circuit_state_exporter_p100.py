import importlib


def test_import_and_key_parsing_helpers():
    m = importlib.import_module("orderflow_services.of_inputs_v3_circuit_state_exporter_p100")

    assert m._sym_from_cfg_disabled_key("cfg:of_inputs:v3_disabled:BTCUSDT") == "BTCUSDT"

    reason, sym = m._reason_sym_from_dg_key("state:of_inputs:v3_downgrades:seq_gap:ETHUSDT")
    assert (reason, sym) == ("seq_gap", "ETHUSDT")

    assert m._reason_from_ap_glob_key("cfg:of_inputs_v3:auto_apply_block_global:of_inputs_v3") == "of_inputs_v3"

    sym2, rsn2 = m._sym_reason_from_ap_sym_key("cfg:of_inputs_v3:auto_apply_block:BTCUSDT:of_inputs_v3")
    assert (sym2, rsn2) == ("BTCUSDT", "of_inputs_v3")


def test_derive_until_ms_fallbacks():
    m = importlib.import_module("orderflow_services.of_inputs_v3_circuit_state_exporter_p100")

    now = 1_700_000_000_000
    until, reason = m._derive_until_ms({"until_ms": now + 5000, "reason": "seq_gap"}, now_ms=now, pttl_ms=4000)
    assert until == now + 5000
    assert reason == "seq_gap"

    hard = m._derive_hard_until_ms({"until_ms": now + 5000, "hard_until_ms": now + 2000}, until_ms=until)
    assert hard == now + 2000

    hard2 = m._derive_hard_until_ms({"until_ms": now + 5000}, until_ms=until)
    assert hard2 == until

    # until_ms missing => derive from pttl
    until2, reason2 = m._derive_until_ms({"reason": "missing_lob_fields"}, now_ms=now, pttl_ms=10_000)
    assert until2 == now + 10_000
    assert reason2 == "missing_lob_fields"
