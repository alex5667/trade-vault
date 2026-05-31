from tools.replay_diff_report import compare, row_key


def test_row_key_sid():
    assert row_key({"sid": "123", "symbol": "BTC"}) == "123"
    assert row_key({"symbol": "BTC", "ts_ms": 1000, "direction": "LONG"}) == "BTC|1000|LONG"


def test_diff_analytics_nested():
    base = [
        {"sid": "1", "ok": 1, "evidence": {"scenario_v4": "SCN_A"}, "reason": "X"}
    ]
    cand = [
        {"sid": "1", "ok": 0, "evidence": {"scenario_v4": "SCN_A"}, "reason": "Y"}
    ]
    rep = compare(base_rows=base, cand_rows=cand)
    assert rep["mismatch"] == 1
    assert rep["mismatch_by_field"]["ok"] == 1
    assert rep["mismatch_by_type"]["ok:1->0"] == 1
    assert rep["mismatch_by_scenario_v4"]["SCN_A"] == 1
    assert rep["mismatch_by_reason"]["X->Y"] == 1


def test_diff_score_eps():
    base = [{"sid": "1", "score": 0.5}]
    cand = [{"sid": "1", "score": 0.5000001}]
    rep = compare(base_rows=base, cand_rows=cand, score_eps=1e-5)
    assert rep["mismatch"] == 0

    rep2 = compare(base_rows=base, cand_rows=cand, score_eps=1e-8)
    assert rep2["mismatch"] == 1
