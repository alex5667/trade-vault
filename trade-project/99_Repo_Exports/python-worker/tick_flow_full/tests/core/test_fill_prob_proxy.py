import pytest

from core.fill_prob_proxy import compute_fill_prob_proxy


def test_compute_fill_prob_proxy():
    res = compute_fill_prob_proxy(
        direction="LONG",
        cancel_to_trade_bid=0.2,
        cancel_to_trade_ask=0.5,
        eta_fill_bid_sec=0.5,
        eta_fill_ask_sec=1.2,
        max_wait_s=2.0
    )

    # For LONG direction: implementation uses BID-side cancel + BID-side ETA
    # (buyer posts on bid side; taker sell fills it)
    assert "fill_prob_proxy" in res
    assert "eta_fill_sec" in res
    # LONG → bid side → eta_fill_bid_sec = 0.5
    assert res["eta_fill_sec"] == pytest.approx(0.5, abs=1e-9)

    # Basic bounds check on probability
    assert 0.0 <= res["fill_prob_proxy"] <= 1.0
    assert 0.0 <= res["p_base"] <= 1.0
    assert 0.0 <= res["p_wait"] <= 1.0

def test_compute_fill_prob_proxy_short():
    res = compute_fill_prob_proxy(
        direction="SHORT",
        cancel_to_trade_bid=0.8,
        cancel_to_trade_ask=0.1,
        eta_fill_bid_sec=3.0,
        eta_fill_ask_sec=0.1,
        max_wait_s=2.0
    )

    # For SHORT direction: implementation uses ASK-side cancel + ASK-side ETA
    # (seller posts on ask side; taker buy fills it)
    # SHORT → ask side → eta_fill_ask_sec = 0.1
    assert res["eta_fill_sec"] == pytest.approx(0.1, abs=1e-9)
    assert res["fill_prob_proxy"] <= 1.0  # High cancel on ask side is low → penalises less
