from core.expected_slippage_decomp_v1 import expected_slippage_decomp_bps


def test_slippage_decomp_disabled():
    cfg = {"slippage_decomp_enable": 0}
    res = expected_slippage_decomp_bps(spread_bps=10.0, impact_proxy=1.0, cfg=cfg)
    assert res.total_bps == 0.0

def test_slippage_decomp_basic():
    cfg = {
        "slippage_decomp_enable": 1,
        "slippage_decomp_half_spread_mult": 0.5,
        "slippage_decomp_impact_coeff_bps": 10.0,
        "slippage_decomp_size_ref_usd": 1000.0,
        "slippage_decomp_size_power": 1.0,
        "slippage_decomp_cap_bps": 100.0
    }
    # order size = 2000 (ratio = 2), impact = 1.0
    res = expected_slippage_decomp_bps(spread_bps=10.0, impact_proxy=1.0, cfg=cfg, order_size_usd=2000.0)
    assert res.spread_bps == 5.0
    assert res.impact_bps == 20.0  # 10.0 * 1.0 * 2
    assert res.total_bps == 25.0

def test_slippage_decomp_cap():
    cfg = {
        "slippage_decomp_enable": 1,
        "slippage_decomp_cap_bps": 20.0
    }
    res = expected_slippage_decomp_bps(spread_bps=50.0, impact_proxy=1.0, cfg=cfg)
    # spread = 25.0 -> min(25 + ..., 20.0)
    assert res.total_bps == 20.0
