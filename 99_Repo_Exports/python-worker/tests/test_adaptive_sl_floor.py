import os
from types import SimpleNamespace
from signals.risk_levels import compute_sl_floor_bps

def test_floor_basic():
    # Setup test environment
    os.environ["SL_FLOOR_DEFAULT_BPS"] = "25.0"
    os.environ["SL_FLOOR_SPREAD_MULT"] = "2.0"
    os.environ["SL_FLOOR_SLIPPAGE_MULT"] = "1.5"
    os.environ["SL_FLOOR_ATR_MULT"] = "0.25"
    os.environ["SL_FLOOR_BPS__BTCUSDT"] = "15.0"

    cfg = {
        "spread_bps": 2.0,
        "slippage_ema_bps": 3.0,
    }

    # 1. basic dynamic calculation
    # spread_bps * 2.0 = 4.0
    # slippage_ema_bps * 1.5 = 4.5
    # atr_pct_bps * 0.25 = (100 / 10000) * 10000 * 0.25 = 100 * 0.25 = 25.0
    # Expected max(15.0 fixed, 4.0, 4.5, 25.0) = 25.0
    
    floor = compute_sl_floor_bps("BTCUSDT", entry=10000.0, atr=100.0, cfg=cfg)
    assert floor >= 15.0
    assert floor == 25.0

    # 2. test fallback to default fixed floor when no symbol override
    # default fixed = 25.0
    # Dynamic = 25.0
    floor2 = compute_sl_floor_bps("UNKNOWNSYMBOL", entry=10000.0, atr=10.0, cfg=cfg)
    assert floor2 == 25.0
    
    # 3. test spread dominating
    cfg_spread = {"spread_bps": 20.0, "slippage_ema_bps": 1.0}
    # dynamic: 20 * 2 = 40.0
    floor3 = compute_sl_floor_bps("BTCUSDT", entry=10000.0, atr=10.0, cfg=cfg_spread)
    assert floor3 == 40.0

    # 4. test slippage dominating
    cfg_slip = {"spread_bps": 1.0, "slippage_ema_bps": 30.0}
    # dynamic: 30 * 1.5 = 45.0
    floor4 = compute_sl_floor_bps("BTCUSDT", entry=10000.0, atr=10.0, cfg=cfg_slip)
    assert floor4 == 45.0
    
    # 5. test ATR dominating
    # atr_pct = 500 / 10000 * 10000 = 500
    # 500 * 0.25 = 125.0
    floor5 = compute_sl_floor_bps("BTCUSDT", entry=10000.0, atr=500.0, cfg=cfg)
    assert floor5 == 125.0
