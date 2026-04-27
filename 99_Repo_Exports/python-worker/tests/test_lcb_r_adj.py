import sys
import os
# Add python-worker to sys.path to allow importing from core
# [AUTOGRAVITY CLEANUP] sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

from core.lcb_r_adj import PenaltyCfg, compute_r_and_adj, thresholds_for, lcb

def test_r_adj_penalizes_microstructure():
    ev = {
        "pnl": 10.0,
        "risk_usd": 10.0,  # R=1.0
        "entry_spread_z": 3.0,
        "entry_pressure_sps": 0.16,  # 2x pressure_hi
        "entry_obi_age_ms": 5000,
        "entry_abs_th_unstable": 1,
        "entry_news_blocked": 1,
    }
    cfg = PenaltyCfg(pressure_hi_sps=0.08, obi_ttl_ms=5000)
    R, Radj, pen = compute_r_and_adj(ev, cfg)
    print(f"DEBUG: R={R}, Radj={Radj}, pen={pen}")
    assert abs(R - 1.0) < 1e-9
    assert Radj < R
    assert pen["pen"] > 0
    # Expected penalty: 0.03*3 (spread) + 0.05*2 (pressure) + 0.03*1 (obi) + 0.03*1 (unstable) + 0.05*1 (news)
    # 0.09 + 0.10 + 0.03 + 0.03 + 0.05 = 0.30
    assert abs(pen["pen"] - 0.30) < 1e-9
    assert abs(Radj - 0.70) < 1e-9

def test_thresholds_by_regime():
    t_thin = thresholds_for("thin", "reversal")
    t_trend = thresholds_for("trend", "continuation")
    print(f"DEBUG: thin_reversal={t_thin}, trend_continuation={t_trend}")
    assert t_thin.min_n > t_trend.min_n
    assert t_thin.z >= t_trend.z
    assert t_thin.margin >= t_trend.margin

def test_lcb_basic():
    # mean=0.2, std=0.1, n=100, z=1.28
    # se = 0.1 / sqrt(100) = 0.01
    # lcb = 0.2 - 1.28 * 0.01 = 0.2 - 0.0128 = 0.1872
    l = lcb(0.2, 0.1, 100, 1.28)
    print(f"DEBUG: lcb={l}")
    assert abs(l - 0.1872) < 1e-9

if __name__ == "__main__":
    try:
        test_r_adj_penalizes_microstructure()
        test_thresholds_by_regime()
        test_lcb_basic()
        print("✅ All R_adj unit tests passed!")
    except Exception as e:
        print(f"❌ Tests failed: {e}")
        sys.exit(1)
