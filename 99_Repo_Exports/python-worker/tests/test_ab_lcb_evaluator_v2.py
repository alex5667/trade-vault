
import math
from core.ab_lcb_evaluator import eval_winner_lcb, RegimeThresholds, WinnerDecision

def test_lcb_evaluator_lists():
    # Setup stats: B is better
    # A: mean 0.1, std 0.5
    # B: mean 0.2, std 0.5
    # Generate fake samples
    
    samples_A = [0.1] * 500  # simplified zero-variance for checking logic paths, 
                             # but eval requires variance? _mean_std handles it.
                             # If std is 0, LCB = mean.
    samples_B = [0.2] * 500
    
    # Introduce variance
    samples_A = [0.5, -0.3] * 250 # mean 0.1, std ~0.4, WR 50%
    # samples_B: Increase WR slightly for high Z test. 
    # [0.6, 0.6, -0.2] * 166 -> 498 samples + 2 pad
    samples_B = ([0.6, 0.6, -0.2] * 166) + [0.6, -0.2] # mean > 0.3, WR ~66%
    
    arms = {
        "A": samples_A,
        "B": samples_B,
    }

    thr_map = {
        "default": RegimeThresholds(min_n=100, min_lcb_r=0.0, min_lcb_wr=0.4, min_delta_lcb_vs_a=0.0, z=1.96),
        "thin": RegimeThresholds(min_n=100, min_lcb_r=0.0, min_lcb_wr=0.4, min_delta_lcb_vs_a=0.05, z=5.0), # Strict
    }

    # Case 1: Default
    res = eval_winner_lcb(samples_by_arm=arms, regime="default", group="g", scenario="s", thr_by_regime=thr_map)
    assert res.ok
    assert res.winner == "B"
    print(f"Case 1 OK: Winner {res.winner}")

    # Case 2: Thin (Higher Z)
    # LCB A: 0.1 - 5 * (0.4 / sqrt(500)) = 0.1 - 5 * 0.0178 = 0.1 - 0.089 = 0.011
    # LCB B: 0.2 - 5 * (0.4 / sqrt(500)) = 0.2 - 0.089 = 0.111
    # Delta: 0.1
    # Required delta: 0.05.
    # Should win.
    res2 = eval_winner_lcb(samples_by_arm=arms, regime="thin", group="g", scenario="s", thr_by_regime=thr_map)
    if not res2.ok:
        print(f"Case 2 FAILED. Reason: {res2.reason}")
        print(f"LCB A: {res2.baseline_a_lcb_r}")
        print(f"LCB B: {res2.lcb_r.get('B')}")
        print(f"Delta: {res2.delta_lcb_vs_a}")
        
    assert res2.ok
    assert res2.winner == "B"
    print(f"Case 2 OK: Winner {res2.winner}")
    
    # Case 3: Very High Z (super strict)
    thr_map["strict"] = RegimeThresholds(min_n=100, min_lcb_r=0.0, min_lcb_wr=0.4, min_delta_lcb_vs_a=0.0, z=10.0)
    # LCB A: 0.1 - 0.178 = -0.078
    # LCB B: 0.2 - 0.178 = 0.022
    # Delta: 0.1.
    res3 = eval_winner_lcb(samples_by_arm=arms, regime="strict", group="g", scenario="s", thr_by_regime=thr_map)
    assert res3.ok
    assert res3.winner == "B"
    print(f"Case 3 OK: Winner {res3.winner}")

    print("test_lcb_evaluator_lists passed")

if __name__ == "__main__":
    test_lcb_evaluator_lists()
