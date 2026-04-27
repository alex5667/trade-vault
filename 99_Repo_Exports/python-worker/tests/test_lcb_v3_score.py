from core.lcb_r_adj import lcb

def test_score_min_logic():
    # Scenario: LCB Mean is better than LCB Tail
    # mean=0.5, std=0.2, n=50, z=1.28
    # lcb = 0.5 - 1.28 * (0.2 / sqrt(50)) = 0.5 - 1.28 * 0.028 = 0.5 - 0.036 = 0.464
    l1 = lcb(0.5, 0.2, 50, 1.28)
    
    # tail_mu=0.1, tail_std=0.1, tail_n=30, tail_z=1.28
    # lcb = 0.1 - 1.28 * (0.1 / sqrt(30)) = 0.1 - 1.28 * 0.018 = 0.1 - 0.023 = 0.077
    l2 = lcb(0.1, 0.1, 30, 1.28)
    
    # WIN_SCORE should be min(l1, l2) which is l2
    win_score = min(l1, l2)
    assert abs(win_score - l2) < 1e-7
    assert l1 > l2

def test_score_tail_penalized():
    # If a strategy has a huge fat tail (worst trades are very bad),
    # lcb_tail will be low even if lcb_mean is high.
    l_mean = lcb(1.0, 0.5, 100, 1.65) # robust mean
    l_tail = lcb(-2.0, 0.5, 50, 1.65) # horrible worst trades
    
    win_score = min(l_mean, l_tail)
    assert win_score == l_tail
    assert win_score < 0
