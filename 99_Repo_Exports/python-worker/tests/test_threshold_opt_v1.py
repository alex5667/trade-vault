import numpy as np

from core.threshold_opt_v1 import best_threshold_by_utility


def test_best_threshold_prefers_higher_sum_util():
    p = np.array([0.4,0.5,0.6,0.7,0.8], dtype=float)
    y = np.array([0,1,1,0,1], dtype=int)
    util = np.array([-0.1, 0.2, 0.2, -0.3, 0.5], dtype=float)

    r = best_threshold_by_utility(p=p, y_edge=y, util_r=util, thr_min=0.4, thr_max=0.8, thr_step=0.1, min_trades=2)
    assert r.n_take >= 2
    # at thr=0.6 take {0.6,0.7,0.8} sum=0.2-0.3+0.5=0.4
    # at thr=0.5 take {0.5,0.6,0.7,0.8} sum=0.2+0.2-0.3+0.5=0.6 (better)
    assert abs(r.thr - 0.5) < 1e-9

