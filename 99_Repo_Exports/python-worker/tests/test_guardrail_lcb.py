
import pytest
from services.autopilot_guardrail_service import _lcb, _mad_sigma, _median

def test_median():
    assert _median([1.0, 3.0, 2.0]) == 2.0
    assert _median([1.0, 2.0, 3.0, 4.0]) == 2.5
    assert _median([]) == 0.0

def test_mad_sigma():
    # median = 2.0
    # devs = |1-2|=1, |2-2|=0, |3-2|=1
    # median_dev = 1.0
    # sigma = 1.4826 * 1.0 = 1.4826
    data = [1.0, 2.0, 3.0]
    sig = _mad_sigma(data)
    assert abs(sig - 1.4826) < 0.0001
    
    # Constant = 0 sigma
    assert _mad_sigma([1.0, 1.0]) == 0.0

def test_lcb_logic():
    # positive mean, low variance -> positive LCB
    data_good = [0.5, 0.6, 0.4, 0.5, 0.5, 0.6] * 10
    lcb_good = _lcb(data_good, 1.64)
    assert lcb_good > 0.4
    
    # negative mean -> negative LCB
    data_bad = [-0.2, -0.3, -0.1, -0.2] * 10
    lcb_bad = _lcb(data_bad, 1.64)
    assert lcb_bad < -0.1
    
    # high variance penalizes LCB
    # mean ~ 0.5
    data_volatile = [0.0, 1.0] * 30
    lcb_vol = _lcb(data_volatile, 1.64)
    # mu=0.5. sigma ~ 0.5*1.48
    # LCB = 0.5 - 1.64 * (sigma/sqrt(60))
    # It should be lower than mean
    assert lcb_vol < 0.5
