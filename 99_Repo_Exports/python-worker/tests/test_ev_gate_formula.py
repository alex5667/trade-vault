from signals.ev_gate import evaluate_ev_gate, EvGateConfig

def test_ev_gate_formula_pass():
    cfg = EvGateConfig(
        enabled=True,
        p_min=0.4,
        k_cost=1.0,
        default_costs_bps=8.0,
        log_veto=False
    )
    # entry=100, tp1=102 (+2%), sl=99 (-1%)
    # tp1_bps = 200, stop_bps = 100
    # p=0.5
    # EV = 0.5*200 - 0.5*100 = 100 - 50 = 50 bps
    # costs=10 bps. required = 1.0 * 10 = 10 bps
    # 50 > 10 -> PASS
    res = evaluate_ev_gate(
        cfg=cfg,
        entry=100, tp1=102, sl=99,
        p_hit_tp1=0.5,
        costs_bps=10.0
    )
    assert res.passed is True
    assert res.ev_bps == 50.0

def test_ev_gate_veto_low_p():
    cfg = EvGateConfig(
        enabled=True,
        p_min=0.6,
        k_cost=1.0,
        default_costs_bps=8.0,
        log_veto=False
    )
    # p=0.5 < 0.6 -> VETO
    res = evaluate_ev_gate(
        cfg=cfg,
        entry=100, tp1=102, sl=99,
        p_hit_tp1=0.5,
        costs_bps=10.0
    )
    assert res.passed is False
    assert "p_hit_tp1<0.60" in res.veto_reason

def test_ev_gate_veto_negative_ev():
    cfg = EvGateConfig(
        enabled=True,
        p_min=0.3, # low enough
        k_cost=1.0,
        default_costs_bps=8.0,
        log_veto=False
    )
    # tp1=+100bps, sl=-200bps. p=0.4
    # EV = 0.4*100 - 0.6*200 = 40 - 120 = -80 bps
    # -80 < required -> VETO
    res = evaluate_ev_gate(
        cfg=cfg,
        entry=100, tp1=101, sl=98,
        p_hit_tp1=0.4,
        costs_bps=10.0
    )
    assert res.passed is False
    assert "ev<10.0bps" in res.veto_reason
    assert res.ev_bps < 0
