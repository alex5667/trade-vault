from core.triple_barrier import label_path, BarrierSpec, BarrierOutcome

def test_tb_long_tp_hit():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)
    path = [(0, 100.0), (100, 100.11)]
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=path, spec=spec)
    assert r.outcome == BarrierOutcome.TP_HIT

def test_tb_long_sl_hit():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)
    path = [(0, 100.0), (100, 99.89)]
    r = label_path(ts0_ms=0, direction="LONG", entry_px=100.0, path=path, spec=spec)
    assert r.outcome == BarrierOutcome.SL_HIT

def test_tb_short_tp_hit():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)
    path = [(0, 100.0), (100, 99.88)]
    r = label_path(ts0_ms=0, direction="SHORT", entry_px=100.0, path=path, spec=spec)
    assert r.outcome == BarrierOutcome.TP_HIT

def test_tb_zero_entry_price():
    spec = BarrierSpec(h_ms=1000, tp_bps=10.0, sl_bps=10.0)
    path = [(0, 100.0)]
    r = label_path(ts0_ms=0, direction="LONG", entry_px=0.0, path=path, spec=spec)
    assert r.outcome == BarrierOutcome.NO_TICKS
    assert r.hit_ms == 0
