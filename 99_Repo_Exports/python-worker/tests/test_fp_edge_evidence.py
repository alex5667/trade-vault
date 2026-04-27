import types

from core.fp_edge_evidence import compute_fp_edge_absorb


def test_fp_edge_absorb_ok_long():
    cfg = {"fp_edge_valid_ms": 30000, "fp_edge_min_strength": 1.0, "fp_edge_require_no_range_expansion": 1}
    ind = {}
    now = 1_000_000
    fe = types.SimpleNamespace(ts_ms=now - 1000, p90=10.0, value=15.0, bias="LONG", range_expansion=0)
    ok, strength, rng, bias = compute_fp_edge_absorb(direction="LONG", now_ts_ms=now, last_edge=fe, cfg=cfg, indicators=ind)
    assert ok is True
    assert strength == 1.5
    assert rng == 0
    assert bias == "LONG"
    assert ind["fp_edge_absorb"] == 1


def test_fp_edge_reject_range_expansion_when_required():
    cfg = {"fp_edge_valid_ms": 30000, "fp_edge_min_strength": 1.0, "fp_edge_require_no_range_expansion": 1}
    ind = {}
    now = 1_000_000
    fe = types.SimpleNamespace(ts_ms=now - 1000, p90=10.0, value=20.0, bias="LONG", range_expansion=1)
    ok, *_ = compute_fp_edge_absorb(direction="LONG", now_ts_ms=now, last_edge=fe, cfg=cfg, indicators=ind)
    assert ok is False
    assert ind["fp_edge_absorb"] == 0


def test_fp_edge_reject_low_strength():
    cfg = {"fp_edge_valid_ms": 30000, "fp_edge_min_strength": 1.2, "fp_edge_require_no_range_expansion": 1}
    ind = {}
    now = 1_000_000
    fe = types.SimpleNamespace(ts_ms=now - 1000, p90=10.0, value=11.0, bias="LONG", range_expansion=0)
    ok, strength, *_ = compute_fp_edge_absorb(direction="LONG", now_ts_ms=now, last_edge=fe, cfg=cfg, indicators=ind)
    assert strength == 1.1
    assert ok is False

