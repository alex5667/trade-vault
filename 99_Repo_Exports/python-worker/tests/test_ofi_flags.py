from core.book_evidence import compute_ofi_flags


def test_ofi_flags_basic():
    ind = {}
    ev = {"ts_ms": 1000, "direction":"LONG", "ofi":1.0, "ofi_z":2.0, "stable_secs":2.0, "stability_score":0.9, "stable":1}
    cfg = {"ofi_event_ttl_ms": 15000, "ofi_stable_min_secs": 1.5, "ofi_stability_score_min": 0.6}
    d_ok, s_ok, secs, ofi, z, stab = compute_ofi_flags(direction="LONG", now_ts_ms=2000, last_event=ev, cfg=cfg, indicators=ind)
    assert d_ok is True
    assert s_ok is True
    assert ind["ofi_stable"] == 1

