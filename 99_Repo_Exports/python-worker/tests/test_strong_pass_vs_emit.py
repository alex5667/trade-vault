from __future__ import annotations


class RT:
    last_emit_ts_ms = 0
    last_emit_dir = "NONE"
    last_of_strong_ts_ms = 0
    last_of_dir = "NONE"
    last_strong_pass_ts_ms = 0
    last_strong_pass_dir = "NONE"


def test_do_not_overwrite_strong_on_non_pass():
    rt = RT()
    # prior strong-pass
    rt.last_of_strong_ts_ms = 1000
    rt.last_of_dir = "LONG"
    # emulate non-pass emit
    ts = 2000
    ok = 0
    
    # Logic similar to service
    rt.last_emit_ts_ms = ts
    rt.last_emit_dir = "SHORT"
    
    if ok == 1:
        rt.last_of_strong_ts_ms = ts
        rt.last_of_dir = "SHORT"
        
    # must remain LONG
    assert rt.last_of_dir == "LONG"
    assert rt.last_of_strong_ts_ms == 1000
    
    # but emit must update
    assert rt.last_emit_dir == "SHORT"
    assert rt.last_emit_ts_ms == 2000

def test_overwrite_strong_on_pass():
    rt = RT()
    rt.last_of_strong_ts_ms = 1000
    rt.last_of_dir = "LONG"
    
    ts = 2000
    ok = 1
    
    rt.last_emit_ts_ms = ts
    rt.last_emit_dir = "SHORT"
    
    if ok == 1:
        rt.last_of_strong_ts_ms = ts
        rt.last_of_dir = "SHORT"
        
    assert rt.last_of_dir == "SHORT"
    assert rt.last_of_strong_ts_ms == 2000
