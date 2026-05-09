from __future__ import annotations

# -*- coding: utf-8 -*-
"""
Fees-aware minimum ATR(bps) needed to cover roundtrip fees + buffer, given TP1 share and rocket multiplier.
Pure function: easy to test and reuse.
"""




def fees_aware_min_atr_bps(
    *,
    fees_bps_rt: float,
    tp_bps_buffer: float,
    tp1_share: float,
    rocket_mult: float,
) -> tuple[float, dict]:
    fb = float(fees_bps_rt or 0.0)
    buf = float(tp_bps_buffer or 0.0)
    share = float(tp1_share or 0.0)
    mult = float(rocket_mult or 0.0)

    denom = share * mult
    if denom <= 0:
        return 0.0, {"ok": 0, "reason": "bad_denom", "denom": denom}

    th = float((fb + buf) / denom)
    return th, {
        "ok": 1,
        "fees_bps_rt": fb,
        "tp_bps_buffer": buf,
        "tp1_share": share,
        "rocket_mult": mult,
        "denom": denom,
        "th_bps": th,
    }
