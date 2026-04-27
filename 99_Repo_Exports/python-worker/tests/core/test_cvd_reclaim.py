from core.cvd_reclaim import compute_cvd_reclaim


def test_cvd_reclaim_sign_alignment():
    # SHORT bias expects delta_cvd < 0
    ev = compute_cvd_reclaim(
        ts_ms=2000,
        bias="SHORT",
        sweep_ts_ms=1000,
        reclaim_ts_ms=2000,
        cvd_sweep=100.0,
        cvd_reclaim=70.0,
        min_abs_delta=0.0,
    )
    assert ev.ok == 1

    # LONG bias expects delta_cvd > 0
    ev2 = compute_cvd_reclaim(
        ts_ms=2000, bias="LONG", sweep_ts_ms=1000, reclaim_ts_ms=2000,
        cvd_sweep=100.0, cvd_reclaim=70.0, min_abs_delta=0.0
    )
    assert ev2.ok == 0
