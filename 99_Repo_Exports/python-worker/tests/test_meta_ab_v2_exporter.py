from services.orderflow.meta_ab_v2_report_exporter_v1 import ACTION, REPORT_PRESENT, REPORT_SHARE_NEXT, _apply_report


def test_exporter_smoke():
    sample_report = {
        "ts_ms": 1600000000000,
        "run_id": "test-run",
        "winner": "challenger",
        "reason": "win",
        "counts": {
            "n_total": 5000,
            "n_eligible": 4000
        },
        "config": {
            "p_min": 0.55
        },
        "ramp": {
            "share_current": 0.10,
            "share_next": 0.15,
            "action": "increase_share"
        },
        "delta": {
            "exp_r_per_candidate": 0.005,
            "tail_rate_per_candidate": 0.001
        }
    }

    _apply_report(sample_report)

    # Exporter test
    assert REPORT_PRESENT._value.get() == 1.0
    assert REPORT_SHARE_NEXT._value.get() == 0.15
    assert ACTION.labels(action="increase_share")._value.get() == 1.0
