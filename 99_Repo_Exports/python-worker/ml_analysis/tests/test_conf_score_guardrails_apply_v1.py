from orderflow_services.conf_score_guardrails_apply_v1 import (
    decide_actions,
    decide_actions_thresholds,
    apply_hysteresis_and_recovery,
)


def test_guardrails_decide_actions_supports_parts_list_schema():
    report = {
        "group_by": "symbol",
        "rows": [
            {
                "group": {"symbol": "BTCUSDT"},
                "n": 1000,
                "target_day": "2026-02-18",
                "baseline_days": ["2026-02-17"],
                "parts": [
                    {
                        "key": "base",
                        "n_base": 700,
                        "n_target": 250,
                        "ref_med": 0.50,
                        "cur_med": 0.80,
                        "ref_mad": 0.02,
                        "cur_mad": 0.02,
                        "drift_z": 6.5,
                    },
                ],
            },
            {
                "group": {"symbol": "ETHUSDT"},
                "n": 1000,
                "target_day": "2026-02-18",
                "baseline_days": ["2026-02-17"],
                "parts": [
                    {
                        "key": "base",
                        "n_base": 700,
                        "n_target": 300,
                        "ref_med": 0.50,
                        "cur_med": 0.55,
                        "ref_mad": 0.02,
                        "cur_mad": 0.02,
                        "drift_z": 3.0,
                    },
                ],
            },
        ],
    }

    # BTC -> Z=6.5 (>6.0) -> freeze=1, scale=0.85
    # ETH -> Z=3.0 (<4.0) -> freeze=0, scale=1.0

    dec = decide_actions(report, warn_z=4.0, crit_z=6.0, min_n=200)
    assert dec["BTCUSDT"]["freeze"] == 1
    assert abs(dec["BTCUSDT"]["scale"] - 0.85) < 1e-12

    assert dec["ETHUSDT"]["freeze"] == 0
    assert abs(dec["ETHUSDT"]["scale"] - 1.0) < 1e-12


def test_guardrails_decide_actions_warn_level():
    report = {
        "rows": [
            {
                "group": "BTCUSDT",
                "n": 500,
                "parts": {"base": {"dz": 4.5}},
            }
        ]
    }
    # 4.5 >= 4.0 but < 6.0 -> freeze=0, scale=0.92
    dec = decide_actions(report, warn_z=4.0, crit_z=6.0, min_n=200)
    assert dec["BTCUSDT"]["freeze"] == 0
    assert abs(dec["BTCUSDT"]["scale"] - 0.92) < 1e-12


def test_guardrails_hysteresis_latch_and_recovery_ramp():
    # Run1: critical -> enter freeze, latch for 1h
    report1 = {"rows": [{"group": {"symbol": "BTCUSDT"}, "n": 300, "parts": {"base": {"dz": 7.0}}}]}
    raw1 = decide_actions_thresholds(report1, warn_z=4.0, crit_z=6.0, min_n=200)
    dec1 = apply_hysteresis_and_recovery(
        raw1,
        prev_state={},
        now_ms=1000,
        recover_z=3.0,
        recover_runs=2,
        freeze_hold_sec=3600,
        recover_scale_start=0.92,
        recover_scale_step=0.05,
        scale_bump_min_sec=0,
        canary_share=1.0,
        canary_salt="x",
    )
    assert dec1["BTCUSDT"]["freeze"] == 1
    assert dec1["BTCUSDT"]["latch_remaining_sec"] > 0

    # Run2 (still within latch): stable but must stay frozen
    report2 = {"rows": [{"group": {"symbol": "BTCUSDT"}, "n": 300, "parts": {"base": {"dz": 1.0}}}]}
    raw2 = decide_actions_thresholds(report2, warn_z=4.0, crit_z=6.0, min_n=200)
    dec2 = apply_hysteresis_and_recovery(
        raw2,
        prev_state={"decisions": dec1},
        now_ms=2000,
        recover_z=3.0,
        recover_runs=2,
        freeze_hold_sec=3600,
        recover_scale_start=0.92,
        recover_scale_step=0.05,
        scale_bump_min_sec=0,
        canary_share=1.0,
        canary_salt="x",
    )
    assert dec2["BTCUSDT"]["freeze"] == 1
    assert "latched" in dec2["BTCUSDT"]["reason"]

    # Run3 (after latch expiry): stable streak >=2 -> unfreeze at recover_scale_start
    dec1_mod = dict(dec1["BTCUSDT"])
    dec1_mod["latched_until_ms"] = 0  # simulate latch expiry
    dec1_state = {"decisions": {"BTCUSDT": dec1_mod}}

    raw3 = decide_actions_thresholds(report2, warn_z=4.0, crit_z=6.0, min_n=200)
    dec3 = apply_hysteresis_and_recovery(
        raw3,
        prev_state=dec1_state,
        now_ms=4000,
        recover_z=3.0,
        recover_runs=1,
        freeze_hold_sec=3600,
        recover_scale_start=0.92,
        recover_scale_step=0.05,
        scale_bump_min_sec=0,
        canary_share=1.0,
        canary_salt="x",
    )
    assert dec3["BTCUSDT"]["freeze"] == 0
    assert abs(dec3["BTCUSDT"]["scale"] - 0.92) < 1e-9


def test_guardrails_canary_gate():
    report = {"rows": [{"group": {"symbol": "BTCUSDT"}, "n": 300, "parts": {"base": {"dz": 7.0}}}]}
    raw = decide_actions_thresholds(report, warn_z=4.0, crit_z=6.0, min_n=200)

    dec = apply_hysteresis_and_recovery(
        raw,
        prev_state={},
        now_ms=1000,
        recover_z=3.0,
        recover_runs=2,
        freeze_hold_sec=3600,
        recover_scale_start=0.92,
        recover_scale_step=0.05,
        scale_bump_min_sec=0,
        canary_share=0.0,
        canary_salt="x",
    )
    assert dec["BTCUSDT"]["canary"] == 0
    assert dec["BTCUSDT"]["freeze"] == 0  # skipped -> neutral
