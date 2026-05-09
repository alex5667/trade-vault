from __future__ import annotations

from tools.entry_policy_tuner_suggest import TunerCfg, suggest_from_records


def test_tuner_insufficient_samples_no_change():
    tcfg = TunerCfg(enable=True, min_total=50, min_allow=10)
    recs = [{"ok": 1, "coh": 0.8, "leader_conf_score": 0.8, "zone_dist_bp": 8, "regime": "range"}] * 9
    out = suggest_from_records(records=recs, tuner=tcfg, current_env={
        "SMT_COH_THRESHOLD": 0.65,
        "SMT_LEADER_CONF_MIN_SCORE": 0.65,
        "SMT_ENTRY_MAX_ZONE_BP": 15.0,
        "SMT_ENTRY_MAX_ZONE_BP_THIN": 10.0,
        "SMT_ENTRY_OBI_MIN_SEC": 1.5,
    })
    assert out["safe_to_apply"] == 0
    assert out["proposed"] == {}


def test_tuner_tighten_zone_bp_with_step_cap():
    # many allows with small zone_dist_bp => propose tightening max_zone_bp downwards
    tcfg = TunerCfg(enable=True, tighten_only=True, min_total=50, min_allow=10, q_zone_bp=0.90, step_zone_bp=3.0)
    recs = []
    for _ in range(80):
        recs.append({"ok": 1, "regime": "range", "zone_dist_bp": 6.0, "coh": 0.8, "leader_conf_score": 0.8, "obi_stable_sec": 0.0, "iceberg_strict": 0})
    out = suggest_from_records(records=recs, tuner=tcfg, current_env={
        "SMT_COH_THRESHOLD": 0.65,
        "SMT_LEADER_CONF_MIN_SCORE": 0.65,
        "SMT_ENTRY_MAX_ZONE_BP": 15.0,
        "SMT_ENTRY_MAX_ZONE_BP_THIN": 10.0,
        "SMT_ENTRY_OBI_MIN_SEC": 1.5,
    })
    assert out["safe_to_apply"] == 1
    # step cap: 15 -> 12 maximum tightening per day
    assert out["proposed"]["SMT_ENTRY_MAX_ZONE_BP"] == 12.0


def test_tuner_raise_coh_threshold_step_cap():
    tcfg = TunerCfg(enable=True, tighten_only=True, min_total=50, min_allow=10, q_coh=0.20, step_coh=0.03)
    recs = []
    # allowed cohs clustered at 0.78 => q20 ~0.78 -> propose raising from 0.65 to 0.68 (step cap)
    for _ in range(80):
        recs.append({"ok": 1, "regime": "range", "zone_dist_bp": 8.0, "coh": 0.78, "leader_conf_score": 0.8, "obi_stable_sec": 0.0, "iceberg_strict": 0})
    out = suggest_from_records(records=recs, tuner=tcfg, current_env={
        "SMT_COH_THRESHOLD": 0.65,
        "SMT_LEADER_CONF_MIN_SCORE": 0.65,
        "SMT_ENTRY_MAX_ZONE_BP": 15.0,
        "SMT_ENTRY_MAX_ZONE_BP_THIN": 10.0,
        "SMT_ENTRY_OBI_MIN_SEC": 1.5,
    })
    assert out["safe_to_apply"] == 1
    assert abs(out["proposed"]["SMT_COH_THRESHOLD"] - 0.68) < 1e-9
