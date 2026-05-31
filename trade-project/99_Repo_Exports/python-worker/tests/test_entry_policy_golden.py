from __future__ import annotations

from services.entry_policy_core import EntryPolicyCfg, evaluate_entry_policy


def test_entry_policy_golden_minimal():
    cfg = EntryPolicyCfg(coh_thr=0.65, leader_conf_min=0.65, min_of_score=1.0, max_zone_bp=15, max_zone_bp_thin=10, obi_min_sec=1.5, dedup_ms=60000)
    dedup = {}

    # Case 1: allow in range
    cand = {"symbol": "ETHUSDT", "side": "LONG", "zone_id": "W_HIGH", "bundle": "b1", "ts_ms": 1000}
    snap = {"zone_id": "W_HIGH", "zone_dist_bp": 8.0, "zone_ok": 1, "zone_side": "MID", "regime": "range", "abs_lvl_th_unstable": 0,
            "of_strong": 1, "of_dir": "LONG", "of_confirm_score": 1.0, "obi_stable_sec": 0.0, "iceberg_strict": 0}
    bundle = {"decision": "continuation", "pick": "ETHUSDT", "coh": 0.8, "leader_conf_score": 0.8, "news_blocked": 0}
    d = evaluate_entry_policy(now_ms=1000, cand=cand, snap=snap, bundle=bundle, cfg=cfg, dedup_state=dedup)
    assert d.ok is True and d.reason_code == "ALLOW"

    # Case 2: deny in thin without obi/ice
    cand2 = {"symbol": "ETHUSDT", "side": "LONG", "zone_id": "W_HIGH", "bundle": "b1", "ts_ms": 2000}
    snap2 = dict(snap)
    snap2["regime"] = "thin"
    snap2["obi_stable_sec"] = 0.0
    snap2["iceberg_strict"] = 0
    d2 = evaluate_entry_policy(now_ms=2000, cand=cand2, snap=snap2, bundle=bundle, cfg=cfg, dedup_state=dedup)
    assert d2.ok is False and d2.reason_code == "EXTRA_CONFIRM_FAIL"
