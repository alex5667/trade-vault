from tools.of_gate_sre_monitor import compute_stats, _fmt, build_alerts

def test_reasons_breakdown():
    # Mock rows with different rejection reasons
    rows = [
        # OK
        {"ok": 1, "ok_soft": 0, "ts_ms": 1000},
        # Low Conf
        {"ok": 0, "ok_soft": 0, "ts_ms": 1100, "have": 1, "need": 2, "present_legs": '["obi_stable"]', "missing_legs": '["iceberg_strict", "reclaim_recent"]'},
        # ML Veto
        {"ok": 0, "ok_soft": 0, "ts_ms": 1200, "ml_allow": 0},
        # Meta Veto
        {"ok": 0, "ok_soft": 0, "ts_ms": 1300, "meta_veto": 1},
        # Book Bad
        {"ok": 0, "ok_soft": 0, "ts_ms": 1400, "book_health_ok": 0},
        # Data Health Bad
        {"ok": 0, "ok_soft": 0, "ts_ms": 1500, "data_health": 0.5, "missing_legs": '["obi_stable"]'}, # Data health bad vetoes book evidence
        # Multiple reasons
        {"ok": 0, "ok_soft": 0, "ts_ms": 1600, "book_health_ok": 0, "meta_veto": 1}
    ]
    
    stats = compute_stats(rows, prev=None, dh_bad_th=0.7)
    print("Stats Rejection Reasons:", stats["rejection_reasons"])
    
    expected_reasons = ["low_conf", "ml_veto", "meta_veto", "book_bad", "dh_bad"]
    for r in expected_reasons:
        assert r in stats["rejection_reasons"], f"Missing {r}"
        assert stats["rejection_reasons"][r] > 0
        
    formatted = _fmt(stats, [], window_min=60)
    print("\nFormatted Report:\n", formatted)
    
    assert "REASONS (share of total):" in formatted
    assert "CONF_DETAIL (top legs): PRESENT: obi_stable=0.143 | MISSING: iceberg_strict=0.143 reclaim_recent=0.143" in formatted

if __name__ == "__main__":
    try:
        test_reasons_breakdown()
        print("\n✅ Verification SUCCESS")
    except Exception as e:
        print(f"\n❌ Verification FAILED: {e}")
        import traceback
        traceback.print_exc()
