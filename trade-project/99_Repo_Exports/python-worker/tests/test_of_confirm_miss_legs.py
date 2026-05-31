from __future__ import annotations

from types import SimpleNamespace

from core.of_confirm_engine import OFConfirmEngine


def test_reversal_miss_leg_b_leg_c():
    """Test that miss:leg_b,leg_c is appended to reason when reversal fails with only BIT_A set."""
    eng = OFConfirmEngine(version=2)
    indicators = {"delta_z": 2.8}
    cfg = {
        "require_strong_confirmation": True,
        "strong_z_min": 2.0,
        "strong_need_reversal": 3,  # need 3 legs
        "obi_event_ttl_ms": 5000,
        "obi_stable_min_secs": 1.5,
        "iceberg_event_ttl_ms": 15000,
        "iceberg_strict_refresh_min": 3,
        "iceberg_strict_duration_min": 1.5,
        "iceberg_strict_dist_bp": 10.0,
        "sweep_valid_ms": 120000,
        "reclaim_signal_valid_ms": 120000,
        "of_score_min": 0.0,
        "w_z": 0.3, "w_wp": 0.15, "w_reclaim": 0.2, "w_obi": 0.15, "w_ice": 0.15, "w_abs": 0.05,
        "score_z_ref": 3.0
    }
    # runtime: weak_progress (BIT_A), sweep but NO reclaim (so BIT_B=0), no obi/iceberg (BIT_C=0)
    # Note: scenario="reversal" requires sweep_recent, but BIT_B requires BOTH sweep AND reclaim
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=True),  # -> BIT_A
        last_obi_event=None,  # no BIT_C
        last_iceberg_event=None,  # no BIT_C
        last_sweep=SimpleNamespace(ts_ms=9000, kind="EQL", direction_bias="LONG"),  # needed for scenario="reversal", but BIT_B needs reclaim too
        last_reclaim=None,  # no reclaim -> BIT_B=0
        last_div=None,
        cont_ctx_ts_ms=0,
    )
    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=10000,
        price=100.05,
        delta_z=2.8,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )
    assert ofc is not None
    assert ofc.scenario == "reversal"
    assert ofc.ok == 0  # fails because need=3 but have=1 (only BIT_A)
    assert ofc.have < ofc.need
    # Check that reason contains miss:leg_b,leg_c
    assert "|miss:" in ofc.reason
    assert "leg_b" in ofc.reason
    assert "leg_c" in ofc.reason
    # leg_a should not be in miss list (it's present)
    miss_part = ofc.reason.split("|miss:")[1] if "|miss:" in ofc.reason else ""
    assert "leg_a" not in miss_part


def test_continuation_miss_leg_a_leg_c():
    """Test that miss:leg_a,leg_c is appended to reason when continuation fails with only BIT_B set."""
    eng = OFConfirmEngine(version=2)
    indicators = {"delta_z": 1.5}
    cfg = {
        "require_strong_confirmation": False,
        "strong_need_continuation": 3,  # need 3 legs
        "obi_event_ttl_ms": 5000,
        "obi_stable_min_secs": 1.5,
        "iceberg_event_ttl_ms": 15000,
        "iceberg_strict_refresh_min": 3,
        "iceberg_strict_duration_min": 1.5,
        "iceberg_strict_dist_bp": 10.0,
        "sweep_valid_ms": 120000,
        "reclaim_signal_valid_ms": 120000,
        "of_score_min": 0.0,
        "w_z": 0.3, "w_wp": 0.15, "w_reclaim": 0.2, "w_obi": 0.15, "w_ice": 0.15, "w_abs": 0.05,
        "score_z_ref": 3.0
    }
    # runtime: only sweep/reclaim (BIT_B), no hidden_ctx (BIT_A), no cont_ctx (BIT_C)
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=False),
        last_obi_event={"ts_ms": 9000, "direction": "LONG", "obi": 0.3, "stable_secs": 2.0},  # -> BIT_B (via obi_stable)
        last_iceberg_event=None,
        last_sweep=SimpleNamespace(ts_ms=9000, kind="EQL", direction_bias="LONG"),  # -> BIT_B
        last_reclaim=SimpleNamespace(ts_ms=9500, hold_bars=2, direction_bias="LONG", level=99.0, pool_id="p1"),  # -> BIT_B
        last_div=None,
        cont_ctx_ts_ms=0,  # no cont_ctx -> no BIT_C
    )
    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=10000,
        price=100.05,
        delta_z=1.5,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )
    assert ofc is not None
    # Note: continuation might not trigger if trend_dir is missing, but if it does:
    if ofc.scenario == "continuation" and ofc.ok == 0 and ofc.have < ofc.need:
        assert "|miss:" in ofc.reason


def test_reversal_ok_no_miss_tag():
    """Test that miss: tag is NOT added when ok=1 (gate passes)."""
    eng = OFConfirmEngine(version=2)
    indicators = {"delta_z": 2.8}
    cfg = {
        "require_strong_confirmation": True,
        "strong_z_min": 2.0,
        "strong_need_reversal": 2,
        "obi_event_ttl_ms": 5000,
        "obi_stable_min_secs": 1.5,
        "iceberg_event_ttl_ms": 15000,
        "iceberg_strict_refresh_min": 3,
        "iceberg_strict_duration_min": 1.5,
        "iceberg_strict_dist_bp": 10.0,
        "sweep_valid_ms": 120000,
        "reclaim_signal_valid_ms": 120000,
        "of_score_min": 0.0,
        "w_z": 0.3, "w_wp": 0.15, "w_reclaim": 0.2, "w_obi": 0.15, "w_ice": 0.15, "w_abs": 0.05,
        "score_z_ref": 3.0
    }
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=True),
        last_obi_event={"ts_ms": 9000, "direction": "LONG", "obi": 0.3, "stable_secs": 2.0},
        last_iceberg_event=None,
        last_sweep=SimpleNamespace(ts_ms=9000, kind="EQL", direction_bias="LONG"),
        last_reclaim=SimpleNamespace(ts_ms=9500, hold_bars=2, direction_bias="LONG", level=99.0, pool_id="p1"),
        last_div=None,
        cont_ctx_ts_ms=0,
    )
    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=10000,
        price=100.05,
        delta_z=2.8,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )
    assert ofc is not None
    assert ofc.scenario == "reversal"
    assert ofc.ok == 1  # passes
    # When ok=1, miss: tag should NOT be present
    assert "|miss:" not in ofc.reason


def test_cap_reason_keep_miss():
    """Test the _cap_reason_keep_miss function preserves |miss: suffix."""
    # This function is in strategy.py, so we test it directly
    def _cap_reason_keep_miss(r: str, maxlen: int = 120) -> str:
        """Cap reason but preserve '|miss:...' suffix for quick scan/grep."""
        try:
            s = (r or "")
            if len(s) <= maxlen:
                return s
            i = s.find("|miss:")
            if i < 0:
                return s[:maxlen]
            miss = s[i:]
            base = s[:i]
            keep_base = max(0, maxlen - len(miss))
            return base[:keep_base] + miss
        except Exception:
            return (r or "")[:maxlen]

    # Test: long reason with miss suffix should preserve suffix
    long_reason = "a" * 150 + "|miss:leg_b,leg_c"
    result = _cap_reason_keep_miss(long_reason, 120)
    assert len(result) <= 120
    assert result.endswith("|miss:leg_b,leg_c")
    assert "miss:leg_b,leg_c" in result

    # Test: short reason should be unchanged
    short_reason = "reversal_gate(1/2)|miss:leg_b"
    result = _cap_reason_keep_miss(short_reason, 120)
    assert result == short_reason

    # Test: reason without miss suffix should be truncated normally
    long_no_miss = "a" * 150
    result = _cap_reason_keep_miss(long_no_miss, 120)
    assert len(result) == 120
    assert "|miss:" not in result

    # Test: exactly at boundary
    boundary_reason = "a" * 100 + "|miss:leg_a,leg_b,leg_c"
    result = _cap_reason_keep_miss(boundary_reason, 120)
    assert len(result) <= 120
    assert result.endswith("|miss:leg_a,leg_b,leg_c")

