# tick_flow_full/tests/core/test_of_confirm_engine_liqmap_gate_wiring_v1.py
"""Unit tests: OFConfirmEngine wiring for LiqMap gate.

Contract enforced here:
  - In SHADOW mode, a liqmap gate hit should set a dedicated gate bit
    (even though it does not hard-veto the trade).
  - The engine must export stable `liqmap_gate_*` indicators.

If this test fails, you almost certainly have one of:
  - liqmap gate not wired into of_confirm_engine
  - indicator key mismatch (liqmap_* naming drift)
  - gate bit not applied in shadow
"""


def test_of_confirm_engine_liqmap_gate_shadow_sets_bits_and_exports_indicators():
    from core.of_confirm_engine import OFConfirmEngine

    engine = OFConfirmEngine()

    class _Runtime:
        # Minimal runtime attributes used by OFConfirmEngine.build
        symbol = "BTCUSDT"
        dynamic_cfg = {}
        # Strategy/TF are used in some telemetry paths.
        config = {"micro_tf": "1m", "strategy_name": "cryptoorderflow"}

        last_regime = "bull"

        # These are frequently accessed; keep them present.
        last_obi_event = None
        last_iceberg_event = None
        last_ofi_event = None
        last_sweep = None
        last_reclaim = None
        last_div = None
        last_wp = None
        last_fp_edge = None

    # Indicators must include a minimal set for a stable build path.
    indicators = {
        "price": 100.0,
        "atr_bps": 50.0,
        "tick_time_age_ms": 0.0,
        "data_health": 1.0,
        "book_health_ok": 1.0,
        # LiqMap features used by the gate (window=1h)
        "liqmap_1h_dist_dn_bps": 5.0,
        "liqmap_1h_dist_up_bps": 250.0,
        "liqmap_1h_peak_dn1_usd": 600_000.0,
        "liqmap_1h_peak_up1_usd": 50_000.0,
        "liqmap_1h_age_ms": 500.0,
        # Totals/near are optional for v1 gate, but often logged.
        "liqmap_1h_total_usd": 1_000_000.0,
        "liqmap_1h_near_total_usd": 800_000.0,
        "liqmap_1h_near_imb": -0.2,
    }

    cfg = {
        # Keep the rest defaults; only set liqmap gate controls.
        "liqmap_gate_mode": "shadow",
        "liqmap_gate_window": "1h",
        # Soft/hard are irrelevant in shadow; choose conservative.
        "liqmap_gate_peak_min_usd": 250_000.0,
        "liqmap_gate_sl_band_mult": 2.0,
    }

    # Should not raise.
    confirm, _gate_decision = engine.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        tick_ts_ms=1_000_000,
        price=100.0,
        delta_z=1.5,
        runtime=_Runtime(),
        cfg=cfg,
        indicators=indicators,
        absorption=None,
    )

    # Wiring sanity: stable exports.
    assert "liqmap_gate_shadow_veto" in indicators, "liqmap gate exports missing"
    assert int(indicators.get("liqmap_gate_shadow_veto") or 0) in (0, 1)
    assert "liqmap_gate_veto" in indicators
    assert "liqmap_gate_reason" in indicators

    # Gate bit contract: must be set on shadow hit.
    liqmap_bit = 1 << 25
    assert confirm is not None, "Engine returned no confirm object"
    assert int(getattr(confirm, "gate_bits", 0) or 0) & liqmap_bit, "LiqMap gate bit not set in SHADOW"
