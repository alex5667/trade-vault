
import pytest

from core.book_evidence import compute_ofi_flags
from core.of_confirm_engine import OFConfirmEngine


# Stub for runtime
class MockRuntime:
    def __init__(self):
        self.last_ofi_event = None
        self.last_obi_event = None
        self.last_iceberg_event = None
        self.last_sweep = None
        self.last_reclaim = None
        self.last_wp = None
        self.last_div = None
        self.last_regime = "neutral"
        self.dynamic_cfg = {}
        self.pressure = MockPressure()
        self.book_churn_hi = 0
        self.cont_ctx_ts_ms = 0
        self.last_bar = None

class MockPressure:
    def is_pressure_hi(self, ts, threshold):
        return False

def test_compute_ofi_flags_basic():
    """Test basic OFI flags computation"""
    cfg = {
        "ofi_event_ttl_ms": 15000,
        "ofi_stable_min_secs": 1.5
    }
    indicators = {}
    now_ts = 100000

    # Case 1: No event
    res = compute_ofi_flags(
        direction="LONG", now_ts_ms=now_ts, last_event=None, cfg=cfg, indicators=indicators
    )
    assert res == (False, False, 0.0, 0.0, 0.0, 0.0)

    # Case 2: Stale event
    last_event = {
        "ts_ms": now_ts - 20000, # 20s old
        "direction": "LONG",
        "ofi": 100.0,
        "stable_secs": 2.0
    }
    res = compute_ofi_flags(
        direction="LONG", now_ts_ms=now_ts, last_event=last_event, cfg=cfg, indicators=indicators
    )
    assert res == (False, False, 0.0, 0.0, 0.0, 0.0)
    assert indicators["ofi_age_ms"] == 20000

    # Case 3: Wrong direction
    last_event = {
        "ts_ms": now_ts - 1000,
        "direction": "SHORT",
        "ofi": 100.0,
        "stable_secs": 2.0
    }
    res = compute_ofi_flags(
        direction="LONG", now_ts_ms=now_ts, last_event=last_event, cfg=cfg, indicators=indicators
    )
    assert res == (False, False, 0.0, 0.0, 0.0, 0.0)

    # Case 4: Valid event, stable
    last_event = {
        "ts_ms": now_ts - 1000,
        "direction": "LONG",
        "ofi": 500.0,
        "ofi_z": 3.5,
        "stable_secs": 2.0,
        "stability_score": 0.9
    }
    res = compute_ofi_flags(
        direction="LONG", now_ts_ms=now_ts, last_event=last_event, cfg=cfg, indicators=indicators
    )
    # (ofi_dir_ok, ofi_stable, stable_secs, ofi_val, ofi_z, stability_score)
    assert res[0] is True  # dir_ok
    assert res[1] is True  # stable (2.0 >= 1.5)
    assert res[2] == 2.0   # stable_secs
    assert res[3] == 500.0 # val
    assert res[4] == 3.5   # z
    assert res[5] == 0.9   # score
    assert indicators["ofi_dir_ok"] == 1
    assert indicators["ofi_stable"] == 1

def test_of_confirm_engine_ofi_integration():
    """Test integration of OFI into OFConfirmEngine score"""
    engine = OFConfirmEngine()
    runtime = MockRuntime()
    cfg = {
        "w_z": 0.30,
        "w_wp": 0.15,
        "w_reclaim": 0.20,
        "w_obi": 0.15,
        "w_ice": 0.15,
        "w_abs": 0.05,
        "w_ofi_z": 0.10,    # custom weight for test
        "w_ofi_stab": 0.05, # custom weight for test
        "score_z_ref": 3.0,
        "ofi_z_ref": 3.0,
        "data_health_min_for_book_evidence": 0.7
    }
    indicators = {
        "now_ts_ms": 100000,
        "book_health_ok": 1,
        "data_health": 1.0
    }

    # Setup OFI event in runtime
    runtime.last_ofi_event = {
        "ts_ms": 99000, # 1s ago
        "direction": "LONG",
        "ofi": 1000.0,
        "ofi_z": 3.0,      # Normalized: 3.0/3.0 = 1.0
        "stable_secs": 2.0,
        "stability_score": 1.0
    }

    # Call build
    confirm, dec = engine.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        tick_ts_ms=100000,
        price=50000.0,
        delta_z=0.0, # Zero Z to focus on OFI
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )

    assert confirm is not None
    ev = confirm.evidence
    contrib = confirm.contrib

    # Check evidence fields
    assert ev["ofi_dir_ok"] == 1
    assert ev["ofi_stable"] == 1
    assert ev["ofi_z"] == 3.0
    assert ev["ofi_stability_score"] == 1.0

    # Check contribution
    # OFI Z: (3.0/3.0) * 0.10 = 0.10
    # OFI Stab: 1.0 * 0.05 = 0.05
    # Total contrib expected from OFI = 0.15
    # Total weights sum (assuming other features are 0 or disabled):
    # Z=0 (contrib 0), WP=0, Reclaim=0, OBI=0, Ice=0, Abs=0.
    # Weights active: w_z(0.3) + w_wp(0.15) + w_rec(0.2) + w_obi(0.15) + w_ice(0.15) + w_abs(0.05) + w_ofi_z(0.1) + w_ofi_stab(0.05) = 1.15
    # Score = 0.15 / 1.15 ~= 0.13

    assert "ofi_z" in contrib
    assert contrib["ofi_z"] == pytest.approx(0.10)
    assert contrib["ofi_stab"] == pytest.approx(0.05)

    # Verify score calculation
    # raw_sum = 0.15
    # w_sum = 0.3+0.15+0.2+0.15+0.15+0.05 + 0.10+0.05 = 1.15
    expected_score = 0.15 / 1.15
    assert confirm.score == pytest.approx(expected_score)

def test_of_confirm_engine_veto_book_health():
    """Test that bad book health vetoes OFI"""
    engine = OFConfirmEngine()
    runtime = MockRuntime()
    cfg = {}
    indicators = {
        "now_ts_ms": 100000,
        "book_health_ok": 0, # BAD HEALTH
        "data_health": 1.0
    }

    runtime.last_ofi_event = {
        "ts_ms": 99000,
        "direction": "LONG",
        "ofi": 1000.0,
        "ofi_z": 3.0,
        "stable_secs": 2.0
    }

    confirm, _ = engine.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        tick_ts_ms=100000,
        price=50000.0,
        delta_z=0.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )

    # Evidence should be cleared
    assert confirm.evidence["ofi_dir_ok"] == 0
    assert confirm.evidence["ofi_stable"] == 0
    assert confirm.evidence["ofi_z"] == 0.0

    # Contribution should be 0
    assert confirm.contrib["ofi_z"] == 0.0
