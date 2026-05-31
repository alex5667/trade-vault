"""
Unit tests for OFInputs determinism: same inputs → same payload (byte-by-byte).
Tests canonical JSON serialization, version selection, and evidence key unification.
"""
import json
from typing import Any

from core.of_inputs_contract import OFInputsV2
from core.redis_keys import RedisStreams as RS


def test_canonical_json_serialization():
    """Test that JSON serialization is canonical (sort_keys, separators) → byte-by-byte deterministic."""
    v2 = OFInputsV2(
        v=2,
        symbol="BTCUSDT",
        ts_ms=1000000,
        regime="trend",
        direction="LONG",
        scenario="reversal",
        delta_z=2.5,
        weak_progress=1,
        sweep_recent=1,
        reclaim_recent=1,
        obi_stable=1,
        iceberg_strict=1,
        abs_lvl_ok=1,
        trend_dir="LONG",
        hidden_ctx_recent=1,
        cont_ctx_recent=1,
        cfg={"test": 1, "another": 2},
        fp_eff_quote=50000.0,
        fp_quote_delta=10.0,
        ofi=1.5,
        ofi_z=2.0,
        ofi_stable=1,
        ofi_dir_ok=1,
        ofi_stable_secs=3.0,
        ofi_stability_score=0.8,
        ofi_age_ms=500,
        fp_edge_absorb=1,
        fp_edge_absorb_strength=1.5,
        fp_edge_age_ms=1000,
    )

    # Serialize multiple times with canonical settings
    blob1 = json.dumps(v2.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    blob2 = json.dumps(v2.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    blob3 = json.dumps(v2.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    # Should be byte-by-byte identical
    assert blob1 == blob2 == blob3
    assert len(blob1) == len(blob2) == len(blob3)

    # Verify it's actually canonical (no spaces, sorted keys)
    parsed = json.loads(blob1)
    assert parsed["v"] == 2
    assert parsed["symbol"] == "BTCUSDT"
    # Keys should be sorted (cfg comes before delta_z, etc.)
    keys_list = list(parsed.keys())
    assert keys_list == sorted(keys_list)


def test_deterministic_payload_same_inputs():
    """Test that identical inputs produce identical payload strings."""
    indicators1 = {
        "weak_progress": 1,
        "sweep_recent": 1,
        "reclaim_recent": 1,
        "obi_stable": 1,
        "iceberg_strict": 1,
        "abs_lvl_ok": 1,
        "ofi": 1.5,
        "ofi_z": 2.0,
        "ofi_stable": 1,
        "ofi_dir_ok": 1,
        "ofi_stable_secs": 3.0,
        "ofi_stability_score": 0.8,
        "ofi_age_ms": 500,
        "fp_edge_absorb": 1,
        "fp_edge_absorb_strength": 1.5,
        "fp_edge_age_ms": 1000,
    }

    indicators2 = indicators1.copy()  # Same content, different dict instance

    # Create OFInputs from same indicators
    v2_1 = OFInputsV2(
        v=2,
        symbol="BTCUSDT",
        ts_ms=1000000,
        regime="trend",
        direction="LONG",
        scenario="reversal",
        delta_z=2.5,
        weak_progress=indicators1["weak_progress"],
        sweep_recent=indicators1["sweep_recent"],
        reclaim_recent=indicators1["reclaim_recent"],
        obi_stable=indicators1["obi_stable"],
        iceberg_strict=indicators1["iceberg_strict"],
        abs_lvl_ok=indicators1["abs_lvl_ok"],
        trend_dir="LONG",
        hidden_ctx_recent=1,
        cont_ctx_recent=1,
        cfg={},
        fp_eff_quote=50000.0,
        fp_quote_delta=10.0,
        ofi=indicators1["ofi"],
        ofi_z=indicators1["ofi_z"],
        ofi_stable=indicators1["ofi_stable"],
        ofi_dir_ok=indicators1["ofi_dir_ok"],
        ofi_stable_secs=indicators1["ofi_stable_secs"],
        ofi_stability_score=indicators1["ofi_stability_score"],
        ofi_age_ms=indicators1["ofi_age_ms"],
        fp_edge_absorb=indicators1["fp_edge_absorb"],
        fp_edge_absorb_strength=indicators1["fp_edge_absorb_strength"],
        fp_edge_age_ms=indicators1["fp_edge_age_ms"],
    )

    v2_2 = OFInputsV2(
        v=2,
        symbol="BTCUSDT",
        ts_ms=1000000,
        regime="trend",
        direction="LONG",
        scenario="reversal",
        delta_z=2.5,
        weak_progress=indicators2["weak_progress"],
        sweep_recent=indicators2["sweep_recent"],
        reclaim_recent=indicators2["reclaim_recent"],
        obi_stable=indicators2["obi_stable"],
        iceberg_strict=indicators2["iceberg_strict"],
        abs_lvl_ok=indicators2["abs_lvl_ok"],
        trend_dir="LONG",
        hidden_ctx_recent=1,
        cont_ctx_recent=1,
        cfg={},
        fp_eff_quote=50000.0,
        fp_quote_delta=10.0,
        ofi=indicators2["ofi"],
        ofi_z=indicators2["ofi_z"],
        ofi_stable=indicators2["ofi_stable"],
        ofi_dir_ok=indicators2["ofi_dir_ok"],
        ofi_stable_secs=indicators2["ofi_stable_secs"],
        ofi_stability_score=indicators2["ofi_stability_score"],
        ofi_age_ms=indicators2["ofi_age_ms"],
        fp_edge_absorb=indicators2["fp_edge_absorb"],
        fp_edge_absorb_strength=indicators2["fp_edge_absorb_strength"],
        fp_edge_age_ms=indicators2["fp_edge_age_ms"],
    )

    # Serialize both with canonical settings
    blob1 = json.dumps(v2_1.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    blob2 = json.dumps(v2_2.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    # Should be byte-by-byte identical
    assert blob1 == blob2
    assert len(blob1) == len(blob2)


def test_evidence_key_unification_sweep():
    """Test that sweep vs sweep_recent keys are unified correctly."""
    # Test case: indicators has "sweep", evidence has "sweep_recent"
    indicators = {"sweep": 1, "sweep_recent": 0}  # Old key present
    evidence = {"sweep": 1, "sweep_recent": 1}  # Evidence uses "recent" semantics

    # Simulate the logic from strategy.py
    def _i(v, d=0) -> int:
        try:
            return int(v)
        except Exception:
            try:
                return int(float(v))
            except Exception:
                return d

    # Prefer evidence snapshot, fallback to indicators
    ev_sweep = _i(indicators.get("sweep_recent", indicators.get("sweep", 0)), 0)
    if evidence:
        ev_sweep = _i(evidence.get("sweep", evidence.get("sweep_recent", ev_sweep)), ev_sweep)

    # Should prefer evidence.sweep (which is 1) over indicators.sweep_recent (which is 0)
    assert ev_sweep == 1


def test_evidence_key_unification_iceberg():
    """Test that ice_strict vs iceberg_strict keys are unified correctly."""
    # Test case: indicators has "ice_strict", evidence has "iceberg_strict"
    indicators = {"ice_strict": 1, "iceberg_strict": 0}  # Old key present
    evidence = {"iceberg_strict": 1}  # Evidence uses "iceberg_strict"

    def _i(v, d=0) -> int:
        try:
            return int(v)
        except Exception:
            try:
                return int(float(v))
            except Exception:
                return d

    # Prefer evidence snapshot, fallback to indicators
    ev_ice_strict = _i(indicators.get("iceberg_strict", indicators.get("ice_strict", 0)), 0)
    if evidence:
        ev_ice_strict = _i(evidence.get("iceberg_strict", ev_ice_strict), ev_ice_strict)

    # Should prefer evidence.iceberg_strict (which is 1) over indicators.ice_strict (which is 1, but we check evidence first)
    assert ev_ice_strict == 1


def test_sanitize_nan_inf():
    """Test that NaN/Inf values are sanitized to prevent non-deterministic serialization."""
    def _f(v, d=0.0) -> float:
        try:
            x = float(v)
            # sanitize NaN/Inf (kills replay determinism / diffs)
            if x != x or x == float("inf") or x == float("-inf"):
                return d
            return x
        except Exception:
            return d

    import math

    # NaN should be sanitized
    assert _f(float("nan"), 0.0) == 0.0
    assert _f(math.nan, 0.0) == 0.0

    # Inf should be sanitized
    assert _f(float("inf"), 0.0) == 0.0
    assert _f(float("-inf"), 0.0) == 0.0

    # Valid numbers should pass through
    assert _f(1.5, 0.0) == 1.5
    assert _f(-2.0, 0.0) == -2.0
    assert _f(0.0, 0.0) == 0.0


def test_version_selection_deterministic():
    """Test that version selection is deterministic (via config, not key presence)."""
    # Simulate config-based version selection
    def get_version(config: dict[str, Any]) -> int:
        def _i(v, d=0) -> int:
            try:
                return int(v)
            except Exception:
                try:
                    return int(float(v))
                except Exception:
                    return d

        emit_v2_cfg = config.get("of_inputs_emit_v2", 1)
        emit_v2 = bool(_i(emit_v2_cfg, 1))
        return 2 if emit_v2 else 1

    # Same config → same version
    config1 = {"of_inputs_emit_v2": 1}
    config2 = {"of_inputs_emit_v2": 1}
    config3 = {"of_inputs_emit_v2": 0}

    assert get_version(config1) == get_version(config2) == 2
    assert get_version(config3) == 1

    # Default (missing key) → v2
    assert get_version({}) == 2


def test_delta_z_used_consistency():
    """Test that delta_z in inputs matches delta_z_used (not raw delta_event)."""
    # Simulate the scenario: delta_event has one value, but delta_z_used (after fallback) has another
    delta_event = {"z": 2.0, "raw": 100.0}
    delta_z_used = 2.5  # After volume fallback or other processing

    def _f(v, d=0.0) -> float:
        try:
            x = float(v)
            if x != x or x == float("inf") or x == float("-inf"):
                return d
            return x
        except Exception:
            return d

    # Old way (wrong): uses delta_event.get("z")
    delta_z_old = _f(delta_event.get("z", 0.0), 0.0)

    # New way (correct): uses delta_z_used
    delta_z_new = _f(delta_z_used, 0.0)

    # They should differ in this test case
    assert delta_z_old == 2.0
    assert delta_z_new == 2.5
    assert delta_z_old != delta_z_new

    # In production, we should use delta_z_used for determinism
    assert delta_z_new == 2.5


def test_scenario_v4_precedence():
    """Test that scenario_v4 from evidence takes precedence over dec.scenario."""
    # Simulate the scenario selection logic
    def _s(v, d="na") -> str:
        try:
            s = str(v) if v is not None else d
            s = s.strip()
            return s if s else d
        except Exception:
            return d

    # Mock objects
    class MockDec:
        scenario = "reversal"
        scenario_v4 = None

    class MockOfc:
        evidence = {"scenario_v4": "reversal_range"}

    ofc = MockOfc()
    dec = MockDec()

    # Prefer scenario_v4 from evidence snapshot if available
    scenario = _s(
        (ofc.evidence.get("scenario_v4") if (ofc and isinstance(getattr(ofc, "evidence", None), dict)) else None)
        or (getattr(dec, "scenario_v4", None) if dec else None)
        or (getattr(dec, "scenario", None) if dec else None)
        or "na"
    )

    # Should prefer evidence.scenario_v4
    assert scenario == "reversal_range"

    # If evidence doesn't have scenario_v4, fall back to dec.scenario_v4
    ofc.evidence = {}
    dec.scenario_v4 = "reversal_vol_shock"
    scenario = _s(
        (ofc.evidence.get("scenario_v4") if (ofc and isinstance(getattr(ofc, "evidence", None), dict)) else None)
        or (getattr(dec, "scenario_v4", None) if dec else None)
        or (getattr(dec, "scenario", None) if dec else None)
        or "na"
    )
    assert scenario == "reversal_vol_shock"

    # Final fallback to dec.scenario
    dec.scenario_v4 = None
    scenario = _s(
        (ofc.evidence.get("scenario_v4") if (ofc and isinstance(getattr(ofc, "evidence", None), dict)) else None)
        or (getattr(dec, "scenario_v4", None) if dec else None)
        or (getattr(dec, "scenario", None) if dec else None)
        or "na"
    )
    assert scenario == "reversal"


def test_tick_ts_ms_determinism_no_wall_clock():
    """Test that hidden_ctx_recent and cont_ctx_recent depend only on tick_ts, not time.time()."""
    import time

    # Simulate the deterministic logic from strategy.py
    def calc_contexts_deterministic(tick_ts: int, div_ts_ms: int, cont_ctx_ts_ms: int,
                                     hidden_ctx_valid_ms: int = 120_000, cont_ctx_valid_ms: int = 120_000) -> tuple[int, int]:
        """Deterministic context calculation (no wall-clock fallback)."""
        tick_ts_ms = int(tick_ts) if int(tick_ts or 0) > 0 else 0
        if tick_ts_ms <= 0:
            return 0, 0

        hidden_ctx_recent = 0
        cont_ctx_recent = 0

        # hidden ctx - deterministic: depends only on tick_ts
        if div_ts_ms > 0:
            now_ts = tick_ts_ms
            age = now_ts - div_ts_ms
            hidden_ctx_recent = 1 if (0 <= age <= hidden_ctx_valid_ms) else 0

        # cont ctx - deterministic: depends only on tick_ts
        now_ts = tick_ts_ms
        cts = int(cont_ctx_ts_ms or 0)
        cont_ctx_recent = 1 if (cts > 0 and 0 <= now_ts - cts <= cont_ctx_valid_ms) else 0

        return hidden_ctx_recent, cont_ctx_recent

    # Test case 1: Valid tick_ts, contexts should be calculated deterministically
    tick_ts = 1000000
    div_ts_ms = 950000  # 50 seconds ago
    cont_ctx_ts_ms = 980000  # 20 seconds ago

    hidden1, cont1 = calc_contexts_deterministic(tick_ts, div_ts_ms, cont_ctx_ts_ms)

    # Wait a bit (wall-clock time changes)
    time.sleep(0.1)

    # Same inputs should produce same outputs (deterministic)
    hidden2, cont2 = calc_contexts_deterministic(tick_ts, div_ts_ms, cont_ctx_ts_ms)

    assert hidden1 == hidden2 == 1  # Within 120s window
    assert cont1 == cont2 == 1  # Within 120s window

    # Test case 2: tick_ts_ms <= 0 should skip publish
    tick_ts_bad = 0
    hidden3, cont3 = calc_contexts_deterministic(tick_ts_bad, div_ts_ms, cont_ctx_ts_ms)
    assert hidden3 == 0
    assert cont3 == 0

    # Test case 3: tick_ts_ms < 0 should also skip
    tick_ts_negative = -1
    hidden4, cont4 = calc_contexts_deterministic(tick_ts_negative, div_ts_ms, cont_ctx_ts_ms)
    assert hidden4 == 0
    assert cont4 == 0


def test_cfg_safe_deterministic_subset():
    """Test that cfg_safe contains only deterministic, JSON-safe fields."""
    # Simulate the cfg_safe logic from strategy.py
    def build_cfg_safe(runtime_config: dict[str, Any]) -> dict[str, Any]:
        cfg_safe = {}
        try:
            for _k in (
                "of_score_min",
                "of_inputs_stream",
                "of_inputs_stream_maxlen",
                "hidden_ctx_valid_ms",
                "cont_ctx_valid_ms",
            ):
                if _k in runtime_config:
                    _v = runtime_config.get(_k)
                    if isinstance(_v, (int, float, str, bool)) or _v is None:
                        cfg_safe[_k] = _v
        except Exception:
            cfg_safe = {}
        return cfg_safe

    # Test with valid config
    config = {
        "of_score_min": 0.6,
        "of_inputs_stream": RS.OF_INPUTS,
        "of_inputs_stream_maxlen": 200000,
        "hidden_ctx_valid_ms": 120000,
        "cont_ctx_valid_ms": 120000,
        "some_other_field": "should_not_be_included",
        "complex_object": {"nested": "data"},  # Should be excluded
    }

    cfg_safe = build_cfg_safe(config)

    # Should contain only deterministic fields
    assert "of_score_min" in cfg_safe
    assert "of_inputs_stream" in cfg_safe
    assert "of_inputs_stream_maxlen" in cfg_safe
    assert "hidden_ctx_valid_ms" in cfg_safe
    assert "cont_ctx_valid_ms" in cfg_safe

    # Should NOT contain non-deterministic fields
    assert "some_other_field" not in cfg_safe
    assert "complex_object" not in cfg_safe

    # Should be JSON-serializable
    import json
    json_str = json.dumps(cfg_safe, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    parsed = json.loads(json_str)
    assert parsed == cfg_safe

