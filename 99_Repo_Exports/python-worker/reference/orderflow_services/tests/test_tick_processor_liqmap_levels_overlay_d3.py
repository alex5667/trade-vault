"""D3: wiring + ENV-flags test for LiqMap TP/SL overlay in _calculate_levels() (mirror).

D3 changes:
  - Canonical ENV names normalised; back-compat aliases supported.
  - New canonical names:  LIQMAP_LEVELS_SL_MAX_WIDEN_BPS, LIQMAP_LEVELS_TP1_ENABLE
                          LIQMAP_LEVELS_SL_ENABLE
  - Legacy aliases:       LIQMAP_LEVELS_MAX_SL_WIDEN_BPS → SL_MAX_WIDEN_BPS
                          LIQMAP_LEVELS_ENABLE_TP1       → TP1_ENABLE
                          LIQMAP_LEVELS_ENABLE_SL        → SL_ENABLE

Mirror of: tick_flow_full/services/orderflow/tests/test_tick_processor_liqmap_levels_overlay_d2.py

Checks:
  - LIQMAP_LEVELS_ENABLE=1 + peak present → TP1 anchored before the peak.
  - LIQMAP_LEVELS_ENABLE=0 → no-op, base TP1 preserved, no overlay keys in indicators.
  - D3: SL_MAX_WIDEN_BPS default is 20 (was 30 in D2).
  - D3: Legacy aliases are honoured when new names are absent.
"""
import os


from services.orderflow.components.tick_processor import TickProcessor


class DummyRuntime:
    def __init__(self):
        self.symbol = "BTCUSDT"
        self.config = {
            # keep it minimal and deterministic
            "stop_mode": "ATR"
            "stop_atr_mult": 1.0
            "tp_rr": "1.3,2.0,2.7"
            "min_lot": 0.01
            "max_lot": 0.01
        }
        self.dynamic_cfg = {}
        self.calibrated_specs = {}

    def get_atr_tf_selected(self):
        return "1m"


def _clean_liqmap_env():
    """Remove all LiqMap levels overlay env vars to ensure test isolation."""
    for k in (
        "LIQMAP_LEVELS_ENABLE"
        "LIQMAP_LEVELS_WINDOW"
        "LIQMAP_LEVELS_MIN_USD"
        "LIQMAP_LEVELS_BUFFER_BPS"
        # D3 canonical
        "LIQMAP_LEVELS_SL_MAX_WIDEN_BPS"
        "LIQMAP_LEVELS_TP1_ENABLE"
        "LIQMAP_LEVELS_SL_ENABLE"
        # D2 legacy aliases
        "LIQMAP_LEVELS_MAX_SL_WIDEN_BPS"
        "LIQMAP_LEVELS_ENABLE_TP1"
        "LIQMAP_LEVELS_ENABLE_SL"
    ):
        os.environ.pop(k, None)


def _mk_tp(overlay_enable: bool):
    """Build TickProcessor using D3 canonical ENV names."""
    _clean_liqmap_env()
    os.environ["LIQMAP_LEVELS_ENABLE"] = "1" if overlay_enable else "0"
    os.environ["LIQMAP_LEVELS_WINDOW"] = "1h"
    os.environ["LIQMAP_LEVELS_MIN_USD"] = "250000"
    os.environ["LIQMAP_LEVELS_BUFFER_BPS"] = "5"
    # D3 canonical names:
    os.environ["LIQMAP_LEVELS_SL_MAX_WIDEN_BPS"] = "20"
    os.environ["LIQMAP_LEVELS_TP1_ENABLE"] = "1"
    os.environ["LIQMAP_LEVELS_SL_ENABLE"] = "0"
    return TickProcessor(
        redis=None
        ticks=None
        publisher=None
        of_engine=None
        calib_svc=None
        atr_cache=None
        atr_sanity=None
        conf_scorer=None
    )


def test_liqmap_levels_overlay_tp1_adjusts_when_enabled():
    # Hard dependency: D1 must provide this helper.
    from services.orderflow.liqmap_features import apply_liqmap_tp_sl_adjustment

    assert callable(apply_liqmap_tp_sl_adjustment)

    tp = _mk_tp(overlay_enable=True)
    rt = DummyRuntime()

    entry = 100.0
    indicators = {
        "atr": 1.0
        # Derived peak_up_price = entry * (1 + 100bps) = 101.0
        # which sits between entry and base_tp1 (~101.3) → should anchor TP1 before peak.
        "liqmap_1h_dist_up_bps": 100.0
        "liqmap_1h_peak_up1_usd": 500000.0
    }

    sl, tps, lot, atr = tp._calculate_levels(rt, entry, "LONG", indicators, trail_profile="classic")

    assert tps[0] > entry  # TP must remain on correct side
    assert tps[0] < 101.3  # anchored before peak vs base_tp1
    assert abs(float(indicators.get("liqmap_levels_applied", 0)) - 1.0) < 1e-9
    # SL overlay is disabled by D3 defaults (SL_ENABLE=0)
    assert sl < entry


def test_liqmap_levels_overlay_disabled_is_noop():
    tp = _mk_tp(overlay_enable=False)
    rt = DummyRuntime()

    entry = 100.0
    indicators = {
        "atr": 1.0
        "liqmap_1h_dist_up_bps": 100.0
        "liqmap_1h_peak_up1_usd": 500000.0
    }

    sl, tps, lot, atr = tp._calculate_levels(rt, entry, "LONG", indicators, trail_profile="classic")

    assert abs(tps[0] - 101.3) < 1e-6
    assert "liqmap_levels_applied" not in indicators


def test_d3_sl_max_widen_bps_default_is_20():
    """D3: default SL_MAX_WIDEN_BPS changed from 30 (D2) to 20 (D3 prod standard)."""
    _clean_liqmap_env()
    # No SL_MAX_WIDEN_BPS / MAX_SL_WIDEN_BPS set → must default to 20.
    tp = TickProcessor(
        redis=None, ticks=None, publisher=None
        of_engine=None, calib_svc=None, atr_cache=None
        atr_sanity=None, conf_scorer=None
    )
    assert tp.liqmap_levels_max_sl_widen_bps == 20.0


def test_d3_legacy_alias_max_sl_widen_honoured():
    """D3 back-compat: legacy LIQMAP_LEVELS_MAX_SL_WIDEN_BPS is read when new name absent."""
    _clean_liqmap_env()
    os.environ["LIQMAP_LEVELS_MAX_SL_WIDEN_BPS"] = "15"
    # New name absent → legacy alias wins.
    tp = TickProcessor(
        redis=None, ticks=None, publisher=None
        of_engine=None, calib_svc=None, atr_cache=None
        atr_sanity=None, conf_scorer=None
    )
    assert tp.liqmap_levels_max_sl_widen_bps == 15.0
    _clean_liqmap_env()


def test_d3_new_name_beats_legacy_alias():
    """D3 priority: new canonical name takes priority over legacy alias."""
    _clean_liqmap_env()
    os.environ["LIQMAP_LEVELS_SL_MAX_WIDEN_BPS"] = "25"
    os.environ["LIQMAP_LEVELS_MAX_SL_WIDEN_BPS"] = "99"  # should be ignored
    tp = TickProcessor(
        redis=None, ticks=None, publisher=None
        of_engine=None, calib_svc=None, atr_cache=None
        atr_sanity=None, conf_scorer=None
    )
    assert tp.liqmap_levels_max_sl_widen_bps == 25.0
    _clean_liqmap_env()


def test_d3_legacy_alias_enable_tp1_honoured():
    """D3 back-compat: LIQMAP_LEVELS_ENABLE_TP1=0 is respected when new name absent."""
    _clean_liqmap_env()
    os.environ["LIQMAP_LEVELS_ENABLE_TP1"] = "0"
    tp = TickProcessor(
        redis=None, ticks=None, publisher=None
        of_engine=None, calib_svc=None, atr_cache=None
        atr_sanity=None, conf_scorer=None
    )
    assert tp.liqmap_levels_enable_tp1 is False
    _clean_liqmap_env()


def test_d3_legacy_alias_enable_sl_honoured():
    """D3 back-compat: LIQMAP_LEVELS_ENABLE_SL=1 is respected when new name absent."""
    _clean_liqmap_env()
    os.environ["LIQMAP_LEVELS_ENABLE_SL"] = "1"
    tp = TickProcessor(
        redis=None, ticks=None, publisher=None
        of_engine=None, calib_svc=None, atr_cache=None
        atr_sanity=None, conf_scorer=None
    )
    assert tp.liqmap_levels_enable_sl is True
    _clean_liqmap_env()
