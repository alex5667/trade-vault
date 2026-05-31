import os
import unittest
from unittest.mock import patch

from services.signal_preprocess import preprocess_signal_for_publish
from utils.time_utils import get_ny_time_millis


class TestSignalPreprocess(unittest.TestCase):
    def test_adds_required_fields_and_flags(self):
        sig = {
            "symbol": "btcusdt",
            "confidence": 87.0,  # percent-like
            "ts_ms": get_ny_time_millis(),  # Valid timestamp to avoid bad_ts flag
            "micro": {
                "spread_bps": 15.0,  # > 12.0 new default threshold
                "book_stale_ms": 5000,  # > 1500 new default threshold
            },
            "indicators": {
                "touch_is_stale": True,
                "tick_oood": True,
            },
        }
        preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="CryptoOrderFlow", logger=None)

        self.assertEqual(sig["symbol"], "BTCUSDT")
        self.assertTrue(int(sig["ts_ms"]) > 0)
        self.assertIn("data_quality_flags", sig)
        flags = set(sig["data_quality_flags"])
        # wide_spread: spread_bps=15.0 >= _DQ_SPREAD_WIDE_FLAG_BPS (12.0)
        self.assertIn("wide_spread", flags, f"Expected 'wide_spread' in flags: {sorted(flags)}")
        # stale_l2: book_stale_ms=5000 >= _DQ_BOOK_STALE_FLAG_MS (1500)
        self.assertIn("stale_l2", flags, f"Expected 'stale_l2' in flags: {sorted(flags)}")
        # tick_oood: tick_oood=True
        self.assertIn("tick_oood", flags, f"Expected 'tick_oood' in flags: {sorted(flags)}")
        self.assertAlmostEqual(sig["confidence01"], 0.87, places=6)

    def test_fail_open(self):
        sig = {"symbol": "ETHUSDT", "confidence": "nan", "indicators": "not-a-dict"}
        preprocess_signal_for_publish(sig, symbol="ETHUSDT", source="CryptoOrderFlow", logger=None)
        self.assertEqual(sig["symbol"], "ETHUSDT")
        self.assertIn("ts_ms", sig)


_POLICY_SHADOW = {
    "policy_ver": 1, "level": "symbol", "active_key": "k", "reason_code": "OK",
    "stop_ttl_mode": "shadow", "trailing_mode": "shadow",
    "updated_at_ms": 0, "rollout_stage_stop_ttl": "shadow",
}
_POLICY_ENFORCE = {**_POLICY_SHADOW, "stop_ttl_mode": "live", "rollout_stage_stop_ttl": "enforce"}
_SURFACE_OK = {
    "selected_sl_price": 68000.0, "selected_tp1_price": 72000.0,
    "selected_max_signal_age_ms": 3_600_000,
    "reason_code": "LIVE_SURFACE_OK", "atr_tf_ms": 60000, "atr_value": 500.0,
}


def _patch_phase24(policy, surface, should_apply_decision, rollout=False):
    """Return a list of context managers that mock Phase 2.4 dependencies."""
    return [
        patch("services.signal_preprocess.get_atr_policy_resolver",
              return_value=type("R", (), {"resolve": staticmethod(lambda **_: policy)})()),
        patch("services.signal_preprocess.build_live_risk_surface", return_value=surface),
        patch("services.signal_preprocess.should_apply_live_surface",
              return_value=should_apply_decision),
        patch("services.signal_preprocess.build_rollout_sticky_key", return_value="sticky"),
        patch("services.signal_preprocess.should_apply_rollout", return_value=rollout),
        patch.dict(os.environ, {"ATR_HORIZON_LIVE_SURFACE_ENABLE": "1"}),
    ]


def _get_baseline(sig: dict) -> dict:
    meta = sig.get("meta")
    if not isinstance(meta, dict):
        return {}
    b = meta.get("live_surface_baseline")
    return b if isinstance(b, dict) else {}


class TestLiveSurfaceBaselineCapture(unittest.TestCase):
    """Regression: secondary bug from BLOCKER 3 (2026-05-28).
    Phase 2.4 baseline snapshot used signal.get("sl_price") which is always 0 —
    _calculate_levels stores the result in signal["sl"] / signal["tp_levels"], not
    sl_price / tp1_price. Fix: fall back to sl / tp_levels[0] so that the A/B
    baseline control group for path-tp / bounded-sl / trailing-autocal has real prices.
    """

    def _sig(self, sl=69000.0, tp_levels=None, sl_price=None, tp1_price=None):
        s: dict = {"symbol": "BTCUSDT", "sl": sl, "tp_levels": tp_levels or [71000.0],
                   "ts_ms": get_ny_time_millis()}
        if sl_price is not None:
            s["sl_price"] = sl_price
        if tp1_price is not None:
            s["tp1_price"] = tp1_price
        return s

    def test_baseline_uses_sl_and_tp_levels_when_price_keys_absent(self):
        """Pipeline passes sl/tp_levels after _calculate_levels but no sl_price/tp1_price.
        Baseline must capture sl and tp_levels[0], not zero.
        """
        patches = _patch_phase24(_POLICY_SHADOW, _SURFACE_OK, {"should_apply": False})
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            sig = self._sig()
            preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="CryptoOrderFlow", logger=None)

        baseline = _get_baseline(sig)
        self.assertEqual(baseline.get("sl_price"), 69000.0)
        self.assertEqual(baseline.get("tp1_price"), 71000.0)

    def test_live_override_does_not_corrupt_baseline(self):
        """When apply_live=True the signal gets new sl_price/tp1_price from live surface.
        The baseline (set via setdefault BEFORE the override) must stay at pre-override
        sl/tp_levels[0] so A/B can quantify the override effect.
        """
        patches = _patch_phase24(
            _POLICY_ENFORCE, _SURFACE_OK,
            {"should_apply": True, "reason_code": "LIVE_SURFACE_CANARY_APPLY"},
            rollout=True,
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            sig = self._sig()
            preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="CryptoOrderFlow", logger=None)

        baseline = _get_baseline(sig)
        self.assertEqual(baseline.get("sl_price"), 69000.0, "baseline must be pre-override")
        self.assertEqual(baseline.get("tp1_price"), 71000.0, "baseline must be pre-override")
        self.assertEqual(sig.get("sl_price"), 68000.0, "signal gets live-surface override")
        self.assertEqual(sig.get("tp1_price"), 72000.0, "signal gets live-surface override")

    def test_explicit_sl_price_preferred_over_sl(self):
        """When the caller explicitly sets sl_price/tp1_price, baseline uses those."""
        patches = _patch_phase24(_POLICY_SHADOW, _SURFACE_OK, {"should_apply": False})
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            sig = self._sig(sl_price=68500.0, tp1_price=71500.0)
            preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="CryptoOrderFlow", logger=None)

        baseline = _get_baseline(sig)
        self.assertEqual(baseline.get("sl_price"), 68500.0)
        self.assertEqual(baseline.get("tp1_price"), 71500.0)

    def test_setdefault_is_idempotent(self):
        """When baseline is already present in meta, Phase 2.4 must not overwrite it."""
        patches = _patch_phase24(_POLICY_SHADOW, _SURFACE_OK, {"should_apply": False})
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            sig = self._sig()
            sig["meta"] = {"live_surface_baseline": {"sl_price": 67000.0, "tp1_price": 73000.0}}
            preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="CryptoOrderFlow", logger=None)

        baseline = _get_baseline(sig)
        self.assertEqual(baseline.get("sl_price"), 67000.0)
        self.assertEqual(baseline.get("tp1_price"), 73000.0)


if __name__ == "__main__":
    unittest.main()

