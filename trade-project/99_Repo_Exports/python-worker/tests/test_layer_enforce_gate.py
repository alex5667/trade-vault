from __future__ import annotations

import json
import os
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from handlers.crypto_orderflow.components.layer_enforce_gate import (
    EnforceInputs,
    _eval_layer_a,
    _eval_layer_b,
    _eval_layer_c,
    _is_default_fallback_slip,
    evaluate,
)
from handlers.crypto_orderflow.components.layer_enforce_reader import (
    LayerEnforceReader,
    LayerEnforceState,
    LayerEnforceStates,
    _verify_hmac,
)


def _inp(
    symbol="BTCUSDT",
    side="LONG",
    slip=None,
    spread=None,
    regime="uptrend",
    features=None,
) -> EnforceInputs:
    return EnforceInputs(
        symbol=symbol,
        side=side,
        slippage_bps_est=slip,
        spread_bps_at_entry=spread,
        regime=regime,
        features=features or {},
    )


def _reader_with_states(states: LayerEnforceStates) -> LayerEnforceReader:
    r = LayerEnforceReader(redis_client=None, secret="")
    r._cached = states
    r._cached_at = time.monotonic()
    return r


def _state(mode="prod", canary_symbols=()) -> LayerEnforceState:
    return LayerEnforceState(
        mode=mode,
        canary_symbols=tuple(canary_symbols),
        bundle_valid=True,
    )


# ─────────────────────────── Layer A ─────────────────────────────────────────

class TestLayerA(unittest.TestCase):

    def test_no_veto_when_slip_and_spread_below_threshold(self):
        inp = _inp(slip=1.0, spread=1.0)
        with patch.dict(os.environ, {
            "OF_LAYER_A_ENFORCE_SLIPPAGE_BPS": "2.0",
            "OF_LAYER_A_ENFORCE_SPREAD_BPS": "1.5",
            "EDGE_SLIPPAGE_BPS_DEFAULT": "4.0",
        }):
            veto, reasons = _eval_layer_a(inp)
        self.assertFalse(veto)
        self.assertEqual(reasons, [])

    def test_veto_on_high_slippage(self):
        inp = _inp(slip=2.5, spread=0.5)
        with patch.dict(os.environ, {
            "OF_LAYER_A_ENFORCE_SLIPPAGE_BPS": "2.0",
            "OF_LAYER_A_ENFORCE_SPREAD_BPS": "1.5",
            "EDGE_SLIPPAGE_BPS_DEFAULT": "4.0",
            "OF_LAYER_ENFORCE_SLIP_DEFAULT_TOL_BPS": "0.05",
        }):
            veto, reasons = _eval_layer_a(inp)
        self.assertTrue(veto)
        self.assertIn("la_slippage", reasons)

    def test_veto_on_high_spread(self):
        inp = _inp(slip=0.5, spread=2.0)
        with patch.dict(os.environ, {
            "OF_LAYER_A_ENFORCE_SLIPPAGE_BPS": "2.0",
            "OF_LAYER_A_ENFORCE_SPREAD_BPS": "1.5",
            "EDGE_SLIPPAGE_BPS_DEFAULT": "4.0",
        }):
            veto, reasons = _eval_layer_a(inp)
        self.assertTrue(veto)
        self.assertIn("la_spread", reasons)

    def test_both_veto_reasons(self):
        inp = _inp(slip=3.0, spread=2.0)
        with patch.dict(os.environ, {
            "OF_LAYER_A_ENFORCE_SLIPPAGE_BPS": "2.0",
            "OF_LAYER_A_ENFORCE_SPREAD_BPS": "1.5",
            "EDGE_SLIPPAGE_BPS_DEFAULT": "99.0",
        }):
            veto, reasons = _eval_layer_a(inp)
        self.assertTrue(veto)
        self.assertIn("la_slippage", reasons)
        self.assertIn("la_spread", reasons)

    def test_no_veto_when_slip_none(self):
        inp = _inp(slip=None, spread=0.5)
        with patch.dict(os.environ, {
            "OF_LAYER_A_ENFORCE_SLIPPAGE_BPS": "2.0",
            "EDGE_SLIPPAGE_BPS_DEFAULT": "4.0",
        }):
            veto, reasons = _eval_layer_a(inp)
        self.assertFalse(veto)

    def test_ema_fallback_slip_skips_rule(self):
        """Slippage == default_bps (EMA not loaded) must not trigger veto."""
        default = 4.0
        inp = _inp(slip=default, spread=0.5)
        with patch.dict(os.environ, {
            "OF_LAYER_A_ENFORCE_SLIPPAGE_BPS": "2.0",
            "EDGE_SLIPPAGE_BPS_DEFAULT": str(default),
            "OF_LAYER_ENFORCE_SLIP_DEFAULT_TOL_BPS": "0.05",
        }):
            veto, reasons = _eval_layer_a(inp)
        self.assertFalse(veto, "EMA fallback slip must not trigger la_slippage")
        self.assertNotIn("la_slippage", reasons)


class TestIsDefaultFallbackSlip(unittest.TestCase):

    def test_exact_default_is_fallback(self):
        with patch.dict(os.environ, {
            "EDGE_SLIPPAGE_BPS_DEFAULT": "4.0",
            "OF_LAYER_ENFORCE_SLIP_DEFAULT_TOL_BPS": "0.05",
        }):
            self.assertTrue(_is_default_fallback_slip(4.0))
            self.assertTrue(_is_default_fallback_slip(4.04))
            self.assertFalse(_is_default_fallback_slip(4.1))
            self.assertFalse(_is_default_fallback_slip(2.0))


# ─────────────────────────── Layer B ─────────────────────────────────────────

class TestLayerB(unittest.TestCase):

    def _env_b(self) -> dict:
        return {
            "OF_LAYER_B_ENFORCE_SLIP_LO": "1.0",
            "OF_LAYER_B_ENFORCE_SLIP_HI": "2.0",
            "OF_LAYER_B_ENFORCE_SLIP_CLAMP": "0.5",
            "OF_LAYER_B_ENFORCE_SPR_LO": "0.8",
            "OF_LAYER_B_ENFORCE_SPR_HI": "1.5",
            "OF_LAYER_B_ENFORCE_SPR_CLAMP": "0.5",
            "OF_LAYER_B_ENFORCE_LONG_CLAMP": "0.7",
            "OF_LAYER_B_ENFORCE_CONFIRM_LONG": "uptrend,trend_up",
            "OF_LAYER_B_ENFORCE_MIN_CLAMP": "0.2",
            "EDGE_SLIPPAGE_BPS_DEFAULT": "4.0",
            "OF_LAYER_ENFORCE_SLIP_DEFAULT_TOL_BPS": "0.05",
        }

    def test_no_clamp_below_all_thresholds(self):
        inp = _inp(slip=0.5, spread=0.5, regime="uptrend", side="LONG")
        with patch.dict(os.environ, self._env_b()):
            factor, reasons = _eval_layer_b(inp)
        self.assertAlmostEqual(factor, 1.0)
        self.assertEqual(reasons, [])

    def test_slip_clamp_in_mid_range(self):
        inp = _inp(slip=1.5, spread=0.3, regime="uptrend", side="LONG")
        with patch.dict(os.environ, self._env_b()):
            factor, reasons = _eval_layer_b(inp)
        self.assertAlmostEqual(factor, 0.5)
        self.assertIn("lb_slip_mid", reasons)

    def test_spread_clamp_in_mid_range(self):
        inp = _inp(slip=0.3, spread=1.0, regime="uptrend", side="LONG")
        with patch.dict(os.environ, self._env_b()):
            factor, reasons = _eval_layer_b(inp)
        self.assertAlmostEqual(factor, 0.5)
        self.assertIn("lb_spr_mid", reasons)

    def test_long_no_htf_clamp(self):
        inp = _inp(slip=0.3, spread=0.3, regime="downtrend", side="LONG")
        with patch.dict(os.environ, self._env_b()):
            factor, reasons = _eval_layer_b(inp)
        self.assertAlmostEqual(factor, 0.7)
        self.assertIn("lb_long_no_htf", reasons)

    def test_compound_clamp_floored_at_min(self):
        # slip_clamp(0.5) × spr_clamp(0.5) × long_clamp(0.7) = 0.175 → clamped to 0.2
        inp = _inp(slip=1.5, spread=1.0, regime="downtrend", side="LONG")
        with patch.dict(os.environ, self._env_b()):
            factor, reasons = _eval_layer_b(inp)
        self.assertAlmostEqual(factor, 0.2)

    def test_short_not_affected_by_long_clamp(self):
        inp = _inp(slip=0.3, spread=0.3, regime="downtrend", side="SHORT")
        with patch.dict(os.environ, self._env_b()):
            factor, reasons = _eval_layer_b(inp)
        self.assertAlmostEqual(factor, 1.0)
        self.assertNotIn("lb_long_no_htf", reasons)

    def test_ema_fallback_slip_skips_clamp(self):
        """Slippage == default_bps must not trigger lb_slip_mid."""
        inp = _inp(slip=4.0, spread=0.3, regime="uptrend", side="LONG")
        with patch.dict(os.environ, {**self._env_b(),
                                     "EDGE_SLIPPAGE_BPS_DEFAULT": "4.0"}):
            factor, reasons = _eval_layer_b(inp)
        self.assertAlmostEqual(factor, 1.0)
        self.assertNotIn("lb_slip_mid", reasons)


# ─────────────────────────── Layer C ─────────────────────────────────────────

class TestLayerC(unittest.TestCase):

    def _env_c(self) -> dict:
        return {
            "OF_LAYER_C_ENFORCE_MIN_LEGS": "2",
            "OF_LAYER_C_ENFORCE_LEG1_KEY": "qimb_wmean",
            "OF_LAYER_C_ENFORCE_LEG1_THR": "1.0",
            "OF_LAYER_C_ENFORCE_LEG1_ENABLED": "1",
            "OF_LAYER_C_ENFORCE_LEG2_KEY": "lob_dw_obi_z",
            "OF_LAYER_C_ENFORCE_LEG2_THR": "1.5",
            "OF_LAYER_C_ENFORCE_LEG2_ENABLED": "1",
            "OF_LAYER_C_ENFORCE_LEG3_KEY": "liq_pressure_boost",
            "OF_LAYER_C_ENFORCE_LEG3_THR": "1.0",
            "OF_LAYER_C_ENFORCE_LEG3_ENABLED": "1",
            "OF_LAYER_C_ENFORCE_LEG4_ENABLED": "1",
            "OF_LAYER_C_ENFORCE_CONFIRM_LONG": "uptrend,trend_up",
            "OF_LAYER_C_ENFORCE_CONFIRM_SHORT": "downtrend,trend_down",
        }

    def test_pass_when_enough_legs_long(self):
        feats = {"qimb_wmean": 1.5, "lob_dw_obi_z": 2.0}
        inp = _inp(side="LONG", regime="uptrend", features=feats)
        with patch.dict(os.environ, self._env_c()):
            veto, reasons, active, _ = _eval_layer_c(inp)
        self.assertFalse(veto)
        self.assertGreaterEqual(active, 2)

    def test_veto_when_insufficient_legs(self):
        feats = {"qimb_wmean": 0.1, "lob_dw_obi_z": 0.1, "liq_pressure_boost": 0.1}
        inp = _inp(side="LONG", regime="downtrend", features=feats)
        with patch.dict(os.environ, self._env_c()):
            veto, reasons, active, _ = _eval_layer_c(inp)
        self.assertTrue(veto)
        self.assertTrue(any("lc_legs_lt_min" in r for r in reasons))

    def test_short_directional_legs(self):
        feats = {"qimb_wmean": -1.5, "lob_dw_obi_z": -2.0}
        inp = _inp(side="SHORT", regime="downtrend", features=feats)
        with patch.dict(os.environ, self._env_c()):
            veto, reasons, active, _ = _eval_layer_c(inp)
        self.assertFalse(veto)
        self.assertGreaterEqual(active, 2)

    def test_missing_features_are_absent(self):
        """Missing feature key → not counted as present."""
        feats = {"qimb_wmean": 2.0}  # only 1 of 3 numeric legs present
        inp = _inp(side="LONG", regime="other", features=feats)
        with patch.dict(os.environ, {**self._env_c(),
                                     "OF_LAYER_C_ENFORCE_MIN_LEGS": "3"}):
            veto, reasons, active, _ = _eval_layer_c(inp)
        self.assertTrue(veto)

    def test_disabled_legs_not_counted(self):
        feats = {"qimb_wmean": 2.0, "lob_dw_obi_z": 2.0}
        inp = _inp(side="LONG", regime="uptrend", features=feats)
        with patch.dict(os.environ, {**self._env_c(),
                                     "OF_LAYER_C_ENFORCE_LEG3_ENABLED": "0",
                                     "OF_LAYER_C_ENFORCE_LEG4_ENABLED": "0",
                                     "OF_LAYER_C_ENFORCE_MIN_LEGS": "2"}):
            veto, reasons, active, _ = _eval_layer_c(inp)
        self.assertFalse(veto)
        self.assertEqual(active, 2)

    def test_hot_key_override_from_reader(self):
        """Layer C leg key overridable via reader without restart."""
        feats = {"alt_obi_key": 2.0, "lob_dw_obi_z": 2.0}
        inp = _inp(side="LONG", regime="uptrend", features=feats)
        mock_reader = MagicMock()
        mock_reader.get_leg_key_override.side_effect = lambda layer, leg: (
            "alt_obi_key" if leg == 1 else None
        )
        with patch.dict(os.environ, {**self._env_c(),
                                     "OF_LAYER_C_ENFORCE_MIN_LEGS": "2",
                                     "OF_LAYER_C_ENFORCE_LEG4_ENABLED": "0"}):
            veto, reasons, active, _ = _eval_layer_c(inp, reader=mock_reader)
        self.assertFalse(veto)
        mock_reader.get_leg_key_override.assert_called()


# ─────────────────────────── LayerEnforceReader ───────────────────────────────

class TestLayerEnforceReader(unittest.TestCase):

    def test_get_for_symbol_returns_none_when_all_off(self):
        r = _reader_with_states(LayerEnforceStates(
            a=_state("off"), b=_state("off"), c=_state("off")
        ))
        self.assertIsNone(r.get_for_symbol("A", "BTCUSDT"))
        self.assertIsNone(r.get_for_symbol("B", "ETHUSDT"))
        self.assertIsNone(r.get_for_symbol("C", "SOLUSDT"))

    def test_get_for_symbol_returns_state_in_prod(self):
        r = _reader_with_states(LayerEnforceStates(
            a=_state("prod"), b=_state("off"), c=_state("off")
        ))
        state = r.get_for_symbol("A", "BTCUSDT")
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state.mode, "prod")

    def test_canary_mode_allows_canary_symbol(self):
        r = _reader_with_states(LayerEnforceStates(
            a=_state("canary", ("BTCUSDT", "ETHUSDT")),
            b=_state("off"), c=_state("off")
        ))
        self.assertIsNotNone(r.get_for_symbol("A", "BTCUSDT"))
        self.assertIsNotNone(r.get_for_symbol("A", "ETHUSDT"))

    def test_canary_mode_blocks_non_canary_symbol(self):
        r = _reader_with_states(LayerEnforceStates(
            a=_state("canary", ("BTCUSDT",)),
            b=_state("off"), c=_state("off")
        ))
        self.assertIsNone(r.get_for_symbol("A", "SOLUSDT"))

    def test_cache_ttl_respected(self):
        r = LayerEnforceReader(redis_client=None, secret="", cache_ttl_sec=0.1)
        r._cached = LayerEnforceStates(a=_state("prod"), b=_state("off"), c=_state("off"))
        r._cached_at = time.monotonic()
        self.assertIsNotNone(r.get_for_symbol("A", "BTCUSDT"))
        time.sleep(0.15)
        # cache expired → fetch from redis=None → returns default (off)
        result = r.get_for_symbol("A", "BTCUSDT")
        self.assertIsNone(result)

    def test_unknown_layer_returns_none(self):
        r = _reader_with_states(LayerEnforceStates(
            a=_state("prod"), b=_state("prod"), c=_state("prod")
        ))
        self.assertIsNone(r.get_for_symbol("D", "BTCUSDT"))
        self.assertIsNone(r.get_for_symbol("X", "BTCUSDT"))

    def test_thread_safety(self):
        r = _reader_with_states(LayerEnforceStates(
            a=_state("prod"), b=_state("off"), c=_state("off")
        ))
        results: list[bool] = []

        def _read():
            state = r.get_for_symbol("A", "BTCUSDT")
            results.append(state is not None)

        threads = [threading.Thread(target=_read) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(results), 20)
        self.assertTrue(all(results))


class TestVerifyHmac(unittest.TestCase):

    def _bundle_sig(self, bundle: dict, secret: str) -> str:
        import hashlib, hmac as _hmac
        canonical = json.dumps(bundle, sort_keys=True, separators=(",", ":")).encode()
        return _hmac.new(secret.encode(), canonical, hashlib.sha256).hexdigest()

    def test_valid_signature_passes(self):
        bundle = {"threshold_slip": 2.0, "layer": "A"}
        sig = self._bundle_sig(bundle, "mysecret")
        self.assertTrue(_verify_hmac(json.dumps(bundle), sig, "mysecret"))

    def test_wrong_secret_fails(self):
        bundle = {"threshold_slip": 2.0}
        sig = self._bundle_sig(bundle, "secret_a")
        self.assertFalse(_verify_hmac(json.dumps(bundle), sig, "secret_b"))

    def test_tampered_bundle_fails(self):
        bundle = {"threshold_slip": 2.0}
        sig = self._bundle_sig(bundle, "mysecret")
        tampered = json.dumps({"threshold_slip": 99.0})
        self.assertFalse(_verify_hmac(tampered, sig, "mysecret"))

    def test_empty_inputs_return_false(self):
        self.assertFalse(_verify_hmac("", "abc", "secret"))
        self.assertFalse(_verify_hmac("{}", "", "secret"))
        self.assertFalse(_verify_hmac("{}", "abc", ""))


# ─────────────────────────── evaluate() integration ──────────────────────────

class TestEvaluateIntegration(unittest.TestCase):

    def _prod_states(self) -> LayerEnforceStates:
        return LayerEnforceStates(
            a=_state("prod"), b=_state("prod"), c=_state("prod")
        )

    def _off_states(self) -> LayerEnforceStates:
        return LayerEnforceStates(
            a=_state("off"), b=_state("off"), c=_state("off")
        )

    def test_all_off_no_veto(self):
        reader = _reader_with_states(self._off_states())
        inp = _inp(slip=5.0, spread=5.0)
        with patch.dict(os.environ, {"EDGE_SLIPPAGE_BPS_DEFAULT": "99.0"}):
            res = evaluate(reader, inp)
        self.assertFalse(res.veto)
        self.assertAlmostEqual(res.clamp_factor, 1.0)

    def test_layer_a_veto_in_prod(self):
        reader = _reader_with_states(LayerEnforceStates(
            a=_state("prod"), b=_state("off"), c=_state("off")
        ))
        inp = _inp(slip=3.0, spread=0.5)
        with patch.dict(os.environ, {
            "OF_LAYER_A_ENFORCE_SLIPPAGE_BPS": "2.0",
            "OF_LAYER_A_ENFORCE_SPREAD_BPS": "1.5",
            "EDGE_SLIPPAGE_BPS_DEFAULT": "99.0",
        }):
            res = evaluate(reader, inp)
        self.assertTrue(res.veto)
        self.assertIn("la_slippage", res.veto_reasons)
        self.assertTrue(res.layer_a_active)
        self.assertFalse(res.layer_b_active)

    def test_layer_b_clamp_no_veto(self):
        reader = _reader_with_states(LayerEnforceStates(
            a=_state("off"), b=_state("prod"), c=_state("off")
        ))
        inp = _inp(slip=1.5, spread=0.3, regime="uptrend", side="LONG")
        with patch.dict(os.environ, {
            "OF_LAYER_B_ENFORCE_SLIP_LO": "1.0",
            "OF_LAYER_B_ENFORCE_SLIP_HI": "2.0",
            "OF_LAYER_B_ENFORCE_SLIP_CLAMP": "0.5",
            "OF_LAYER_B_ENFORCE_SPR_LO": "0.8",
            "OF_LAYER_B_ENFORCE_SPR_HI": "1.5",
            "OF_LAYER_B_ENFORCE_SPR_CLAMP": "0.5",
            "OF_LAYER_B_ENFORCE_LONG_CLAMP": "0.7",
            "OF_LAYER_B_ENFORCE_CONFIRM_LONG": "uptrend,trend_up",
            "OF_LAYER_B_ENFORCE_MIN_CLAMP": "0.2",
            "EDGE_SLIPPAGE_BPS_DEFAULT": "99.0",
        }):
            res = evaluate(reader, inp)
        self.assertFalse(res.veto)
        self.assertAlmostEqual(res.clamp_factor, 0.5)
        self.assertTrue(res.layer_b_active)

    def test_result_notes_contain_modes(self):
        reader = _reader_with_states(LayerEnforceStates(
            a=_state("prod"), b=_state("canary", ("BTCUSDT",)), c=_state("off")
        ))
        inp = _inp(slip=0.5, spread=0.5, side="LONG", regime="uptrend")
        res = evaluate(reader, inp)
        self.assertIsNotNone(res.notes)
        assert res.notes is not None
        self.assertEqual(res.notes["layer_a_mode"], "prod")
        self.assertEqual(res.notes["layer_c_mode"], "off")

    def test_canary_symbol_gets_enforcement(self):
        reader = _reader_with_states(LayerEnforceStates(
            a=_state("canary", ("BTCUSDT",)), b=_state("off"), c=_state("off")
        ))
        with patch.dict(os.environ, {
            "OF_LAYER_A_ENFORCE_SLIPPAGE_BPS": "2.0",
            "OF_LAYER_A_ENFORCE_SPREAD_BPS": "1.5",
            "EDGE_SLIPPAGE_BPS_DEFAULT": "99.0",
        }):
            res_btc = evaluate(reader, _inp(symbol="BTCUSDT", slip=3.0, spread=0.5))
            res_eth = evaluate(reader, _inp(symbol="ETHUSDT", slip=3.0, spread=0.5))
        self.assertTrue(res_btc.layer_a_active)
        self.assertFalse(res_eth.layer_a_active)
        self.assertTrue(res_btc.veto)
        self.assertFalse(res_eth.veto)


if __name__ == "__main__":
    unittest.main()
