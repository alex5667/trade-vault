"""
Tests for per-symbol mode override in MLConfirmGate and champion_cfg_validator.

Covers:
- ChampionCfg mode_overrides validation (by_symbol, enforce_share_by_symbol)
- MLConfirmGate per-symbol mode resolution (cfg override > ENV override > global)
- Per-symbol OFF short-circuit
- Per-symbol canary with custom enforce_share
- Backward compatibility (no mode_overrides key)
"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

# Ensure python-worker is on path
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from core.champion_cfg_validator import (
    _validate_mode_overrides,
    validate_champion_cfg,
)

# ---------------------------------------------------------------------------
# champion_cfg_validator: mode_overrides
# ---------------------------------------------------------------------------

class TestValidateModeOverrides(unittest.TestCase):
    """Tests for _validate_mode_overrides()."""

    def test_none_returns_empty(self):
        mo, warns = _validate_mode_overrides(None)
        self.assertEqual(mo.by_symbol, {})
        self.assertEqual(mo.enforce_share_by_symbol, {})
        self.assertEqual(warns, [])

    def test_not_dict_returns_empty_with_warning(self):
        mo, warns = _validate_mode_overrides("bad")
        self.assertEqual(mo.by_symbol, {})
        self.assertEqual(len(warns), 1)
        self.assertIn("expected dict", warns[0])

    def test_valid_by_symbol(self):
        raw = {
            "by_symbol": {
                "BTCUSDT": "ENFORCE",
                "ETHUSDT": "shadow",   # lowercase should be uppercased
                "DOGEUSDT": "OFF",
                "SOLUSDT": "CANARY",
            }
        }
        mo, warns = _validate_mode_overrides(raw)
        self.assertEqual(len(warns), 0)
        self.assertEqual(mo.by_symbol["BTCUSDT"], "ENFORCE")
        self.assertEqual(mo.by_symbol["ETHUSDT"], "SHADOW")
        self.assertEqual(mo.by_symbol["DOGEUSDT"], "OFF")
        self.assertEqual(mo.by_symbol["SOLUSDT"], "CANARY")

    def test_invalid_mode_skipped_with_warning(self):
        raw = {"by_symbol": {"BTCUSDT": "INVALID_MODE"}}
        mo, warns = _validate_mode_overrides(raw)
        self.assertEqual(mo.by_symbol, {})
        self.assertEqual(len(warns), 1)
        self.assertIn("INVALID_MODE", warns[0])

    def test_valid_enforce_share_by_symbol(self):
        raw = {
            "enforce_share_by_symbol": {
                "ETHUSDT": 0.20,
                "SOLUSDT": 0.50,
            }
        }
        mo, warns = _validate_mode_overrides(raw)
        self.assertEqual(len(warns), 0)
        self.assertAlmostEqual(mo.enforce_share_by_symbol["ETHUSDT"], 0.20)
        self.assertAlmostEqual(mo.enforce_share_by_symbol["SOLUSDT"], 0.50)

    def test_enforce_share_out_of_range(self):
        raw = {"enforce_share_by_symbol": {"BTCUSDT": 1.5}}
        mo, warns = _validate_mode_overrides(raw)
        self.assertEqual(mo.enforce_share_by_symbol, {})
        self.assertEqual(len(warns), 1)
        self.assertIn("out of range", warns[0])

    def test_enforce_share_not_parseable(self):
        raw = {"enforce_share_by_symbol": {"BTCUSDT": "abc"}}
        mo, warns = _validate_mode_overrides(raw)
        self.assertEqual(mo.enforce_share_by_symbol, {})
        self.assertEqual(len(warns), 1)
        self.assertIn("cannot parse float", warns[0])

    def test_by_symbol_not_dict(self):
        raw = {"by_symbol": "wrong"}
        mo, warns = _validate_mode_overrides(raw)
        self.assertEqual(mo.by_symbol, {})
        self.assertEqual(len(warns), 1)

    def test_full_combined(self):
        raw = {
            "by_symbol": {
                "BTCUSDT": "ENFORCE",
                "ETHUSDT": "CANARY",
            },
            "enforce_share_by_symbol": {
                "ETHUSDT": 0.25,
            },
        }
        mo, warns = _validate_mode_overrides(raw)
        self.assertEqual(len(warns), 0)
        self.assertEqual(mo.by_symbol["BTCUSDT"], "ENFORCE")
        self.assertEqual(mo.by_symbol["ETHUSDT"], "CANARY")
        self.assertAlmostEqual(mo.enforce_share_by_symbol["ETHUSDT"], 0.25)


class TestChampionCfgWithModeOverrides(unittest.TestCase):
    """Tests for validate_champion_cfg with mode_overrides."""

    _BASE = {
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test_run_001",
        "created_ms": 1700000000000,
        "model_path": "/var/lib/trade/models/champion.joblib",
        "mode": "SHADOW",
        "enforce_share": 0.0,
    }

    def _make_json(self, **overrides):
        obj = {**self._BASE, **overrides}
        return json.dumps(obj)

    def test_no_mode_overrides_backward_compat(self):
        """Without mode_overrides key, should still work (backward compat)."""
        cfg, info = validate_champion_cfg(self._make_json())
        self.assertEqual(cfg.mode, "SHADOW")
        self.assertIsNotNone(cfg.mode_overrides)
        self.assertEqual(cfg.mode_overrides.by_symbol, {})
        self.assertEqual(cfg.mode_overrides.enforce_share_by_symbol, {})

    def test_with_valid_mode_overrides(self):
        cfg, info = validate_champion_cfg(self._make_json(
            mode_overrides={
                "by_symbol": {"BTCUSDT": "ENFORCE", "DOGEUSDT": "OFF"},
                "enforce_share_by_symbol": {"ETHUSDT": 0.15},
            }
        ))
        self.assertEqual(cfg.mode_overrides.by_symbol["BTCUSDT"], "ENFORCE")
        self.assertEqual(cfg.mode_overrides.by_symbol["DOGEUSDT"], "OFF")
        self.assertAlmostEqual(cfg.mode_overrides.enforce_share_by_symbol["ETHUSDT"], 0.15)
        self.assertEqual(info.get("mode_overrides_warnings"), [])

    def test_with_invalid_entries_lenient(self):
        """Invalid entries should produce warnings, not errors."""
        cfg, info = validate_champion_cfg(self._make_json(
            mode_overrides={
                "by_symbol": {"BTCUSDT": "BOGUS"},
            }
        ))
        # Should not raise
        self.assertEqual(cfg.mode_overrides.by_symbol, {})
        warns = info.get("mode_overrides_warnings", [])
        self.assertTrue(len(warns) > 0)
        self.assertIn("BOGUS", warns[0])

    def test_mode_overrides_not_dict_lenient(self):
        """Non-dict mode_overrides should produce warning, not error."""
        cfg, info = validate_champion_cfg(self._make_json(mode_overrides="bad"))
        self.assertEqual(cfg.mode_overrides.by_symbol, {})
        warns = info.get("mode_overrides_warnings", [])
        self.assertTrue(len(warns) > 0)


# ---------------------------------------------------------------------------
# MLConfirmGate: per-symbol mode resolution
# ---------------------------------------------------------------------------

class TestMLConfirmGatePerSymbolMode(unittest.TestCase):
    """Tests for per-symbol mode resolution in MLConfirmGate.check()."""

    def _make_gate(self, mode="SHADOW", cfg=None, mode_overrides=None):
        """Create a minimal MLConfirmGate with mocked internals."""
        # We need to import here because the module has heavy imports
        from services.ml_confirm_gate import MLConfirmGate

        r = MagicMock()
        r.get.return_value = None
        r.xadd = MagicMock()

        gate = MLConfirmGate.__new__(MLConfirmGate)
        gate.r = r
        gate.mode = mode
        gate.fail_policy = "OPEN"
        gate.champion_key = "cfg:ml_confirm:champion"
        gate.challenger_key = "cfg:ml_confirm:challenger"
        gate._cfg_source = "test"
        gate._cfg_hash_key = "cfg:ml_confirm"
        gate._cache_loaded_ms = 999999999999
        gate._cache_ttl_ms = 60000
        gate._cfg = cfg or {}
        gate._cfgs = {}
        gate._models = {}
        gate._cfg_sources = {}
        gate._cfg_keys_used = {}
        gate._mode_by_symbol_by_kind = {}
        gate._enforce_share_by_sym_by_kind = {}
        gate.ab_variant = ""
        gate._model = None
        gate._model_load_error = ""
        gate._last_error_log_ms = 0
        gate._check_call_count = 0
        gate._cfg_key_used = "cfg:ml_confirm:champion"
        gate._cfg_raw_len = 100
        gate._cfg_parse_err = ""
        gate._metrics_stream = "metrics:ml_confirm"
        gate._metrics_enable = False
        gate._metrics_sample = 1.0
        gate._abstain_band = 0.0
        gate._conf_min = 0.0
        gate._abstain_on_missing = False
        gate._p_min_hard_floor = 0.0
        gate._replay_capture = False
        gate._replay_stream = "stream:ml_confirm:inputs"
        gate._replay_sample = 0.01
        gate._replay_maxlen = 200000
        gate._calibrator = None
        gate._calibrate_enabled = False
        gate._calib_type = "none"
        gate._mode_by_symbol = {}
        gate._enforce_share_by_symbol = {}

        # Parse mode_overrides if provided
        if mode_overrides:
            if isinstance(mode_overrides, dict):
                by_sym = mode_overrides.get("by_symbol") or {}
                if isinstance(by_sym, dict):
                    _allowed = {"OFF", "SHADOW", "CANARY", "ENFORCE"}
                    for sym, m in by_sym.items():
                        m_up = str(m).strip().upper()
                        if m_up in _allowed:
                            gate._mode_by_symbol[str(sym).strip().upper()] = m_up
                es_sym = mode_overrides.get("enforce_share_by_symbol") or {}
                if isinstance(es_sym, dict):
                    for sym, share in es_sym.items():
                        try:
                            sv = float(share)
                            if 0.0 <= sv <= 1.0:
                                gate._enforce_share_by_symbol[str(sym).strip().upper()] = sv
                        except (TypeError, ValueError):
                            pass

        # Stub heavy attributes for metrics
        gate._strict_feature_cols = False
        gate._forbid_scenario_v4_onehot = False

        try:
            from unittest.mock import MagicMock as MM
            gate._metrics_events_total = MM()
            gate._metrics_errors_total = MM()
            gate._metrics_latency_seconds = MM()
            gate._metrics_enforce_share = MM()
        except Exception:
            pass

        return gate

    def _check_kwargs(self, symbol="BTCUSDT"):
        return dict(
            symbol=symbol,
            ts_ms=1700000000000,
            direction="LONG",
            scenario="trend",
            indicators={"sid": "test-001"},
            rule_score=0.8,
            rule_have=5,
            rule_need=5,
            cancel_spike_veto=0,
            ok_rule=1,
        )

    def test_global_off_mode(self):
        """mode=OFF globally should return OFF for all symbols."""
        gate = self._make_gate(mode="OFF")
        dec = gate.check(**self._check_kwargs("BTCUSDT"))
        self.assertTrue(dec.allow)
        self.assertEqual(dec.status, "OFF")

    def test_default_no_overrides(self):
        """Without overrides, effective_mode should match global mode."""
        gate = self._make_gate(mode="SHADOW")
        # No cfg loaded → will hit ERR_NO_CFG path, but mode_source should be "global"
        dec = gate.check(**self._check_kwargs("BTCUSDT"))
        # The gate has no model, so it falls through to ERR or no_cfg
        # But effective_mode should default to global
        self.assertIn(dec.mode_source, ("global", ""))

    def test_per_symbol_off_via_config(self):
        """Per-symbol OFF override should short-circuit and allow."""
        gate = self._make_gate(
            mode="ENFORCE",
            mode_overrides={"by_symbol": {"DOGEUSDT": "OFF"}},
        )
        dec = gate.check(**self._check_kwargs("DOGEUSDT"))
        self.assertTrue(dec.allow)
        self.assertEqual(dec.status, "OFF")
        self.assertEqual(dec.effective_mode, "OFF")
        self.assertEqual(dec.mode_source, "cfg_per_symbol")

    def test_per_symbol_enforce_via_config(self):
        """Per-symbol ENFORCE should override global SHADOW for that symbol."""
        gate = self._make_gate(
            mode="SHADOW",
            cfg={"kind": "util_mh_v1"},
            mode_overrides={"by_symbol": {"BTCUSDT": "ENFORCE"}},
        )
        # Will fail on model loading but effective_mode should be set
        dec = gate.check(**self._check_kwargs("BTCUSDT"))
        # The decision should have effective_mode=ENFORCE
        self.assertEqual(dec.effective_mode, "ENFORCE")
        self.assertEqual(dec.mode_source, "cfg_per_symbol")

    def test_per_symbol_does_not_affect_other_symbols(self):
        """Override for BTCUSDT should not affect ETHUSDT."""
        gate = self._make_gate(
            mode="SHADOW",
            mode_overrides={"by_symbol": {"BTCUSDT": "ENFORCE"}},
        )
        dec_btc = gate.check(**self._check_kwargs("BTCUSDT"))
        dec_eth = gate.check(**self._check_kwargs("ETHUSDT"))
        # BTC should be ENFORCE (or OFF per-symbol short-circuit won't apply since it's ENFORCE)
        self.assertEqual(dec_btc.effective_mode, "ENFORCE")
        # ETH should stay SHADOW (global)
        # It will hit ERR_NO_CFG but effective_mode/mode_source should reflect global
        self.assertNotEqual(dec_eth.effective_mode, "ENFORCE")

    @patch.dict(os.environ, {"ML_CONFIRM_MODE__ETHUSDT": "ENFORCE"}, clear=False)
    def test_env_per_symbol_fallback(self):
        """ENV ML_CONFIRM_MODE__SYMBOL should work as fallback."""
        gate = self._make_gate(mode="SHADOW")
        dec = gate.check(**self._check_kwargs("ETHUSDT"))
        # ENV per-symbol should override global
        self.assertEqual(dec.effective_mode, "ENFORCE")
        self.assertEqual(dec.mode_source, "env_per_symbol")

    @patch.dict(os.environ, {"ML_CONFIRM_MODE__BTCUSDT": "SHADOW"}, clear=False)
    def test_config_overrides_env(self):
        """Config per-symbol override should take priority over ENV per-symbol."""
        gate = self._make_gate(
            mode="SHADOW",
            mode_overrides={"by_symbol": {"BTCUSDT": "OFF"}},
        )
        dec = gate.check(**self._check_kwargs("BTCUSDT"))
        # Config says OFF, ENV says SHADOW → config wins
        self.assertEqual(dec.effective_mode, "OFF")
        self.assertEqual(dec.mode_source, "cfg_per_symbol")

    def test_backward_compat_no_mode_overrides_key(self):
        """Gate without mode_overrides in cfg should behave exactly as before."""
        gate = self._make_gate(mode="SHADOW", cfg={"kind": "util_mh_v1"})
        # No overrides set
        self.assertEqual(gate._mode_by_symbol, {})
        self.assertEqual(gate._enforce_share_by_symbol, {})
        dec = gate.check(**self._check_kwargs("BTCUSDT"))
        # Should proceed with global SHADOW
        self.assertIn(dec.mode_source, ("global", ""))


class TestRefreshSelectiveKnobsModeOverrides(unittest.TestCase):
    """Test that _refresh_selective_knobs_from_cfg parses mode_overrides."""

    def test_parses_overrides_from_cfg(self):
        from services.ml_confirm_gate import MLConfirmGate

        gate = MLConfirmGate.__new__(MLConfirmGate)
        gate._cfg = {
            "mode_overrides": {
                "by_symbol": {
                    "BTCUSDT": "ENFORCE",
                    "ethusdt": "SHADOW",  # lowercase
                    "DOGEUSDT": "INVALID",  # should be skipped
                },
                "enforce_share_by_symbol": {
                    "SOLUSDT": 0.30,
                    "XRPUSDT": 1.5,  # out of range, should be skipped
                },
            }
        }
        gate._abstain_band = 0.0
        gate._conf_min = 0.0
        gate._abstain_on_missing = False
        gate._p_min_hard_floor = 0.0
        gate._mode_by_symbol = {}
        gate._enforce_share_by_symbol = {}

        gate._refresh_selective_knobs_from_cfg()

        self.assertEqual(gate._mode_by_symbol["BTCUSDT"], "ENFORCE")
        self.assertEqual(gate._mode_by_symbol["ETHUSDT"], "SHADOW")
        self.assertNotIn("DOGEUSDT", gate._mode_by_symbol)  # INVALID skipped
        self.assertAlmostEqual(gate._enforce_share_by_symbol["SOLUSDT"], 0.30)
        self.assertNotIn("XRPUSDT", gate._enforce_share_by_symbol)  # out of range

    def test_empty_cfg_clears_overrides(self):
        from services.ml_confirm_gate import MLConfirmGate

        gate = MLConfirmGate.__new__(MLConfirmGate)
        gate._mode_by_symbol = {"BTCUSDT": "ENFORCE"}
        gate._enforce_share_by_symbol = {"ETHUSDT": 0.5}
        gate._cfg = {}  # no mode_overrides
        gate._abstain_band = 0.0
        gate._conf_min = 0.0
        gate._abstain_on_missing = False
        gate._p_min_hard_floor = 0.0

        gate._refresh_selective_knobs_from_cfg()

        self.assertEqual(gate._mode_by_symbol, {})
        self.assertEqual(gate._enforce_share_by_symbol, {})


if __name__ == "__main__":
    unittest.main()
