import os
import unittest
from unittest.mock import patch
from core.instrument_config import (
    get_default_delta_tiers, 
    get_default_book_rate_settings, 
    OrderFlowConfig
)

class TestCalibrationConfig(unittest.TestCase):
    def test_get_default_delta_tiers_btc(self):
        tiers = get_default_delta_tiers("BTCUSDT")
        self.assertEqual(tiers["tier0"], 3_100_000.0)
        self.assertEqual(tiers["tier1"], 6_600_000.0)
        self.assertEqual(tiers["tier2"], 8_700_000.0)

    def test_get_default_delta_tiers_eth(self):
        tiers = get_default_delta_tiers("ETHUSDT")
        self.assertEqual(tiers["tier0"], 1_500_000.0)

    def test_get_default_delta_tiers_meme(self):
        tiers = get_default_delta_tiers("1000PEPEUSDT")
        self.assertEqual(tiers["tier0"], 250_000.0)

    def test_get_default_delta_tiers_unknown(self):
        tiers = get_default_delta_tiers("UNKNOWNUSDT")
        # Should fallback to defaults
        self.assertEqual(tiers["tier0"], 100_000.0)

    def test_orderflow_config_has_fields(self):
        cfg = OrderFlowConfig(symbol="TEST")
        self.assertTrue(hasattr(cfg, "dn_tier0_usd"))
        self.assertTrue(hasattr(cfg, "dn_tier1_usd"))
        self.assertTrue(hasattr(cfg, "book_rate_min_hz"))
        # Check defaults
        self.assertEqual(cfg.dn_tier0_usd, 0.0)

    def test_from_env_populates_defaults(self):
        # When calling from_env with no env vars, it should use get_default_delta_tiers
        with patch.dict(os.environ, {}, clear=True):
            cfg = OrderFlowConfig.from_env("XRPUSDT")
            # XRP defaults: 800k, 1.6M, 2.4M
            self.assertEqual(cfg.dn_tier0_usd, 800_000.0)
            self.assertEqual(cfg.dn_tier1_usd, 1_600_000.0)
            
            # Check book rate defaults (min=10.0 for XRP, warn=5.0)
            self.assertEqual(cfg.book_rate_min_hz, 10.0)

    def test_from_env_overrides(self):
        # Override XRP tier0
        with patch.dict(os.environ, {"XRP_DN_TIER0_USD": "999.0"}):
            cfg = OrderFlowConfig.from_env("XRPUSDT")
            self.assertEqual(cfg.dn_tier0_usd, 999.0)
            # tier1 should still be default 1.6M
            self.assertEqual(cfg.dn_tier1_usd, 1_600_000.0)

if __name__ == "__main__":
    unittest.main()
