
import unittest
import os
from unittest.mock import patch
from core.instrument_config import OrderFlowConfig, symbol_env_prefix, get_default_dist_bp_threshold, get_default_delta_tiers

class TestInstrumentConfigValidation(unittest.TestCase):

    def test_symbol_env_prefix_standard(self):
        self.assertEqual(symbol_env_prefix("BTCUSDT"), "BTC")
        self.assertEqual(symbol_env_prefix("ETHUSD"), "ETH")

    def test_symbol_env_prefix_1000_overrides(self):
        self.assertEqual(symbol_env_prefix("1000PEPEUSDT"), "PEPE")
        self.assertEqual(symbol_env_prefix("1000SHIBUSDT"), "SHIB")
        self.assertEqual(symbol_env_prefix("1000FLOKIUSDT"), "FLOKI")

    def test_symbol_env_prefix_fallback(self):
        # Unknown 1000* symbol: should strip digits
        self.assertEqual(symbol_env_prefix("1000UNKNOWNUSDT"), "UNKNOWN")
        # Just digits: edge case, returns original if empty after strip? 
        # based on regex repl logic, let's see implementation:
        # p2 = re.sub(r"^[0-9]+", "", p) -> if p="1000", p2="" -> returns p ("1000")
        self.assertEqual(symbol_env_prefix("1000XYZ"), "XYZ")

    def test_validate_valid_config(self):
        cfg = OrderFlowConfig(symbol="BTCUSDT")
        # Should not raise
        cfg.validate()

    def test_validate_invalid_dist_mode(self):
        with self.assertRaises(ValueError):
            OrderFlowConfig(symbol="BTCUSDT", dist_mode="invalid")

    def test_validate_invalid_stop_mode(self):
        with self.assertRaises(ValueError):
            OrderFlowConfig(symbol="BTCUSDT", stop_mode="MAGIC")

    def test_validate_thresholds(self):
        with self.assertRaises(ValueError):
            OrderFlowConfig(symbol="BTCUSDT", obi_threshold=1.5)
        
        with self.assertRaises(ValueError):
            OrderFlowConfig(symbol="BTCUSDT", weak_progress_atr=5.0)

    @patch.dict(os.environ, {"PEPE_DELTA_WINDOW": "999"})
    def test_from_env_override_1000(self):
        cfg = OrderFlowConfig.from_env("1000PEPEUSDT")
        self.assertEqual(cfg.delta_window_ticks, 999)

    @patch.dict(os.environ, {
        "BTC_DELTA_ABS_MIN_USD": "15000",
        "BTC_ICEBERG_REFRESH_MIN_NOTIONAL_USD": "50000"
    })
    def test_usd_thresholds_env(self):
        cfg = OrderFlowConfig.from_env("BTCUSDT")
        self.assertEqual(cfg.delta_abs_min_usd, 15000.0)
        self.assertEqual(cfg.iceberg_refresh_min_notional_usd, 50000.0)

    def test_usd_defaults_by_symbol(self):
        # BTC -> 15000
        cfg_btc = OrderFlowConfig.from_env("BTCUSDT")
        self.assertEqual(cfg_btc.delta_abs_min_usd, 15000.0)
        
        # ETH -> 5000
        cfg_eth = OrderFlowConfig.from_env("ETHUSDT")
        self.assertEqual(cfg_eth.delta_abs_min_usd, 5000.0)
        
        # PEPE (Meme/Default) -> 200
        cfg_pepe = OrderFlowConfig.from_env("1000PEPEUSDT")
        self.assertEqual(cfg_pepe.delta_abs_min_usd, 200.0)

    def test_obi_defaults_by_symbol(self):
        # BTC -> 0.25 threshold, 1.5s duration
        cfg_btc = OrderFlowConfig.from_env("BTCUSDT")
        self.assertEqual(cfg_btc.obi_threshold, 0.25)
        self.assertEqual(cfg_btc.obi_min_duration, 1.5)
        
        # ETH -> 0.28, 1.5
        cfg_eth = OrderFlowConfig.from_env("ETHUSDT")
        self.assertEqual(cfg_eth.obi_threshold, 0.28)
        
        # Meme (PEPE) -> 0.35, 2.5s duration (stricter)
        cfg_pepe = OrderFlowConfig.from_env("1000PEPEUSDT")
        self.assertEqual(cfg_pepe.obi_threshold, 0.35)
        self.assertEqual(cfg_pepe.obi_min_duration, 2.5)

    def test_dist_bp_defaults(self):
        # BTC/ETH -> 12.0
        cfg_btc = OrderFlowConfig.from_env("BTCUSDT")
        self.assertEqual(cfg_btc.dist_bp_threshold, 12.0)
        
        # SOL/BNB -> 15.0
        cfg_sol = OrderFlowConfig.from_env("SOLUSDT")
        self.assertEqual(cfg_sol.dist_bp_threshold, 15.0)
        sol = get_default_delta_tiers("SOLUSDT")
        self.assertEqual(sol["tier0"], 500000.0)
        
        # Others (Memes) -> 20.0
        cfg_pepe = OrderFlowConfig.from_env("1000PEPEUSDT")
        self.assertEqual(cfg_pepe.dist_bp_threshold, 20.0)

    def test_book_rate_defaults(self):
        from core.instrument_config import get_default_book_rate_settings
        
        # Majors
        btc = get_default_book_rate_settings("BTCUSDT")
        self.assertEqual(btc["book_rate_min_hz"], 50.0)
        
        eth = get_default_book_rate_settings("ETHUSDT")
        self.assertEqual(eth["book_rate_min_hz"], 20.0)
        self.assertEqual(eth["book_rate_warn_hz"], 10.0)
        
        # Large Cap
        bnb = get_default_book_rate_settings("BNBUSDT")
        self.assertEqual(bnb["book_rate_min_hz"], 25.0)
        self.assertEqual(bnb["book_rate_warn_hz"], 12.0)
        
        sol = get_default_book_rate_settings("SOLUSDT")
        self.assertEqual(sol["book_rate_min_hz"], 20.0)

        # Mid Cap
        sui = get_default_book_rate_settings("SUIUSDT")
        self.assertEqual(sui["book_rate_min_hz"], 10.0)
        self.assertEqual(sui["book_rate_warn_hz"], 5.0)
        
        # Memes
        pepe = get_default_book_rate_settings("1000PEPEUSDT")
        self.assertEqual(pepe["book_rate_min_hz"], 8.0)
        self.assertEqual(pepe["book_rate_warn_hz"], 4.0)

if __name__ == '__main__':
    unittest.main()
