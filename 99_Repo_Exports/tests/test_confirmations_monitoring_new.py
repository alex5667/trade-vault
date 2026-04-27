import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add the python-worker directory to the path so we can import the modules
sys.path.append('/home/alex/front/trade/scanner_infra/python-worker')

from services.orderflow.metrics import (
    track_confirmations,
    confirmation_seen_total,
    confirmation_unknown_total,
    confirmation_alias_used_total,
    confirmation_incomplete_total,
    confirmations_per_signal_hist,
    _DEFAULT_COVERAGE_KEYS
)

class TestConfirmationMonitoring(unittest.TestCase):
    def setUp(self):
        # Reset counters for testing
        # Note: prometheus_client metrics are global, so we can't easily reset them fully,
        # but we can check increments or mock the counters.
        # For this test, we'll mock the metrics objects themselves at the module level
        # or just inspect their values.
        pass

    def test_track_confirmations_basic(self):
        """Test basic tracking of known keys."""
        symbol = "BTCUSDT"
        confirmations = ["sweep_eqh=1", "reclaim=1"]
        
        # Determine initial values
        initial_sweep = confirmation_seen_total.labels(key="sweep_eqh", symbol=symbol)._value.get()
        initial_reclaim = confirmation_seen_total.labels(key="reclaim", symbol=symbol)._value.get()
        
        track_confirmations(symbol, confirmations)
        
        # Check increments
        self.assertEqual(confirmation_seen_total.labels(key="sweep_eqh", symbol=symbol)._value.get(), initial_sweep + 1)
        self.assertEqual(confirmation_seen_total.labels(key="reclaim", symbol=symbol)._value.get(), initial_reclaim + 1)

    def test_track_confirmations_drift(self):
        """Test detection of unknown keys (drift)."""
        symbol = "ETHUSDT"
        confirmations = ["unknown_key=1"]
        
        initial_unknown = confirmation_unknown_total.labels(key="unknown_key", symbol=symbol)._value.get()
        
        track_confirmations(symbol, confirmations)
        
        self.assertEqual(confirmation_unknown_total.labels(key="unknown_key", symbol=symbol)._value.get(), initial_unknown + 1)

    def test_track_confirmations_alias(self):
        """Test alias usage."""
        symbol = "SOLUSDT"
        confirmations = ["ice_strict=1"]
        
        initial_alias = confirmation_alias_used_total.labels(from_key="ice_strict", to_key="iceberg_strict", symbol=symbol)._value.get()
        
        track_confirmations(symbol, confirmations)
        
        self.assertEqual(confirmation_alias_used_total.labels(from_key="ice_strict", to_key="iceberg_strict", symbol=symbol)._value.get(), initial_alias + 1)

    def test_track_confirmations_incomplete_sweep(self):
        """Test sweep side missing."""
        symbol = "BTCUSDT"
        confirmations = ["sweep=1"] # Missing sweep_eqh or sweep_eql
        
        initial_incomplete = confirmation_incomplete_total.labels(kind="sweep_side_missing", symbol=symbol)._value.get()
        
        track_confirmations(symbol, confirmations)
        
        self.assertEqual(confirmation_incomplete_total.labels(kind="sweep_side_missing", symbol=symbol)._value.get(), initial_incomplete + 1)

    def test_track_confirmations_mismatch_sweep(self):
        """Test sweep side mismatch."""
        symbol = "BTCUSDT"
        # LONG signal but sweep_eqh (bearish) present
        confirmations = ["sweep_eqh=1"]
        side = "LONG"
        
        initial_mismatch = confirmation_incomplete_total.labels(kind="sweep_side_mismatch", symbol=symbol)._value.get()
        
        track_confirmations(symbol, confirmations, side=side)
        
        self.assertEqual(confirmation_incomplete_total.labels(kind="sweep_side_mismatch", symbol=symbol)._value.get(), initial_mismatch + 1)

if __name__ == '__main__':
    unittest.main()
