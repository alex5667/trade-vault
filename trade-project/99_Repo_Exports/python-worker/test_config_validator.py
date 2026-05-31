"""
Unit tests for ConfigValidator.

Tests configuration and input validation logic.
"""

import unittest
import logging
from types import SimpleNamespace
from unittest.mock import Mock

from handlers.validation.config_validator import (
    ConfigValidator,
    InvalidSymbolError,
    MissingConfigError,
    DependencyError,
)


class TestConfigValidator(unittest.TestCase):
    """Test suite for ConfigValidator."""
    
    def setUp(self):
        """Create fresh validator for each test."""
        self.logger = Mock(spec=logging.Logger)
        self.validator = ConfigValidator(logger=self.logger)
    
    def test_validate_symbol_valid(self):
        """Test validating valid symbols."""
        self.validator.validate_symbol("BTCUSDT")
        self.validator.validate_symbol("ETH-USD")
        self.validator.validate_symbol("XAU/USD")
    
    def test_validate_symbol_invalid(self):
        """Test validating invalid symbols."""
        with self.assertRaises(InvalidSymbolError):
            self.validator.validate_symbol("")
        
        with self.assertRaises(InvalidSymbolError):
            self.validator.validate_symbol("   ")
        
        with self.assertRaises(InvalidSymbolError):
            self.validator.validate_symbol(None)
        
        with self.assertRaises(InvalidSymbolError):
            self.validator.validate_symbol(123)
    
    def test_validate_source_name_valid(self):
        """Test validating valid source names."""
        self.validator.validate_source_name("binance")
        self.validator.validate_source_name("mt5")
    
    def test_validate_source_name_invalid(self):
        """Test validating invalid source names."""
        with self.assertRaises(ValueError):
            self.validator.validate_source_name("")
        
        with self.assertRaises(ValueError):
            self.validator.validate_source_name(None)
    
    def test_validate_signal_stream_prefix_valid(self):
        """Test validating valid stream prefixes."""
        self.validator.validate_signal_stream_prefix("signals")
        self.validator.validate_signal_stream_prefix("orderflow")
    
    def test_validate_signal_stream_prefix_invalid(self):
        """Test validating invalid stream prefixes."""
        with self.assertRaises(ValueError):
            self.validator.validate_signal_stream_prefix("")
        
        with self.assertRaises(ValueError):
            self.validator.validate_signal_stream_prefix(None)
    
    def test_validate_config_valid(self):
        """Test validating valid configuration."""
        config = SimpleNamespace(
            symbol="BTCUSDT",
            main_z_threshold=2.5,
            breakout_z_threshold=3.0,
            obi_threshold=0.6,
            delta_bucket_ms=1000
        )
        
        # Should not raise
        self.validator.validate_config(config)
    
    def test_validate_config_missing_symbol(self):
        """Test config without symbol."""
        config = SimpleNamespace()
        
        with self.assertRaises(MissingConfigError):
            self.validator.validate_config(config)
    
    def test_validate_config_invalid_threshold(self):
        """Test config with invalid threshold."""
        config = SimpleNamespace(
            symbol="BTCUSDT",
            main_z_threshold=-1.0  # Negative threshold
        )
        
        with self.assertRaises(ValueError):
            self.validator.validate_config(config)
    
    def test_validate_config_invalid_delta_bucket(self):
        """Test config with invalid delta_bucket_ms."""
        config = SimpleNamespace(
            symbol="BTCUSDT",
            delta_bucket_ms=0  # Must be positive
        )
        
        with self.assertRaises(ValueError):
            self.validator.validate_config(config)
    
    def test_validate_config_threshold_consistency_warning(self):
        """Test config with inconsistent thresholds logs warning."""
        config = SimpleNamespace(
            symbol="BTCUSDT",
            main_z_threshold=3.0,
            breakout_z_threshold=2.5  # Lower than main
        )
        
        # Should log warning but not raise
        self.validator.validate_config(config)
        self.logger.warning.assert_called_once()
    
    def test_validate_inputs_all_valid(self):
        """Test validating all inputs together."""
        config = SimpleNamespace(
            symbol="BTCUSDT",
            main_z_threshold=2.5
        )
        
        # Should not raise
        self.validator.validate_inputs(
            symbol="BTCUSDT",
            config=config,
            source_name="binance",
            signal_stream_prefix="signals"
        )
    
    def test_validate_inputs_invalid_symbol(self):
        """Test validate_inputs with invalid symbol."""
        with self.assertRaises(InvalidSymbolError):
            self.validator.validate_inputs(
                symbol="",
                config=None,
                source_name="binance",
                signal_stream_prefix="signals"
            )
    
    def test_validate_inputs_no_config(self):
        """Test validate_inputs without config."""
        # Should not raise
        self.validator.validate_inputs(
            symbol="BTCUSDT",
            config=None,
            source_name="binance",
            signal_stream_prefix="signals"
        )
    
    def test_validate_redis_valid(self):
        """Test validating valid Redis connection."""
        redis = Mock()
        # Should not raise
        self.validator.validate_redis(redis)
    
    def test_validate_redis_none(self):
        """Test validating None Redis connection."""
        with self.assertRaises(DependencyError):
            self.validator.validate_redis(None)
    
    def test_validate_stream_valid(self):
        """Test validating valid stream."""
        # Should not raise
        self.validator.validate_stream("tick_stream", "ticks:BTCUSDT")
    
    def test_validate_stream_empty(self):
        """Test validating empty stream."""
        with self.assertRaises(DependencyError):
            self.validator.validate_stream("tick_stream", "")
        
        with self.assertRaises(DependencyError):
            self.validator.validate_stream("tick_stream", None)
    
    def test_validate_infrastructure_all_valid(self):
        """Test validating all infrastructure components."""
        redis = Mock()
        cache_service = Mock()
        
        # Should not raise
        self.validator.validate_infrastructure(
            redis=redis,
            tick_stream="ticks:BTCUSDT",
            book_stream="book:BTCUSDT",
            cache_service=cache_service
        )
    
    def test_validate_infrastructure_missing_redis(self):
        """Test infrastructure validation with missing Redis."""
        with self.assertRaises(DependencyError):
            self.validator.validate_infrastructure(
                redis=None,
                tick_stream="ticks:BTCUSDT",
                book_stream="book:BTCUSDT",
                cache_service=Mock()
            )
    
    def test_validate_infrastructure_missing_cache(self):
        """Test infrastructure validation with missing cache."""
        with self.assertRaises(DependencyError):
            self.validator.validate_infrastructure(
                redis=Mock(),
                tick_stream="ticks:BTCUSDT",
                book_stream="book:BTCUSDT",
                cache_service=None
            )


if __name__ == "__main__":
    unittest.main()
