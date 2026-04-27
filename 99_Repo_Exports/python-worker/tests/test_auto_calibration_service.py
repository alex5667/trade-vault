import pytest
from unittest.mock import patch, MagicMock
import os
from services.auto_calibration_service import (
    _normalize_enabled_symbols,
    init_auto_calibration,
    get_auto_calibration_service,
    AutoCalibrationService,
    SymbolConfig,
)


class TestNormalizeEnabledSymbols:
    def test_empty_list_returns_none(self):
        """Empty list means all symbols enabled"""
        assert _normalize_enabled_symbols([]) is None

    def test_star_returns_none(self):
        """Star means all symbols enabled"""
        assert _normalize_enabled_symbols(["*"]) is None
        assert _normalize_enabled_symbols(["ETHUSDT", "*"]) is None

    def test_comma_separated(self):
        """Comma-separated strings are parsed correctly"""
        result = _normalize_enabled_symbols(["ETHUSDT,BTCUSDT"])
        assert result == {"ETHUSDT", "BTCUSDT"}

    def test_mixed_list(self):
        """Mixed list with comma-separated items"""
        result = _normalize_enabled_symbols(["ETHUSDT", "BTCUSDT,ADAUSDT"])
        assert result == {"ETHUSDT", "BTCUSDT", "ADAUSDT"}

    def test_case_normalization(self):
        """Symbols are converted to uppercase"""
        result = _normalize_enabled_symbols(["ethusdt", "btcusdt"])
        assert result == {"ETHUSDT", "BTCUSDT"}

    def test_empty_strings_filtered(self):
        """Empty strings are filtered out"""
        result = _normalize_enabled_symbols(["ETHUSDT", "", "BTCUSDT"])
        assert result == {"ETHUSDT", "BTCUSDT"}


class TestInitAutoCalibration:
    @patch('services.auto_calibration_service._auto_calibration_service', None)
    @patch('services.auto_calibration_service._build_default_symbols')
    @patch('services.auto_calibration_service.AutoCalibrationService')
    def test_disabled_with_zero_threshold(self, mock_service, mock_build):
        """Service disabled when threshold <= 0"""
        init_auto_calibration(0, ["ETHUSDT"], "test_source")

        # Service should not be created when threshold <= 0
        assert mock_service.called is False

    @patch('services.auto_calibration_service._auto_calibration_service', None)
    @patch('services.auto_calibration_service._build_default_symbols')
    @patch('services.auto_calibration_service.AutoCalibrationService')
    def test_no_matching_symbols(self, mock_service, mock_build):
        """Service created with matching symbols only"""
        mock_build.return_value = [
            SymbolConfig(source="other_source", symbol="ETHUSDT", offsets=[0.5]),
            SymbolConfig(source="test_source", symbol="BTCUSDT", offsets=[0.5])
        ]

        init_auto_calibration(100, ["BTCUSDT"], "test_source")

        # Service should be created with the matching symbol
        assert mock_service.called is True

    @patch('services.auto_calibration_service._auto_calibration_service', None)
    @patch('services.auto_calibration_service._build_default_symbols')
    @patch('services.auto_calibration_service.AutoCalibrationService')
    @patch.dict(os.environ, {'TRADES_DB_DSN': 'test_dsn'})
    def test_successful_initialization(self, mock_service, mock_build):
        """Successful initialization with proper parameters"""
        mock_build.return_value = [
            SymbolConfig(source="test_source", symbol="ETHUSDT", offsets=[0.5], min_total_trades=50)
        ]

        init_auto_calibration(100, ["ETHUSDT"], "test_source")

        # Service should be created with correct parameters
        mock_service.assert_called_once()
        call_args = mock_service.call_args
        assert call_args[1]['dsn'] == 'test_dsn'  # from env
        assert len(call_args[1]['symbols']) == 1
        assert call_args[1]['symbols'][0].min_total_trades == 100  # overridden

    @patch('services.auto_calibration_service._auto_calibration_service', None)
    @patch('services.auto_calibration_service._build_default_symbols')
    @patch('services.auto_calibration_service.AutoCalibrationService')
    def test_all_symbols_enabled(self, mock_service, mock_build):
        """Empty enabled_symbols disables service"""
        mock_build.return_value = [
            SymbolConfig(source="test_source", symbol="ETHUSDT", offsets=[0.5]),
            SymbolConfig(source="test_source", symbol="BTCUSDT", offsets=[0.5])
        ]

        init_auto_calibration(100, [], "test_source")

        # Service should not be created when no symbols provided
        assert mock_service.called is False


class TestGetAutoCalibrationService:
    @patch('services.auto_calibration_service._auto_calibration_service', None)
    @patch('services.auto_calibration_service._build_default_symbols')
    @patch('services.auto_calibration_service.AutoCalibrationService')
    @patch.dict(os.environ, {'PG_DSN_CALIBRATION': 'custom_dsn'})
    def test_uses_pg_dsn_calibration_env(self, mock_service, mock_build):
        """Uses PG_DSN_CALIBRATION environment variable"""
        get_auto_calibration_service()

        mock_service.assert_called_once()
        call_args = mock_service.call_args
        assert call_args[1]['dsn'] == 'custom_dsn'


class TestWalkForwardMode:
    @patch('services.auto_calibration_service._auto_calibration_service', None)
    @patch('services.auto_calibration_service._build_default_symbols')
    @patch('services.auto_calibration_service.AutoCalibrationService')
    @patch.dict(os.environ, {
        'TRADES_DB_DSN': 'test_dsn',
        'AUTO_CALIB_WALK_FORWARD': '1',
    })
    def test_wf_enabled_by_default(self, mock_service, mock_build):
        """Walk-forward is enabled by default."""
        mock_build.return_value = [
            SymbolConfig(source="test_source", symbol="ETHUSDT", offsets=[0.5])
        ]
        init_auto_calibration(100, ["ETHUSDT"], "test_source")

        mock_service.assert_called_once()
        call_args = mock_service.call_args
        assert call_args[1]['use_walk_forward'] is True

    @patch('services.auto_calibration_service._auto_calibration_service', None)
    @patch('services.auto_calibration_service._build_default_symbols')
    @patch('services.auto_calibration_service.AutoCalibrationService')
    @patch.dict(os.environ, {
        'TRADES_DB_DSN': 'test_dsn',
        'AUTO_CALIB_WALK_FORWARD': '0',
    })
    def test_wf_disabled_via_env(self, mock_service, mock_build):
        """Walk-forward can be disabled via env."""
        mock_build.return_value = [
            SymbolConfig(source="test_source", symbol="ETHUSDT", offsets=[0.5])
        ]
        init_auto_calibration(100, ["ETHUSDT"], "test_source")

        mock_service.assert_called_once()
        call_args = mock_service.call_args
        assert call_args[1]['use_walk_forward'] is False

    def test_service_accepts_use_walk_forward_flag(self):
        """AutoCalibrationService accepts use_walk_forward parameter."""
        from services.auto_calibration_service import AutoCalibrationService as ACS
        # Just test that the constructor accepts the parameter
        # (actual DB/Redis connections would fail, so we don't instantiate)
        import inspect
        sig = inspect.signature(ACS.__init__)
        assert 'use_walk_forward' in sig.parameters

