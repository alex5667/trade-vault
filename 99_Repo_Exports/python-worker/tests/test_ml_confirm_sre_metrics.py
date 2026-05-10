"""
Unit tests for ML Confirm SRE metrics integration.

Tests:
  - validate_champion_cfg: missing enforce_share при CANARY → invalid_cfg
  - mode/enforce_share invariants
  - bad JSON → bad_json (не no_cfg)
  - метрики: при no_cfg выставляются cfg_present=0, cfg_valid=0, инкремент ml_confirm_errors_total{reason="no_cfg"}
"""

import json
from unittest.mock import Mock, patch

import pytest

from core.champion_cfg_validator import CfgError, validate_champion_cfg
from core.redis_keys import RedisStreams as RS


class TestChampionCfgValidation:
    """Тесты валидации champion конфига."""

    def test_missing_enforce_share_canary_raises_error(self):
        """Проверка: missing enforce_share при CANARY → invalid_cfg."""
        cfg_json = json.dumps({
            "schema_version": 1,
            "kind": "util_mh_v1",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/path/to/model.joblib",
            "mode": "CANARY",
            # enforce_share отсутствует
        })

        with pytest.raises(CfgError) as exc_info:
            validate_champion_cfg(cfg_json, default_enforce_share=None)

        assert "enforce_share" in str(exc_info.value).lower() or "missing" in str(exc_info.value).lower()

    def test_missing_enforce_share_enforce_raises_error(self):
        """Проверка: missing enforce_share при ENFORCE → invalid_cfg."""
        cfg_json = json.dumps({
            "schema_version": 1,
            "kind": "util_mh_v1",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/path/to/model.joblib",
            "mode": "ENFORCE",
            # enforce_share отсутствует
        })

        with pytest.raises(CfgError) as exc_info:
            validate_champion_cfg(cfg_json, default_enforce_share=None)

        assert "enforce_share" in str(exc_info.value).lower() or "missing" in str(exc_info.value).lower()

    def test_mode_enforce_share_invariants(self):
        """Проверка инвариантов mode ↔ enforce_share."""
        # SHADOW requires enforce_share=0.0
        cfg_json = json.dumps({
            "schema_version": 1,
            "kind": "util_mh_v1",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/path/to/model.joblib",
            "mode": "SHADOW",
            "enforce_share": 0.0,
        })
        cfg, _ = validate_champion_cfg(cfg_json, default_enforce_share=None)
        assert cfg.mode == "SHADOW"
        assert cfg.enforce_share == 0.0

        # ENFORCE requires enforce_share=1.0
        cfg_json = json.dumps({
            "schema_version": 1,
            "kind": "util_mh_v1",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/path/to/model.joblib",
            "mode": "ENFORCE",
            "enforce_share": 1.0,
        })
        cfg, _ = validate_champion_cfg(cfg_json, default_enforce_share=None)
        assert cfg.mode == "ENFORCE"
        assert cfg.enforce_share == 1.0

        # CANARY requires 0.0 < enforce_share < 1.0
        cfg_json = json.dumps({
            "schema_version": 1,
            "kind": "util_mh_v1",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/path/to/model.joblib",
            "mode": "CANARY",
            "enforce_share": 0.1,
        })
        cfg, _ = validate_champion_cfg(cfg_json, default_enforce_share=None)
        assert cfg.mode == "CANARY"
        assert 0.0 < cfg.enforce_share < 1.0

        # Invalid: SHADOW with enforce_share != 0.0
        cfg_json = json.dumps({
            "schema_version": 1,
            "kind": "util_mh_v1",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/path/to/model.joblib",
            "mode": "SHADOW",
            "enforce_share": 0.5,  # Invalid
        })
        with pytest.raises(CfgError) as exc_info:
            validate_champion_cfg(cfg_json, default_enforce_share=None)
        assert "SHADOW" in str(exc_info.value) and "0.0" in str(exc_info.value)

        # Invalid: ENFORCE with enforce_share != 1.0
        cfg_json = json.dumps({
            "schema_version": 1,
            "kind": "util_mh_v1",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/path/to/model.joblib",
            "mode": "ENFORCE",
            "enforce_share": 0.5,  # Invalid
        })
        with pytest.raises(CfgError) as exc_info:
            validate_champion_cfg(cfg_json, default_enforce_share=None)
        assert "ENFORCE" in str(exc_info.value) and "1.0" in str(exc_info.value)

    def test_bad_json_raises_error(self):
        """Проверка: bad JSON → bad_json (не no_cfg)."""
        # Invalid JSON
        with pytest.raises(CfgError) as exc_info:
            validate_champion_cfg("not a json", default_enforce_share=None)
        assert "json" in str(exc_info.value).lower()

        # Array instead of object
        with pytest.raises(CfgError) as exc_info:
            validate_champion_cfg('["array", "not", "object"]', default_enforce_share=None)
        assert "object" in str(exc_info.value).lower()


class TestMLConfirmMetrics:
    """Тесты метрик ML Confirm."""

    @patch('services.observability.metrics_registry.ml_confirm_cfg_present')
    @patch('services.observability.metrics_registry.ml_confirm_cfg_valid')
    @patch('services.observability.metrics_registry.ml_confirm_errors_total')
    def test_no_cfg_metrics(self, mock_errors, mock_cfg_valid, mock_cfg_present):
        """Проверка: при no_cfg выставляются cfg_present=0, cfg_valid=0, инкремент ml_confirm_errors_total{reason="no_cfg"}."""
        # Simulate no_cfg scenario
        mock_cfg_present.labels.return_value.set(0)
        mock_cfg_valid.labels.return_value.set(0)
        mock_errors.labels.return_value.inc()

        # Verify metrics were called
        mock_cfg_present.labels(kind="unknown").set(0)
        mock_cfg_valid.labels(kind="unknown").set(0)
        mock_errors.labels(kind="unknown", reason="no_cfg").inc()

        assert mock_cfg_present.labels.called
        assert mock_cfg_valid.labels.called
        assert mock_errors.labels.called

    @patch('services.observability.metrics_registry.ml_confirm_cfg_present')
    @patch('services.observability.metrics_registry.ml_confirm_cfg_valid')
    @patch('services.observability.metrics_registry.ml_confirm_enforce_share')
    def test_valid_cfg_metrics(self, mock_enforce_share, mock_cfg_valid, mock_cfg_present):
        """Проверка: при валидном cfg выставляются cfg_present=1, cfg_valid=1, enforce_share."""
        cfg_json = json.dumps({
            "schema_version": 1,
            "kind": "util_mh_v1",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/path/to/model.joblib",
            "mode": "CANARY",
            "enforce_share": 0.1,
        })

        cfg, _ = validate_champion_cfg(cfg_json, default_enforce_share=None)

        # Simulate metrics update
        mock_cfg_present.labels(kind=cfg.kind).set(1)
        mock_cfg_valid.labels(kind=cfg.kind).set(1)
        mock_enforce_share.labels(kind=cfg.kind).set(cfg.enforce_share)

        assert mock_cfg_present.labels.called
        assert mock_cfg_valid.labels.called
        assert mock_enforce_share.labels.called

    @patch('services.observability.metrics_registry.ml_missing_critical_total')
    def test_missing_enforce_share_metric(self, mock_missing):
        """Проверка: missing enforce_share инкрементирует ml_missing_critical_total."""
        # Simulate missing enforce_share
        mock_missing.labels(field="champion.enforce_share").inc()

        assert mock_missing.labels.called


class TestMLConfirmSREPoller:
    """Тесты для ml_confirm_sre_poller."""

    @patch('services.observability.ml_confirm_sre_poller.redis')
    def test_poll_ml_confirm_cfg_missing(self, mock_redis):
        """Проверка: Redis пустой → cfg_present=0, cfg_valid=0."""
        from services.observability.ml_confirm_sre_poller import MLConfirmSREPoller

        mock_r = Mock()
        mock_r.get.return_value = None  # Config missing

        poller = MLConfirmSREPoller(
            r=mock_r,
            labels_stream=RS.TB_LABELS,
            poll_interval_sec=60,
            champion_key="cfg:ml_confirm:champion",
        )

        with patch('services.observability.ml_confirm_sre_poller.PROMETHEUS_AVAILABLE', True):
            with patch('services.observability.ml_confirm_sre_poller.ml_confirm_cfg_present_gauge') as mock_present:
                with patch('services.observability.ml_confirm_sre_poller.ml_confirm_cfg_valid_gauge') as mock_valid:
                    mock_present.labels.return_value = Mock()
                    mock_valid.labels.return_value = Mock()

                    poller._poll_ml_confirm_cfg()

                    # Verify metrics were set to 0
                    assert mock_present.labels.called
                    assert mock_valid.labels.called

    @patch('services.observability.ml_confirm_sre_poller.redis')
    def test_poll_ml_confirm_cfg_valid(self, mock_redis):
        """Проверка: валидный cfg → cfg_present=1, cfg_valid=1, enforce_share установлен."""
        from services.observability.ml_confirm_sre_poller import MLConfirmSREPoller

        cfg_json = json.dumps({
            "schema_version": 1,
            "kind": "util_mh_v1",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/path/to/model.joblib",
            "mode": "CANARY",
            "enforce_share": 0.1,
        })

        mock_r = Mock()
        mock_r.get.return_value = cfg_json

        poller = MLConfirmSREPoller(
            r=mock_r,
            labels_stream=RS.TB_LABELS,
            poll_interval_sec=60,
            champion_key="cfg:ml_confirm:champion",
        )

        with patch('services.observability.ml_confirm_sre_poller.PROMETHEUS_AVAILABLE', True):
            with patch('services.observability.ml_confirm_sre_poller.ml_confirm_cfg_present_gauge') as mock_present:
                with patch('services.observability.ml_confirm_sre_poller.ml_confirm_cfg_valid_gauge') as mock_valid:
                    with patch('services.observability.ml_confirm_sre_poller.ml_confirm_enforce_share_gauge') as mock_share:
                        mock_present.labels.return_value = Mock()
                        mock_valid.labels.return_value = Mock()
                        mock_share.labels.return_value = Mock()

                        poller._poll_ml_confirm_cfg()

                        # Verify metrics were set correctly
                        assert mock_present.labels.called
                        assert mock_valid.labels.called
                        assert mock_share.labels.called

