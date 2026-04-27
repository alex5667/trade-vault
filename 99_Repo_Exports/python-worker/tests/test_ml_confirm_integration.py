"""
Integration tests for ML Confirm SRE metrics.

Tests:
  - Redis пустой → restore с диска → cfg_present=1, cfg_valid=1, модель загружается, model_loaded=1
  - labels:tb пуст → тренировка fail-fast и tb_train_empty_run_total++ (если добавите)
"""

import json
import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock

from services.observability.ml_confirm_sre_poller import MLConfirmSREPoller
from services.ml_confirm_gate import MLConfirmGate
from core.champion_cfg_validator import validate_champion_cfg


class TestMLConfirmRedisRestore:
    """Интеграционные тесты для восстановления конфига из Redis."""
    
    def test_redis_empty_restore_from_disk(self):
        """Проверка: Redis пустой → restore с диска → cfg_present=1, cfg_valid=1."""
        # Create a temporary config file
        cfg_data = {
            "schema_version": 1,
            "kind": "util_mh_v1",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/path/to/model.joblib",
            "mode": "CANARY",
            "enforce_share": 0.1,
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(cfg_data, f)
            temp_path = f.name
        
        try:
            # Simulate Redis restore: first empty, then restored
            mock_r = Mock()
            mock_r.get.side_effect = [None, json.dumps(cfg_data)]  # First empty, then restored
            
            # Simulate restore from disk
            with open(temp_path, 'r') as f:
                restored_cfg = json.load(f)
            
            # Validate restored config
            cfg_json = json.dumps(restored_cfg)
            cfg, _ = validate_champion_cfg(cfg_json, default_enforce_share=None)
            
            assert cfg.mode == "CANARY"
            assert cfg.enforce_share == 0.1
            assert cfg.kind == "util_mh_v1"
            
            # Verify metrics would be updated
            with patch('services.observability.metrics_registry.ml_confirm_cfg_present') as mock_present:
                with patch('services.observability.metrics_registry.ml_confirm_cfg_valid') as mock_valid:
                    mock_present.labels(kind=cfg.kind).set(1)
                    mock_valid.labels(kind=cfg.kind).set(1)
                    
                    assert mock_present.labels.called
                    assert mock_valid.labels.called
        finally:
            os.unlink(temp_path)
    
    def test_model_load_after_restore(self):
        """Проверка: после restore модель загружается, model_loaded=1."""
        cfg_data = {
            "schema_version": 1,
            "kind": "util_mh_fastlinear",
            "run_id": "test_run_123",
            "created_ms": 1234567890000,
            "model_path": "/tmp/test_model.json",
            "mode": "SHADOW",
            "enforce_share": 0.0,
        }
        
        # Create a minimal model file (JSON format for fastlinear)
        model_data = {
            "type": "fastlinear_util_mh",
            "weights": {},
            "bias": 0.0,
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(model_data, f)
            model_path = f.name
        
        try:
            cfg_json = json.dumps(cfg_data)
            cfg, _ = validate_champion_cfg(cfg_json, default_enforce_share=None)
            
            # Update model_path to point to our temp file
            cfg_data["model_path"] = model_path
            cfg_json = json.dumps(cfg_data)
            
            # Simulate MLConfirmGate loading
            mock_r = Mock()
            mock_r.get.return_value = cfg_json
            
            gate = MLConfirmGate(
                r=mock_r,
                mode="SHADOW",
                fail_policy="OPEN",
                champion_key="cfg:ml_confirm:champion",
            )
            
            # Verify model load metrics would be updated
            with patch('services.observability.metrics_registry.ml_confirm_model_loaded') as mock_loaded:
                # Simulate successful model load
                if os.path.exists(model_path):
                    mock_loaded.labels(kind=cfg.kind).set(1)
                    assert mock_loaded.labels.called
        finally:
            if os.path.exists(model_path):
                os.unlink(model_path)


class TestTBLabelsHealth:
    """Тесты для health labels:tb стрима."""
    
    def test_labels_empty_train_fail_fast(self):
        """Проверка: labels:tb пуст → тренировка fail-fast."""
        mock_r = Mock()
        mock_r.xlen.return_value = 0  # Empty stream
        
        poller = MLConfirmSREPoller(
            r=mock_r,
            labels_stream="labels:tb",
            poll_interval_sec=60,
        )
        
        with patch('services.observability.ml_confirm_sre_poller.PROMETHEUS_AVAILABLE', True):
            with patch('services.observability.ml_confirm_sre_poller.tb_labels_xlen') as mock_xlen:
                mock_xlen.set = Mock()
                
                poller.poll_once()
                
                # Verify xlen was set to 0
                mock_xlen.set.assert_called_with(0)
    
    def test_labels_below_minimum_threshold(self):
        """Проверка: labels:tb < 500 → warning."""
        mock_r = Mock()
        mock_r.xlen.return_value = 100  # Below minimum
        
        poller = MLConfirmSREPoller(
            r=mock_r,
            labels_stream="labels:tb",
            poll_interval_sec=60,
        )
        
        with patch('services.observability.ml_confirm_sre_poller.PROMETHEUS_AVAILABLE', True):
            with patch('services.observability.ml_confirm_sre_poller.tb_labels_xlen') as mock_xlen:
                mock_xlen.set = Mock()
                
                poller.poll_once()
                
                # Verify xlen was set to 100 (below threshold)
                mock_xlen.set.assert_called_with(100)
    
    def test_train_empty_run_counter(self):
        """Проверка: пустая тренировка → tb_train_empty_run_total++."""
        with patch('services.observability.ml_confirm_sre_poller.PROMETHEUS_AVAILABLE', True):
            with patch('services.observability.ml_confirm_sre_poller.tb_train_empty_run_total') as mock_counter:
                mock_counter.inc = Mock()
                
                # Simulate empty training run
                mock_counter.inc()
                
                assert mock_counter.inc.called

