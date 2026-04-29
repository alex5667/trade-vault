import os
import pytest
from unittest.mock import patch

def test_ml_gate_enforce_closed_policy(monkeypatch):
    monkeypatch.setenv("ML_CONFIRM_MODE", "ENFORCE")
    monkeypatch.setenv("ML_CONFIRM_FAIL_POLICY", "CLOSED")
    
    with patch("services.crypto_orderflow_service.RedisPoolSet"):
        original_import = __import__
        
        def mocked_import(name, *args, **kwargs):
            if name == "services.ml_confirm_gate":
                raise ImportError("mocked import error")
            return original_import(name, *args, **kwargs)
            
        with patch("builtins.__import__", side_effect=mocked_import):
            with pytest.raises(RuntimeError, match="ml_gate_required_but_missing"):
                from services.crypto_orderflow_service import CryptoOrderflowService
                CryptoOrderflowService(redis_dsn="redis://localhost")

def test_ml_gate_enforce_open_policy(monkeypatch):
    monkeypatch.setenv("ML_CONFIRM_MODE", "ENFORCE")
    monkeypatch.setenv("ML_CONFIRM_FAIL_POLICY", "OPEN")
    
    with patch("services.crypto_orderflow_service.RedisPoolSet"):
        original_import = __import__
        
        def mocked_import(name, *args, **kwargs):
            if name == "services.ml_confirm_gate":
                raise ImportError("mocked import error")
            return original_import(name, *args, **kwargs)
            
        with patch("builtins.__import__", side_effect=mocked_import):
            # Should not raise
            from services.crypto_orderflow_service import CryptoOrderflowService
            svc = CryptoOrderflowService(redis_dsn="redis://localhost")
            assert getattr(svc, "_ml_gate", None) is None
