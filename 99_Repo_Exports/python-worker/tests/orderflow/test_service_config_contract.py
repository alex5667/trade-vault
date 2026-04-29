import os
import pytest
from services.orderflow.service_config import ServiceConfig

def test_service_config_from_env_defaults():
    cfg = ServiceConfig()
    assert cfg.pel.cleanup_on_startup is True
    assert cfg.pel.cleanup_idle_threshold_ms == 60000
    assert cfg.lifecycle.drain_timeout_sec == 10.0
    assert cfg.tick.xack_retries == 3
    assert cfg.tick.xack_backoff_ms == 50.0

def test_service_config_from_env_overrides(monkeypatch):
    monkeypatch.setenv("PEL_CLEANUP_ON_STARTUP", "false")
    monkeypatch.setenv("CRYPTO_OF_DRAIN_TIMEOUT_SEC", "25.0")
    monkeypatch.setenv("CRYPTO_OF_XACK_RETRIES", "5")
    
    cfg = ServiceConfig.from_env()
    assert cfg.pel.cleanup_on_startup is False
    assert cfg.lifecycle.drain_timeout_sec == 25.0
    assert cfg.tick.xack_retries == 5
