import os

from services.auto_calibration_service import init_auto_calibration, get_auto_calibration_service


def test_init_auto_calibration_builds_singleton(monkeypatch):
    monkeypatch.setenv("AUTO_CALIB_ENABLED", "1")
    monkeypatch.setenv("TRADES_DB_DSN", "postgresql://user:pass@localhost:5432/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("AUTO_CALIB_OFFSETS", "0.3,0.4,0.5")
    monkeypatch.setenv("AUTO_CALIB_LIMIT_TRADES", "123")
    monkeypatch.setenv("AUTO_CALIB_MIN_NEW_TRADES", "7")

    init_auto_calibration(trades_threshold=55, enabled_symbols=["ethusdt", "BTCUSDT"], source="CryptoOrderFlow")
    svc = get_auto_calibration_service()
    assert svc is not None


def test_init_auto_calibration_disabled(monkeypatch):
    monkeypatch.setenv("AUTO_CALIB_ENABLED", "0")
    init_auto_calibration(trades_threshold=50, enabled_symbols=["ETHUSDT"], source="CryptoOrderFlow")
    # Should not raise; may leave singleton as-is
