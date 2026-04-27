"""
test_exec_health_slo_autoguard_exporter_v1.py
Unit tests for the P5 AutoGuard Prometheus exporter.
Uses mock Redis client — no real Redis connection required.
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

from orderflow_services import exec_health_slo_autoguard_exporter_v1 as exp


def test_exec_health_slo_autoguard_exporter_sets_gauges() -> None:
    """Verify all Prometheus gauges are populated from Redis hash data."""
    mock_redis_mod = MagicMock()
    mock_client = MagicMock()
    mock_redis_mod.Redis.from_url.return_value = mock_client
    mock_client.hgetall.return_value = {
        "updated_ts_ms": "1000",
        "freeze_active": "1",
        "freeze_until_ts_ms": "9999999999999",
        "mode_mismatch_active": "1",
        "rollout_drift_active": "0",
        "last_trigger_ts_ms": "900",
        "last_rollback_ts_ms": "800",
        "rollback_total": "2",
    }
    with patch("orderflow_services.exec_health_slo_autoguard_exporter_v1.redis", mock_redis_mod), \
         patch("orderflow_services.exec_health_slo_autoguard_exporter_v1.start_http_server"), \
         patch("orderflow_services.exec_health_slo_autoguard_exporter_v1.time.sleep", side_effect=KeyboardInterrupt):
        try:
            exp.main()
        except KeyboardInterrupt:
            pass
    assert exp.FREEZE_ACTIVE._value.get() == 1.0
    assert exp.MODE_MISMATCH_ACTIVE._value.get() == 1.0
    assert exp.ROLLBACK_TOTAL._value.get() == 2.0


def test_exec_health_slo_autoguard_exporter_sets_up_zero_on_error() -> None:
    """When Redis raises, UP gauge must be set to 0."""
    mock_redis_mod = MagicMock()
    mock_client = MagicMock()
    mock_redis_mod.Redis.from_url.return_value = mock_client
    mock_client.hgetall.side_effect = RuntimeError("redis down")
    with patch("orderflow_services.exec_health_slo_autoguard_exporter_v1.redis", mock_redis_mod), \
         patch("orderflow_services.exec_health_slo_autoguard_exporter_v1.start_http_server"), \
         patch("orderflow_services.exec_health_slo_autoguard_exporter_v1.time.sleep", side_effect=KeyboardInterrupt):
        try:
            exp.main()
        except KeyboardInterrupt:
            pass
    assert exp.UP._value.get() == 0.0
