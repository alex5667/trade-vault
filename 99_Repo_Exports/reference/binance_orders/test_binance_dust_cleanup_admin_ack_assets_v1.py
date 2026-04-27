"""Asset/integration tests for binance_dust_cleanup_admin_ack (P14).

Verify that:
  1. The Prometheus alerts YAML exists and references expected rule names.
  2. CLI exposes the expected ACK arguments.
  3. HTTP handler exposes the expected ACK route strings.
"""

from pathlib import Path


def test_ack_alerts_file_exists_and_mentions_expected_rules():
    """Verify the Prometheus alerts file exists and contains required alert names."""
    path = Path("services/prometheus_alerts_binance_dust_cleanup_admin_ack_v1.yml")
    assert path.exists(), f"Alerts file not found: {path}"
    text = path.read_text(encoding="utf-8")
    assert "BinanceDustAdminOldDenylistWithoutAck" in text
    assert "BinanceDustAdminCooldownLoopWithoutAck" in text
    assert "BinanceDustAdminAckRenewReminderEmitted" in text


def test_cli_exposes_ack_routes():
    """Verify CLI source exposes the P14 ACK command flags."""
    cli_text = Path("services/binance_dust_cleanup_admin_cli.py").read_text(encoding="utf-8")
    assert "--ack-reminder" in cli_text
    assert "--renew-ack" in cli_text
    assert "--revoke-ack" in cli_text
    assert "--show-ack-dashboard" in cli_text


def test_http_exposes_ack_routes():
    """Verify HTTP handler source exposes the P14 ACK route paths."""
    http_text = Path("services/binance_dust_cleanup_admin_http.py").read_text(encoding="utf-8")
    assert "/api/binance-dust/ack" in http_text
    assert "/api/binance-dust/ack/renew" in http_text
    assert "/api/binance-dust/ack/revoke" in http_text
    assert "/api/binance-dust/ack/dashboard" in http_text


def test_ack_module_imports_cleanly():
    """Verify binance_dust_cleanup_admin_ack can be imported without errors."""
    from services.binance_dust_cleanup_admin_ack import (  # noqa: F401
        ack_reminder,
        renew_reminder_ack,
        revoke_reminder_ack,
        should_suppress_reminder,
        ack_dashboard,
        dashboard_with_unacked,
    )


def test_execution_metrics_has_ack_metrics():
    """Verify P14 metrics are present in execution_metrics module."""
    from services.execution_metrics import (  # noqa: F401
        EXECUTION_DUST_ADMIN_ACK_ACTION_TOTAL,
        EXECUTION_DUST_ADMIN_ACK_STATE_TOTAL,
        EXECUTION_DUST_ADMIN_ACK_TTL_SEC,
        EXECUTION_DUST_ADMIN_UNACKED_ITEMS_TOTAL,
        EXECUTION_DUST_ADMIN_ACK_RENEW_REMINDER_TOTAL,
    )
