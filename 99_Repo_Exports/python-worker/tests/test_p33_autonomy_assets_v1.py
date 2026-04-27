"""Tests for P3.3 autonomy: asset existence checks.

Validates that all files created by the P3.3-autonomy patch are present
and contain the expected key strings.  These are CI-friendly smoke checks
that run without external services.
"""
from pathlib import Path

_base = Path(__file__).resolve().parent.parent


def test_materialized_summary_sql_exists():
    """SQL migration for execution_replay_slo_summary_mv must exist."""
    sql = (_base / 'migrations' / '20260306_07_execution_replay_slo_summary.sql').read_text(encoding='utf-8')
    assert 'CREATE MATERIALIZED VIEW execution_replay_slo_summary_mv' in sql
    assert 'replay_latency_p95_ms' in sql


def test_slo_summary_indexes_sql_exists():
    """Index migration must define the unique index."""
    sql = (_base / 'migrations' / '20260306_08_execution_replay_slo_summary_indexes.sql').read_text(encoding='utf-8')
    assert 'execution_replay_slo_summary_mv_window_name_idx' in sql


def test_grafana_autonomy_dashboard_exists():
    """Grafana dashboard JSON must contain expected metric references."""
    # Dashboard lives at the monitoing level (outside python-worker)
    data = (Path(__file__).resolve().parent.parent.parent / 'monitoring' / 'grafana' / 'dashboards' / 'trade_execution_p33_autonomy.json').read_text(encoding='utf-8')
    assert 'trade_execution_rebuild_last_status_code' in data
    assert 'RETENTION_GUARD_QUARANTINED' in data
    assert 'trade_execution_autonomy_trigger_checkpoint_scrubber' in data


def test_prometheus_rules_exist():
    """Prometheus alerting rules file must define all three P3.3-autonomy alerts."""
    data = (Path(__file__).resolve().parent.parent.parent / 'monitoring' / 'prometheus_rules_execution_p33_autonomy.yml').read_text(encoding='utf-8')
    assert 'TradeExecutionAutonomyScrubberTriggered' in data
    assert 'TradeExecutionRebuildRetentionGuardHigh' in data
    assert 'TradeExecutionRetentionGuardQuarantineSpike' in data


def test_auto_trigger_script_exists():
    """auto_trigger_checkpoint_scrubber.py must exist."""
    assert (_base / 'scripts' / 'auto_trigger_checkpoint_scrubber.py').exists()


def test_refresh_slo_summary_script_exists():
    """refresh_execution_replay_slo_summary.py must exist."""
    assert (_base / 'scripts' / 'refresh_execution_replay_slo_summary.py').exists()


def test_runbook_exists():
    """P33_AUTONOMY_REPLAY.md runbook must exist."""
    assert (_base / 'runbooks' / 'P33_AUTONOMY_REPLAY.md').exists()


def test_systemd_units_exist():
    """All four autonomy systemd units must exist."""
    systemd = _base / 'systemd'
    assert (systemd / 'trade-execution-auto-scrubber.service').exists()
    assert (systemd / 'trade-execution-auto-scrubber.timer').exists()
    assert (systemd / 'trade-execution-replay-slo-refresh.service').exists()
    assert (systemd / 'trade-execution-replay-slo-refresh.timer').exists()
