from __future__ import annotations
"""P3.3-ops-complete test: asset files existence verification.

Tests that all new file system assets added by the ops-complete patch exist
in the expected locations.
"""

from pathlib import Path


def test_scrubber_script_exists():
    """scrub_replay_checkpoints.py script must exist."""
    root = Path(__file__).resolve().parents[1]
    assert (root / 'scripts' / 'scrub_replay_checkpoints.py').exists()


def test_systemd_service_exists():
    """Systemd service file for checkpoint scrubber must exist."""
    root = Path(__file__).resolve().parents[2]
    assert (root / 'deploy' / 'systemd' / 'trade-execution-checkpoint-scrubber.service').exists()


def test_systemd_timer_exists():
    """Systemd timer file for checkpoint scrubber must exist."""
    root = Path(__file__).resolve().parents[2]
    assert (root / 'deploy' / 'systemd' / 'trade-execution-checkpoint-scrubber.timer').exists()


def test_prometheus_rules_include_replay_p95():
    """Prometheus rules file must include histogram_quantile p95 and retention guard alert."""
    src = (
        Path(__file__).resolve().parents[2] / 'monitoring' / 'prometheus_rules_execution_p33_ops_complete.yml'
    ).read_text(encoding='utf-8')
    assert 'histogram_quantile(0.95' in src, "Must include p95 histogram_quantile expression"
    assert 'TradeExecutionReplayRetentionGuard' in src, "Must include retention guard alert"


def test_rebuild_script_writes_report():
    """rebuild_orders_state_from_exec.py must write the rebuild report."""
    src = (Path(__file__).resolve().parents[1] / 'scripts' / 'rebuild_orders_state_from_exec.py').read_text(encoding='utf-8')
    assert 'latest_rebuild_state.json' in src, "rebuild script must write latest_rebuild_state.json"
    assert 'replay_latency_p95_ms' in src, "rebuild script must compute p95 latency"
