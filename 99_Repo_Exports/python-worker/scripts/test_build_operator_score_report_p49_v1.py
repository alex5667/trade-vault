import importlib.util
import json
import sys
from pathlib import Path

# Load build_operator_score_report module dynamically to avoid circular imports
path = Path(__file__).resolve().parents[0] / 'build_operator_score_report.py'
spec = importlib.util.spec_from_file_location('build_operator_score_report_p49', path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_operator_score_penalized_by_mismatch_summary(tmp_path: Path):
    """P4.9: operator score should decrease when mismatch summary shows high quarantine/rate."""
    (tmp_path / 'latest_canary_scoring.json').write_text(json.dumps({'score': 95}), encoding='utf-8')
    (tmp_path / 'latest_replay_slo_summary.json').write_text(
        json.dumps({'items': [{'window_name': '24h', 'rehydrate_total': 10, 'replay_truncated_total': 0, 'retention_guard_total': 0, 'replay_latency_p95_ms': 10}]}),
        encoding='utf-8',
    )
    (tmp_path / 'latest_risk_engine_canary.json').write_text(json.dumps({'score': 95}), encoding='utf-8')
    (tmp_path / 'latest_execution_health.json').write_text(json.dumps({'overall_status': 'ok'}), encoding='utf-8')
    (tmp_path / 'latest_risk_signal_consistency.json').write_text(json.dumps({'mismatch_rate': 0.0}), encoding='utf-8')
    (tmp_path / 'latest_risk_mismatch_summary.json').write_text(
        json.dumps({'rows': [{'window_name': '24h', 'tier': 'ALL', 'quarantine_count': 3, 'avg_mismatch_rate': 0.08}]}),
        encoding='utf-8',
    )
    rep = mod.build_report(tmp_path)
    assert rep['mismatch_penalty'] > 0
    assert rep['score'] < 95


def test_operator_score_no_penalty_when_summary_empty(tmp_path: Path):
    """P4.9: penalty should be 0 when mismatch summary is missing or empty."""
    (tmp_path / 'latest_canary_scoring.json').write_text(json.dumps({'score': 95}), encoding='utf-8')
    (tmp_path / 'latest_replay_slo_summary.json').write_text(json.dumps({'items': []}), encoding='utf-8')
    (tmp_path / 'latest_risk_engine_canary.json').write_text(json.dumps({'score': 95}), encoding='utf-8')
    (tmp_path / 'latest_execution_health.json').write_text(json.dumps({'overall_status': 'ok'}), encoding='utf-8')
    (tmp_path / 'latest_risk_signal_consistency.json').write_text(json.dumps({'mismatch_rate': 0.0}), encoding='utf-8')
    # No latest_risk_mismatch_summary.json written — should gracefully return zero penalty
    rep = mod.build_report(tmp_path)
    assert rep['mismatch_penalty'] == 0.0
    assert 'mismatch_quarantine_count_24h' in rep
