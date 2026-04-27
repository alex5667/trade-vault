from pathlib import Path


def test_p47_alerts_have_silence_rules() -> None:
    txt = Path(__file__).resolve().parents[2].joinpath('orderflow_services/prometheus_alerts_latency_contract_p47_v1.yml').read_text(encoding='utf-8')
    assert 'OF_LatencyDeployLint_PersistentDriftUnsilenced_Crit' in txt
    assert 'OF_LatencyDeployLint_PersistentDriftSilenced_Warn' in txt
