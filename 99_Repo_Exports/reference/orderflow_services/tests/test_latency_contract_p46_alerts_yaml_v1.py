from pathlib import Path


def test_p46_alerts_have_notifier_rules() -> None:
    txt = Path(__file__).resolve().parents[2].joinpath('orderflow_services/prometheus_alerts_latency_contract_p46_v1.yml').read_text(encoding='utf-8')
    assert 'OF_LatencyDeployLint_NotifierStale_Warn' in txt
    assert 'OF_LatencyDeployLint_PersistentDriftNotified_Crit' in txt
