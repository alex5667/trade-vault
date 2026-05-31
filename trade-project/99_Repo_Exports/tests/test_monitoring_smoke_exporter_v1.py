import pytest
from ml_analysis.tools.monitoring_smoke_exporter_v1 import metrics, healthz

def test_healthz():
    response = healthz()
    assert response == {"ok": "1"}

def test_metrics_empty(monkeypatch):
    monkeypatch.setattr("ml_analysis.tools.monitoring_smoke_exporter_v1._read_redis", lambda: {})
    response = metrics()
    assert response.status_code == 200
    text = response.body.decode("utf-8")
    assert "monitoring_smoke_last_success 0.0" in text
    assert "monitoring_smoke_alertmanager_api_ok 0.0" in text
    assert "monitoring_smoke_prometheus_api_ok 0.0" in text
    assert "monitoring_smoke_blackbox_exporter_ok 0.0" in text
    assert "monitoring_smoke_targets_stale 0.0" in text
    assert "monitoring_smoke_targets_age_seconds 0.0" in text

def test_metrics_populated(monkeypatch):
    data = {
        "success": "1",
        "updated_ts_ms": "1234567890000",
        "runbooks_ok": "1",
        "dashboards_ok": "1",
        "alertmanager_api_ok": "1",
        "prometheus_api_ok": "1",
        "blackbox_exporter_ok": "1",
        "failed_total": "0",
        "targets_stale": "1",
        "targets_age_s": "86400"
    }
    monkeypatch.setattr("ml_analysis.tools.monitoring_smoke_exporter_v1._read_redis", lambda: data)
    response = metrics()
    assert response.status_code == 200
    text = response.body.decode("utf-8")
    assert "monitoring_smoke_last_success 1.0" in text
    assert "monitoring_smoke_alertmanager_api_ok 1.0" in text
    assert "monitoring_smoke_prometheus_api_ok 1.0" in text
    assert "monitoring_smoke_blackbox_exporter_ok 1.0" in text
    assert "monitoring_smoke_targets_stale 1.0" in text
    assert "monitoring_smoke_targets_age_seconds 86400.0" in text
