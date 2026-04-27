import pytest
from ml_analysis.tools.nightly_monitoring_smoke_tests_v1 import _split_csv, _mk_url, _smoke_contract_targets

def test_split_csv():
    assert _split_csv("/a,/b;/c") == ["/a", "/b", "/c"]
    assert _split_csv("  /a , /b; ") == ["/a", "/b"]
    assert _split_csv("") == []
    assert _split_csv(None) == []

def test_mk_url():
    assert _mk_url("http://base", "path") == "http://base/path"
    assert _mk_url("http://base/", "/path") == "http://base/path"
    assert _mk_url("http://base", "/path") == "http://base/path"
    assert _mk_url("http://base", "") == "http://base"
    assert _mk_url(None, "/path") == "/path"

def test_smoke_contract_targets_default(monkeypatch):
    monkeypatch.delenv("SMOKE_RUNBOOK_PATHS", raising=False)
    monkeypatch.delenv("SMOKE_DASHBOARD_PATHS", raising=False)
    
    runbook_urls, dash_urls = _smoke_contract_targets("http://public")
    
    assert "http://public/runbooks/web_uptime.md" in runbook_urls
    assert "http://public/grafana/d/monitoring_smoke/monitoring-smoke-nightly-contract?orgId=1" in dash_urls
    assert "http://public/grafana/d/edge_stack_overview/edge-stack-overview?orgId=1" in dash_urls
    assert "http://public/grafana/d/chatops_security/chatops-security?orgId=1" in dash_urls

def test_exporter_metrics_blackbox(monkeypatch):
    from ml_analysis.tools.monitoring_smoke_exporter_v1 import metrics
    from fastapi.responses import PlainTextResponse

    d = {
        "success": "1",
        "blackbox_exporter_ok": "1"
    }
    monkeypatch.setattr('ml_analysis.tools.monitoring_smoke_exporter_v1._read_redis', lambda: d)
    resp = metrics()
    assert isinstance(resp, PlainTextResponse)
    body = resp.body.decode("utf-8")
    assert "monitoring_smoke_blackbox_exporter_ok 1.0" in body

def test_exporter_metrics_blackbox_fail(monkeypatch):
    from ml_analysis.tools.monitoring_smoke_exporter_v1 import metrics
    from fastapi.responses import PlainTextResponse

    d = {
        "success": "0",
        "blackbox_exporter_ok": "0"
    }
    monkeypatch.setattr('ml_analysis.tools.monitoring_smoke_exporter_v1._read_redis', lambda: d)
    resp = metrics()
    body = resp.body.decode("utf-8")
    assert "monitoring_smoke_blackbox_exporter_ok 0.0" in body

def test_exporter_metrics_targets_stale(monkeypatch):
    from ml_analysis.tools.monitoring_smoke_exporter_v1 import metrics
    from fastapi.responses import PlainTextResponse

    d = {
        "success": "1",
        "targets_stale": "1",
        "targets_age_s": "42"
    }
    monkeypatch.setattr('ml_analysis.tools.monitoring_smoke_exporter_v1._read_redis', lambda: d)
    resp = metrics()
    body = resp.body.decode("utf-8")
    assert "monitoring_smoke_targets_stale 1.0" in body
    assert "monitoring_smoke_targets_age_seconds 42.0" in body
