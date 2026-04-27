import json
import os
from pathlib import Path
from unittest import mock
import pytest

from ml_analysis.tools import feature_denylist_proposal_exporter_v1


@pytest.fixture
def proposals_dir(tmp_path):
    d = tmp_path / "proposals"
    d.mkdir()
    
    # Create pending
    p1 = d / "denylist_proposal_1.manifest.json"
    p1.write_text(json.dumps({"status": "pending_ab"}), encoding="utf-8")
    
    # Create approved
    p2 = d / "denylist_proposal_2.manifest.json"
    p2.write_text(json.dumps({"status": "approved"}), encoding="utf-8")
    
    return d


def test_scrape_manifest_statuses(proposals_dir):
    counts = feature_denylist_proposal_exporter_v1._scrape_manifest_statuses(proposals_dir)
    assert counts["pending_ab"] == 1
    assert counts["approved"] == 1
    assert counts["ab_done"] == 0
    assert counts["ab_failed"] == 0


@mock.patch("ml_analysis.tools.feature_denylist_proposal_exporter_v1._get_redis")
def test_exporter_main(mock_get_redis, proposals_dir, tmp_path, monkeypatch):
    mock_redis = mock.Mock()
    mock_get_redis.return_value = mock_redis
    
    # Mock redis return for AB runner metrics
    mock_redis.hgetall.return_value = {
        "pending_n": "1",
        "processed_n": "1",
        "fail_n": "0",
        "oldest_pending_age_s": "3600",
            "ts_utc": feature_denylist_proposal_exporter_v1.datetime.now(tz=feature_denylist_proposal_exporter_v1.UTC).isoformat()
    }
    
    out_prom = tmp_path / "feature_denylist.prom"
    
    monkeypatch.setenv("FEATURE_DENYLIST_PROPOSALS_DIR", str(proposals_dir))
    monkeypatch.setenv("FEATURE_DENYLIST_EXPORT_PATH", str(out_prom))
    
    with mock.patch("sys.argv", ["exporter"]):
        rc = feature_denylist_proposal_exporter_v1.main()
        assert rc == 0
        
    assert out_prom.exists()
    content = out_prom.read_text(encoding="utf-8")
    
    # Check that it contains expected metrics
    assert 'feature_denylist_proposals_total{status="pending_ab"} 1.0\n' in content
    assert 'feature_denylist_proposals_total{status="approved"} 1.0\n' in content
    assert 'feature_denylist_ab_runner_processed 1.0\n' in content
    assert 'feature_denylist_ab_runner_fail 0.0\n' in content
