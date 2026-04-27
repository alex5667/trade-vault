
import os
import sys
import pytest
from unittest.mock import MagicMock, patch

# Ensure python-worker is in path
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from tools.nightly_meta_pipeline_v1 import main

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("META_PROMOTE_DIR_CHECK", "/tmp/mock_promote_check.prom")
    monkeypatch.setenv("META_PROMOTE_RETENTION_ENABLE", "1")
    monkeypatch.setenv("META_PROMOTE_RETENTION_KEEP_LAST", "50")
    monkeypatch.setenv("META_PROMOTE_RETENTION_KEEP_DAYS", "7")
    monkeypatch.setenv("META_PROMOTE_DIR", "/tmp/mock_promote_dir")

@patch("tools.nightly_meta_pipeline_v1.run_cmd") # Mock run_cmd to skip real execution
@patch("sys.argv", [
    "tools.nightly_meta_pipeline_v1",
    "--in-parquet", "/tmp/mock.parquet",
    "--out-model-json", "/tmp/mock_model.json",
    "--out-report-json", "/tmp/mock_report.json"
])
@patch("pathlib.Path.exists")
def test_nightly_pipeline_p24_integration(mock_path_exists, mock_run_cmd, mock_env):
    # Mock Path.exists to return True so checks pass
    mock_path_exists.return_value = True
    
    # Mock the tool modules
    with patch.dict(sys.modules, {
        "tools.meta_promote_dir_check_v1": MagicMock(),
        "tools.cleanup_promoted_models_v1": MagicMock()
    }):
        from tools import meta_promote_dir_check_v1
        from tools import cleanup_promoted_models_v1
        
        # Setup mocks
        mock_check = meta_promote_dir_check_v1.check_promote_dir
        mock_write = meta_promote_dir_check_v1.write_metrics
        mock_cleanup = cleanup_promoted_models_v1.cleanup_promoted_models
        
        mock_check.return_value = {"ok": 1}
        mock_write.return_value = True
        
        # Run main
        ret = main()
        
        assert ret == 0
        
        # Verify check called
        mock_check.assert_called_once_with("/tmp/mock_promote_dir")
        mock_write.assert_called_once()
        
        # Verify cleanup called
        mock_cleanup.assert_called_once_with(
            "/tmp/mock_promote_dir", 
            keep_last=50, 
            keep_days=7, 
            dry_run=False
        )

@patch("tools.nightly_meta_pipeline_v1.run_cmd")
@patch("sys.argv", [
    "tools.nightly_meta_pipeline_v1",
    "--in-parquet", "/tmp/mock.parquet",
    "--out-model-json", "/tmp/mock_model.json",
    "--out-report-json", "/tmp/mock_report.json"
])
@patch("pathlib.Path.exists")
def test_nightly_pipeline_p24_disabled(mock_path_exists, mock_run_cmd, monkeypatch):
    mock_path_exists.return_value = True
    
    # Disable env vars
    monkeypatch.delenv("META_PROMOTE_DIR_CHECK", raising=False)
    monkeypatch.setenv("META_PROMOTE_RETENTION_ENABLE", "0")
    
    with patch.dict(sys.modules, {
        "tools.meta_promote_dir_check_v1": MagicMock(),
        "tools.cleanup_promoted_models_v1": MagicMock()
    }):
        from tools import meta_promote_dir_check_v1
        from tools import cleanup_promoted_models_v1
        
        main()
        
        # Should NOT be called
        meta_promote_dir_check_v1.check_promote_dir.assert_not_called()
        cleanup_promoted_models_v1.cleanup_promoted_models.assert_not_called()
