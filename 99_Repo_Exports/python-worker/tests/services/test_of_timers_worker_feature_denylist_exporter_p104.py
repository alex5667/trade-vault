from __future__ import annotations

"""Tests for run_feature_denylist_proposal_exporter (P104).

Covers:
  - Disabled by default (ENABLE_FEATURE_DENYLIST_EXPORTER not set / 0)
  - Subprocess success (rc=0) → True
  - Subprocess failure (rc!=0, no module-not-found) → False
  - Module-not-found in stderr → no-op True (phased rollout guard)
  - Timeout → False, warning logged
  - Exception in subprocess.run → False
  - Default env vars (FEATURE_DENYLIST_EXPORT_PATH, FEATURE_DENYLIST_PROPOSALS_DIR) are set
"""


import subprocess
from unittest.mock import MagicMock, patch

from services.of_timers_worker import run_feature_denylist_proposal_exporter

# ---------------------------------------------------------------------------
# Guard: disabled by default
# ---------------------------------------------------------------------------

def test_disabled_by_default(monkeypatch):
    """When ENABLE_FEATURE_DENYLIST_EXPORTER is not set (default 0), no-op → True."""
    monkeypatch.delenv("ENABLE_FEATURE_DENYLIST_EXPORTER", raising=False)
    with patch("subprocess.run") as mock_run:
        result = run_feature_denylist_proposal_exporter()
    assert result is True
    mock_run.assert_not_called()


def test_disabled_explicit_zero(monkeypatch):
    """ENABLE_FEATURE_DENYLIST_EXPORTER=0 → no-op."""
    monkeypatch.setenv("ENABLE_FEATURE_DENYLIST_EXPORTER", "0")
    with patch("subprocess.run") as mock_run:
        result = run_feature_denylist_proposal_exporter()
    assert result is True
    mock_run.assert_not_called()


# ---------------------------------------------------------------------------
# Normal success path
# ---------------------------------------------------------------------------

def test_enabled_success(monkeypatch):
    """ENABLE_FEATURE_DENYLIST_EXPORTER=1 + subprocess rc=0 → True."""
    monkeypatch.setenv("ENABLE_FEATURE_DENYLIST_EXPORTER", "1")
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = ""
    mock_result.stderr = ""
    with patch("subprocess.run", return_value=mock_result) as mock_run:
        result = run_feature_denylist_proposal_exporter()
    assert result is True
    mock_run.assert_called_once()
    # Verify the default module is used
    args = mock_run.call_args[0][0]
    assert "ml_analysis.tools.feature_denylist_proposal_exporter_v1" in args


# ---------------------------------------------------------------------------
# Failure path
# ---------------------------------------------------------------------------

def test_subprocess_failure_rc1(monkeypatch):
    """Non-zero exit (not ModuleNotFoundError) → False."""
    monkeypatch.setenv("ENABLE_FEATURE_DENYLIST_EXPORTER", "1")
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stdout = "some output"
    mock_result.stderr = "some real error"
    with patch("subprocess.run", return_value=mock_result):
        result = run_feature_denylist_proposal_exporter()
    assert result is False


# ---------------------------------------------------------------------------
# Phased rollout: module not found → warn + True (no-op)
# ---------------------------------------------------------------------------

def test_module_not_found_noop(monkeypatch):
    """ModuleNotFoundError in stderr → phased rollout no-op → True."""
    monkeypatch.setenv("ENABLE_FEATURE_DENYLIST_EXPORTER", "1")
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stdout = ""
    mock_result.stderr = "ModuleNotFoundError: No module named 'ml_analysis.tools.feature_denylist_proposal_exporter_v1'"
    with patch("subprocess.run", return_value=mock_result):
        result = run_feature_denylist_proposal_exporter()
    assert result is True


def test_no_module_named_noop(monkeypatch):
    """'No module named' in stderr → phased rollout no-op → True."""
    monkeypatch.setenv("ENABLE_FEATURE_DENYLIST_EXPORTER", "1")
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stdout = ""
    mock_result.stderr = "No module named 'ml_analysis'"
    with patch("subprocess.run", return_value=mock_result):
        result = run_feature_denylist_proposal_exporter()
    assert result is True


# ---------------------------------------------------------------------------
# Timeout handling
# ---------------------------------------------------------------------------

def test_timeout(monkeypatch):
    """subprocess.TimeoutExpired → False, warning logged."""
    monkeypatch.setenv("ENABLE_FEATURE_DENYLIST_EXPORTER", "1")
    monkeypatch.setenv("FEATURE_DENYLIST_EXPORTER_TIMEOUT_S", "5")
    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="python", timeout=5)):
        result = run_feature_denylist_proposal_exporter()
    assert result is False


# ---------------------------------------------------------------------------
# Generic exception
# ---------------------------------------------------------------------------

def test_exception(monkeypatch):
    """Any other exception in subprocess.run → False."""
    monkeypatch.setenv("ENABLE_FEATURE_DENYLIST_EXPORTER", "1")
    with patch("subprocess.run", side_effect=OSError("spawn failed")):
        result = run_feature_denylist_proposal_exporter()
    assert result is False


# ---------------------------------------------------------------------------
# Env var defaults injected into subprocess
# ---------------------------------------------------------------------------

def test_export_path_default_injected(monkeypatch):
    """FEATURE_DENYLIST_EXPORT_PATH default is injected when not set."""
    monkeypatch.setenv("ENABLE_FEATURE_DENYLIST_EXPORTER", "1")
    monkeypatch.delenv("FEATURE_DENYLIST_EXPORT_PATH", raising=False)
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = ""
    mock_result.stderr = ""
    with patch("subprocess.run", return_value=mock_result) as mock_run:
        run_feature_denylist_proposal_exporter()
    # env kwarg is passed as keyword
    call_kwargs = mock_run.call_args[1] or {}
    env_passed = call_kwargs.get("env") or {}
    assert "feature_denylist.prom" in env_passed.get("FEATURE_DENYLIST_EXPORT_PATH", "")


def test_custom_module(monkeypatch):
    """FEATURE_DENYLIST_EXPORTER_MODULE override is respected."""
    monkeypatch.setenv("ENABLE_FEATURE_DENYLIST_EXPORTER", "1")
    monkeypatch.setenv("FEATURE_DENYLIST_EXPORTER_MODULE", "my_custom.exporter")
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = ""
    mock_result.stderr = ""
    with patch("subprocess.run", return_value=mock_result) as mock_run:
        run_feature_denylist_proposal_exporter()
    args = mock_run.call_args[0][0]
    assert "my_custom.exporter" in args


def test_compile_both_workers():
    """Smoke-compile both of_timers_worker.py variants (P104/P80 regression guard).

    Uses ast.parse to avoid PermissionError on Docker-root-owned __pycache__.
    """
    import ast

    for path in (
        "services/of_timers_worker.py",
        "tick_flow_full/services/of_timers_worker.py",
    ):
        src = open(path, encoding="utf-8").read()
        ast.parse(src)  # raises SyntaxError on invalid Python
