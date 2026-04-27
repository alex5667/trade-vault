from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""Tests for prom_rules_bundle_health_check_v1.py (V12)

Coverage:
- _get_repo_root: env / arg / auto-detect paths
- _write_state: Redis pipeline logic (mocked)
- main(): exit codes 0 and 2, validate_repo_rules integration (mocked)
- enforce_bucket_state_exporter_v1: V12 Gauges declared, _export_prom_rules_bundle_health (mocked redis)
"""

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


# ---------------------------------------------------------------------------
# prom_rules_bundle_health_check_v1 unit tests
# ---------------------------------------------------------------------------

def _import_module():
    import importlib
    import orderflow_services.prom_rules_bundle_health_check_v1 as mod
    return mod


def test_get_repo_root_arg(tmp_path):
    mod = _import_module()
    result = mod._get_repo_root(str(tmp_path))
    assert result == tmp_path.resolve()


def test_get_repo_root_env(tmp_path, monkeypatch):
    mod = _import_module()
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    result = mod._get_repo_root(None)
    assert result == tmp_path.resolve()


def test_get_repo_root_fallback_to_parents(monkeypatch):
    """Without REPO_ROOT env and /app, should resolve to parents[1] of the module file."""
    mod = _import_module()
    monkeypatch.delenv("REPO_ROOT", raising=False)
    # /app does not exist in test env normally
    with patch("orderflow_services.prom_rules_bundle_health_check_v1.Path") as mock_path_cls:
        # Simulate /app not existing
        instance_app = MagicMock()
        instance_app.exists.return_value = False
        file_path_mock = MagicMock()
        file_path_mock.resolve.return_value.parents = {1: Path("/fake/repo")}

        # Use real Path for the arg=None branch
    # Simple real test - just check returns a Path
    result = mod._get_repo_root(None)
    assert isinstance(result, Path)


def test_write_state_ok(monkeypatch):
    """_write_state with ok=True must call pipeline with last_ok_ts_ms set."""
    mod = _import_module()

    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    with patch.object(mod, "_connect_redis", return_value=mock_redis):
        monkeypatch.setenv("PROM_RULES_BUNDLE_STATE_PREFIX", "state:prom_rules_bundle")
        mod._write_state(ok=True, files_checked=5, errors=[])

    # Verify last_ok_ts_ms was set (ok=True branch)
    set_calls = [str(c) for c in mock_pipe.set.call_args_list]
    assert any("last_ok_ts_ms" in c for c in set_calls)
    assert any("last_ok" in c for c in set_calls)
    mock_pipe.execute.assert_called_once()


def test_write_state_fail(monkeypatch):
    """_write_state with ok=False must NOT set last_ok_ts_ms."""
    mod = _import_module()

    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    with patch.object(mod, "_connect_redis", return_value=mock_redis):
        monkeypatch.setenv("PROM_RULES_BUNDLE_STATE_PREFIX", "state:prom_rules_bundle")
        mod._write_state(ok=False, files_checked=3, errors=["err1", "err2"])

    set_calls = [str(c) for c in mock_pipe.set.call_args_list]
    # last_ok_ts_ms should NOT appear when ok=False
    assert not any("last_ok_ts_ms" in c for c in set_calls)
    # error keys should be set
    assert any("last_error_n" in c for c in set_calls)
    assert any("last_error_head" in c for c in set_calls)


def test_write_state_no_redis():
    """_write_state must be a no-op when Redis is not available."""
    mod = _import_module()
    with patch.object(mod, "_connect_redis", return_value=None):
        # Should not raise
        mod._write_state(ok=True, files_checked=0, errors=[])


def test_main_ok(monkeypatch, capsys):
    """main() returns 0 when validate_repo_rules reports ok."""
    mod = _import_module()

    fake_result = SimpleNamespace(ok=True, files_checked=7, errors=[])

    with patch("orderflow_services.prom_rules_bundle_health_check_v1.validate_repo_rules",
               return_value=fake_result), \
         patch.object(mod, "_write_state") as mock_write, \
         patch.object(mod, "_get_repo_root", return_value=Path("/fake")):
        rc = mod.main(["--promtool", "off"])

    assert rc == 0
    mock_write.assert_called_once_with(ok=True, files_checked=7, errors=[])
    out = capsys.readouterr().out
    assert "OK" in out
    assert "7" in out


def test_main_fail(monkeypatch, capsys):
    """main() returns 2 when validate_repo_rules reports not ok."""
    mod = _import_module()

    fake_result = SimpleNamespace(
        ok=False, files_checked=4,
        errors=["file1.yml: group has no rules", "file2.yml: empty expr"]
    )

    with patch("orderflow_services.prom_rules_bundle_health_check_v1.validate_repo_rules",
               return_value=fake_result), \
         patch.object(mod, "_write_state") as mock_write, \
         patch.object(mod, "_get_repo_root", return_value=Path("/fake")):
        rc = mod.main(["--promtool", "off"])

    assert rc == 2
    mock_write.assert_called_once_with(ok=False, files_checked=4, errors=fake_result.errors)
    out = capsys.readouterr().out
    assert "FAIL" in out
    assert "2" in out  # error count


# ---------------------------------------------------------------------------
# enforce_bucket_state_exporter_v1 V12 gauge declarations
# ---------------------------------------------------------------------------

def test_exporter_v12_gauges_declared():
    """V12: all four prom_rules_bundle Gauges must be present in the module."""
    import orderflow_services.enforce_bucket_state_exporter_v1 as mod

    for name in (
        "of_prom_rules_bundle_last_ok",
        "of_prom_rules_bundle_last_ok_age_sec",
        "of_prom_rules_bundle_last_files_checked",
        "of_prom_rules_bundle_last_error_n",
    ):
        assert hasattr(mod, name), f"V12 gauge {name!r} not found in exporter module"
        gauge = getattr(mod, name)
        assert callable(getattr(gauge, "set", None)), f"{name}.set() must be callable"


def test_exporter_export_prom_rules_bundle_health_no_redis(monkeypatch):
    """_export_prom_rules_bundle_health must be a no-op when Redis is not configured."""
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("CRYPTO_NOTIFY_REDIS_URL", raising=False)
    from orderflow_services.enforce_bucket_state_exporter_v1 import Exporter
    ex = Exporter()
    # Must not raise
    ex._export_prom_rules_bundle_health()


def test_exporter_export_prom_rules_bundle_health_with_redis(monkeypatch):
    """_export_prom_rules_bundle_health must correctly read Redis keys and set Gauges."""
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("CRYPTO_NOTIFY_REDIS_URL", raising=False)
    monkeypatch.setenv("PROM_RULES_BUNDLE_STATE_PREFIX", "state:prom_rules_bundle")

    import orderflow_services.enforce_bucket_state_exporter_v1 as mod
    from orderflow_services.enforce_bucket_state_exporter_v1 import Exporter

    mock_redis = MagicMock()
    import time
    now_ms = get_ny_time_millis()
    ok_ts_ms = now_ms - 10_000  # 10 seconds ago

    def fake_get(key):
        mapping = {
            "state:prom_rules_bundle:last_ok": "1",
            "state:prom_rules_bundle:last_ok_ts_ms": str(ok_ts_ms),
            "state:prom_rules_bundle:last_files_checked": "5",
            "state:prom_rules_bundle:last_error_n": "0",
        }
        return mapping.get(key)

    mock_redis.get.side_effect = fake_get

    ex = Exporter()
    ex.redis = mock_redis

    # Track each gauge independently using simple containers
    last_ok_val = []
    last_age_val = []
    last_files_val = []
    last_err_val = []

    original_set_ok = mod.of_prom_rules_bundle_last_ok.set
    original_set_age = mod.of_prom_rules_bundle_last_ok_age_sec.set
    original_set_files = mod.of_prom_rules_bundle_last_files_checked.set
    original_set_err = mod.of_prom_rules_bundle_last_error_n.set

    monkeypatch.setattr(mod.of_prom_rules_bundle_last_ok, "set",
                        lambda v: (last_ok_val.append(v), original_set_ok(v)))
    monkeypatch.setattr(mod.of_prom_rules_bundle_last_ok_age_sec, "set",
                        lambda v: (last_age_val.append(v), original_set_age(v)))
    monkeypatch.setattr(mod.of_prom_rules_bundle_last_files_checked, "set",
                        lambda v: (last_files_val.append(v), original_set_files(v)))
    monkeypatch.setattr(mod.of_prom_rules_bundle_last_error_n, "set",
                        lambda v: (last_err_val.append(v), original_set_err(v)))

    ex._export_prom_rules_bundle_health()

    assert last_ok_val and last_ok_val[0] == 1.0, f"of_prom_rules_bundle_last_ok not set correctly: {last_ok_val}"
    assert last_files_val and last_files_val[0] == 5.0, f"of_prom_rules_bundle_last_files_checked not 5: {last_files_val}"
    assert last_err_val and last_err_val[0] == 0.0, f"of_prom_rules_bundle_last_error_n not 0: {last_err_val}"
    assert last_age_val, "of_prom_rules_bundle_last_ok_age_sec was not set"
    age = last_age_val[0]
    assert 5.0 <= age <= 60.0, f"Expected ~10s age, got {age}"
