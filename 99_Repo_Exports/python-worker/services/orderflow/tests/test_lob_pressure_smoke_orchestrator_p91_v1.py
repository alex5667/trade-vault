"""Unit tests for P91 LOB pressure smoke-check orchestration in of_timers_worker.

Covers:
  - ENABLE_LOB_PRESSURE_SMOKE gate
  - rc=0 -> OK
  - rc=2 -> ALERT -> notify (dedup allows)
  - dedup suppression

Runs for both worker modules:
  - services.of_timers_worker
  - tick_flow_full.services.of_timers_worker
"""

from __future__ import annotations

import importlib
import json
import os
from unittest.mock import patch

import pytest


def _reload(module_path: str):
    mod = importlib.import_module(module_path)
    importlib.reload(mod)
    return mod


WORKERS = [
    "services.of_timers_worker",
    "tick_flow_full.services.of_timers_worker",
]


@pytest.mark.parametrize("worker_mod", WORKERS)
def test_disabled_by_env(worker_mod):
    w = _reload(worker_mod)
    with patch.dict(os.environ, {"ENABLE_LOB_PRESSURE_SMOKE": "0"}):
        with patch.object(w, "run_tool_rc") as mock_run:
            assert w.run_lob_pressure_smoke_check() is True
            mock_run.assert_not_called()


@pytest.mark.parametrize("worker_mod", WORKERS)
def test_rc0_ok(worker_mod):
    w = _reload(worker_mod)
    with patch.dict(os.environ, {"ENABLE_LOB_PRESSURE_SMOKE": "1"}):
        with patch.object(w, "run_tool_rc", return_value=(0, "ok\n", "")):
            with patch.object(w, "_notify_stream") as mock_notify:
                assert w.run_lob_pressure_smoke_check() is True
                mock_notify.assert_not_called()


@pytest.mark.parametrize("worker_mod", WORKERS)
def test_rc2_alert_notifies(worker_mod):
    w = _reload(worker_mod)
    payload = {
        "no_data": 0,
        "n_recent": 250,
        "missing_max_share": 1.0,
        "stuck_lob": 1,
        "issues": ["missing_max_share>0.250", "stuck_lob"],
    }
    stdout = json.dumps(payload)

    with patch.dict(os.environ, {"ENABLE_LOB_PRESSURE_SMOKE": "1"}):
        with patch.object(w, "run_tool_rc", return_value=(2, stdout, "")):
            with patch.object(w, "_dedup_allow", return_value=True):
                with patch.object(w, "_notify_stream") as mock_notify:
                    assert w.run_lob_pressure_smoke_check() is False
                    assert mock_notify.call_count == 1
                    msg = mock_notify.call_args[0][0]
                    assert "LOB_PRESSURE_SMOKE" in msg
                    assert "missing_max_share" in msg


@pytest.mark.parametrize("worker_mod", WORKERS)
def test_dedup_suppresses(worker_mod):
    w = _reload(worker_mod)
    payload = {"no_data": 0, "n_recent": 250, "missing_max_share": 1.0, "stuck_lob": 0, "issues": ["x"]}
    with patch.dict(os.environ, {"ENABLE_LOB_PRESSURE_SMOKE": "1"}):
        with patch.object(w, "run_tool_rc", return_value=(2, json.dumps(payload), "")):
            with patch.object(w, "_dedup_allow", return_value=False):
                with patch.object(w, "_notify_stream") as mock_notify:
                    assert w.run_lob_pressure_smoke_check() is False
                    mock_notify.assert_not_called()
