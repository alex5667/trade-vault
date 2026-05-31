"""Tests for atr_floor_enrichment_autocal_v1."""
from __future__ import annotations

import os
import tempfile
import time
from typing import Any
from unittest.mock import MagicMock, patch

from orderflow_services.atr_floor_enrichment_autocal_v1 import (
    AtrFloorEnrichmentAutocal,
    Cfg,
    _set_env_var_in_file,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg(**kwargs: Any) -> Cfg:
    defaults = dict(
        enable=True,
        interval=300,
        window_h=2.0,
        fill_rate_min=50.0,
        p99_max_delta_ms=3.0,
        dwell_h=24.0,
        execute_promote=False,
        env_file="/tmp/test_crypto_of.env",
        prom_port=9176,
        redis_url="redis://localhost:6379/0",
        db_dsn="",
        prom_url="http://prometheus:9090",
    )
    defaults.update(kwargs)
    return Cfg(**defaults)


def _make_autocal(cfg: Cfg | None = None) -> AtrFloorEnrichmentAutocal:
    cfg = cfg or _make_cfg()
    ac = AtrFloorEnrichmentAutocal.__new__(AtrFloorEnrichmentAutocal)
    ac.cfg = cfg
    ac.rc = MagicMock()
    ac.rc.hgetall.return_value = {}
    ac._state = {}
    ac._p99_baseline = None
    return ac


# ---------------------------------------------------------------------------
# _set_env_var_in_file
# ---------------------------------------------------------------------------

def test_set_env_var_creates_file_with_key():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
        path = f.name
    os.unlink(path)  # remove so we test creation-on-write

    _set_env_var_in_file(path, "ATR_FLOOR_ENRICHMENT_EARLY", "1")
    with open(path) as f:
        content = f.read()
    assert "ATR_FLOOR_ENRICHMENT_EARLY=1" in content
    os.unlink(path)


def test_set_env_var_appends_to_existing():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
        f.write("FOO=bar\nBAZ=qux\n")
        path = f.name

    _set_env_var_in_file(path, "ATR_FLOOR_ENRICHMENT_EARLY", "1")
    with open(path) as f:
        lines = f.readlines()
    keys = [l.split("=")[0].strip() for l in lines if "=" in l]
    assert "FOO" in keys
    assert "BAZ" in keys
    assert "ATR_FLOOR_ENRICHMENT_EARLY" in keys
    os.unlink(path)


def test_set_env_var_replaces_existing():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
        f.write("FOO=bar\nATR_FLOOR_ENRICHMENT_EARLY=0\nBAZ=qux\n")
        path = f.name

    _set_env_var_in_file(path, "ATR_FLOOR_ENRICHMENT_EARLY", "1")
    with open(path) as f:
        content = f.read()
    assert "ATR_FLOOR_ENRICHMENT_EARLY=1" in content
    assert "ATR_FLOOR_ENRICHMENT_EARLY=0" not in content
    os.unlink(path)


def test_set_env_var_no_duplicates():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
        f.write("ATR_FLOOR_ENRICHMENT_EARLY=0\n")
        path = f.name

    _set_env_var_in_file(path, "ATR_FLOOR_ENRICHMENT_EARLY", "1")
    with open(path) as f:
        lines = [l for l in f.readlines() if "ATR_FLOOR_ENRICHMENT_EARLY" in l and "=" in l]
    assert len(lines) == 1
    os.unlink(path)


# ---------------------------------------------------------------------------
# Phase transitions
# ---------------------------------------------------------------------------

def test_idle_when_not_enabled():
    ac = _make_autocal(_make_cfg(enable=False))
    ac._set_phase("idle")
    assert ac._phase() == "idle"


def test_monitoring_to_dwell_when_criteria_met():
    ac = _make_autocal()
    ac._state["phase"] = "monitoring"
    ac._p99_baseline = 10.0

    with patch.object(ac, "_check_criteria", return_value=True):
        ac._state["phase_entered_ms"] = str(int((time.time() - 1) * 1000))
        ac.step_inner(fill_rate=60.0, p99=10.5)

    assert ac._phase() == "dwell"
    assert "dwell_start_ms" in ac._state


def test_dwell_resets_to_monitoring_when_criteria_lost():
    ac = _make_autocal()
    ac._state["phase"] = "dwell"
    ac._state["phase_entered_ms"] = str(int((time.time() - 100) * 1000))
    ac._p99_baseline = 10.0

    with patch.object(ac, "_check_criteria", return_value=False):
        ac.step_inner(fill_rate=30.0, p99=10.5)

    assert ac._phase() == "monitoring"


def test_dwell_stays_until_dwell_h_elapsed():
    ac = _make_autocal(_make_cfg(dwell_h=24.0))
    ac._state["phase"] = "dwell"
    ac._state["phase_entered_ms"] = str(int((time.time() - 3600) * 1000))  # 1h elapsed
    ac._p99_baseline = 10.0

    with patch.object(ac, "_check_criteria", return_value=True):
        ac.step_inner(fill_rate=60.0, p99=10.5)

    assert ac._phase() == "dwell"  # still in dwell, not 24h yet


def test_dwell_promotes_after_dwell_h_elapsed():
    ac = _make_autocal(_make_cfg(dwell_h=0.001))  # 3.6 seconds
    ac._state["phase"] = "dwell"
    ac._state["phase_entered_ms"] = str(int((time.time() - 10) * 1000))  # 10s elapsed
    ac._p99_baseline = 10.0

    with patch.object(ac, "_check_criteria", return_value=True):
        ac.step_inner(fill_rate=60.0, p99=10.5)

    assert ac._phase() == "promote_ready"


def test_promote_ready_writes_env_when_execute_promote():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
        f.write("FOO=1\n")
        path = f.name

    ac = _make_autocal(_make_cfg(execute_promote=True, env_file=path))
    ac._state["phase"] = "promote_ready"
    ac._state["phase_entered_ms"] = str(int((time.time() - 1) * 1000))
    ac._p99_baseline = 10.0

    with patch.object(ac, "_check_criteria", return_value=True):
        ac.step_inner(fill_rate=60.0, p99=10.5)

    assert ac._phase() == "promoted"
    with open(path) as f:
        content = f.read()
    assert "ATR_FLOOR_ENRICHMENT_EARLY=1" in content
    os.unlink(path)


def test_promote_ready_no_write_when_execute_promote_false():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
        f.write("FOO=1\n")
        path = f.name

    ac = _make_autocal(_make_cfg(execute_promote=False, env_file=path))
    ac._state["phase"] = "promote_ready"
    ac._state["phase_entered_ms"] = str(int((time.time() - 1) * 1000))
    ac._p99_baseline = 10.0

    with patch.object(ac, "_check_criteria", return_value=True):
        ac.step_inner(fill_rate=60.0, p99=10.5)

    assert ac._phase() == "promote_ready"  # stayed
    with open(path) as f:
        content = f.read()
    assert "ATR_FLOOR_ENRICHMENT_EARLY" not in content
    os.unlink(path)


def test_promoted_emits_rollback_signal_on_low_fill_rate():
    ac = _make_autocal()
    ac._state["phase"] = "promoted"
    ac._state["phase_entered_ms"] = str(int((time.time() - 1) * 1000))
    ac._p99_baseline = 10.0

    with patch.object(ac, "_check_criteria", return_value=False):
        ac.step_inner(fill_rate=20.0, p99=10.5)  # 20% << 50% * 0.7 = 35%

    # rollback_signal should have been emitted
    assert ac._state.get("last_rollback_signal_ms") is not None


# ---------------------------------------------------------------------------
# Criteria check
# ---------------------------------------------------------------------------

def test_criteria_fails_when_fill_rate_none():
    ac = _make_autocal()
    ac._p99_baseline = 10.0
    assert not ac._check_criteria(None, 10.5)


def test_criteria_fails_when_fill_rate_below_threshold():
    ac = _make_autocal()
    ac._p99_baseline = 10.0
    assert not ac._check_criteria(49.9, 10.5)


def test_criteria_passes_when_fill_rate_above_threshold():
    ac = _make_autocal()
    ac._p99_baseline = 10.0
    assert ac._check_criteria(50.1, None)  # p99 unknown → skip p99 check


def test_criteria_fails_when_p99_delta_too_high():
    ac = _make_autocal(_make_cfg(p99_max_delta_ms=3.0))
    ac._p99_baseline = 10.0
    assert not ac._check_criteria(60.0, 14.0)  # delta = 4.0 > 3.0


def test_criteria_passes_when_p99_delta_within_threshold():
    ac = _make_autocal(_make_cfg(p99_max_delta_ms=3.0))
    ac._p99_baseline = 10.0
    assert ac._check_criteria(60.0, 12.9)  # delta = 2.9 < 3.0


def test_criteria_skips_p99_check_when_no_baseline():
    ac = _make_autocal()
    ac._p99_baseline = None
    assert ac._check_criteria(60.0, 200.0)  # no baseline → ignore p99


# ---------------------------------------------------------------------------
# Idle mode — no state transitions
# ---------------------------------------------------------------------------

def test_idle_mode_no_phase_change():
    ac = _make_autocal(_make_cfg(enable=False))
    ac._state["phase"] = "idle"
    ac._p99_baseline = 10.0

    ac.step_inner(fill_rate=80.0, p99=10.5)

    assert ac._phase() == "idle"
