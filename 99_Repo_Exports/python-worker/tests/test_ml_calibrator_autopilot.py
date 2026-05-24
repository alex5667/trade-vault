"""Tests for ml_calibrator_autopilot_v1 (2026-05-23 follow-up)."""
from __future__ import annotations

import json
import os
import random
from unittest import mock

import pytest

from orderflow_services.ml_calibrator_autopilot_v1 import (
    _count_kind_matched,
    _decide_target_version,
    _discover_kinds,
    _load_state,
    _save_state,
)


# ---------------------------------------------------------------------------
# Fake Redis for unit isolation (subset of methods needed by autopilot)
# ---------------------------------------------------------------------------

class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def get(self, key: str) -> bytes | None:
        v = self.store.get(key)
        return v.encode() if v is not None else None

    def set(self, key: str, value: str) -> None:
        self.store[key] = value


# ---------------------------------------------------------------------------
# _discover_kinds: read champion + challenger cfgs from Redis
# ---------------------------------------------------------------------------

class TestDiscoverKinds:
    def test_picks_up_champion_and_challenger(self):
        r = FakeRedis()
        r.set("cfg:ml_confirm:champion", json.dumps({
            "kind": "meta_lr", "model_path": "/tmp/meta_lr.joblib",
        }))
        r.set("cfg:ml_confirm:challenger", json.dumps({
            "kind": "edge_stack_v1", "model_path": "/tmp/edge_stack_v1.joblib",
        }))
        out = _discover_kinds(r)
        kinds = {k for k, _ in out}
        assert kinds == {"meta_lr", "edge_stack_v1"}

    def test_champion_wins_when_same_kind(self):
        r = FakeRedis()
        r.set("cfg:ml_confirm:champion", json.dumps({
            "kind": "meta_lr", "model_path": "/tmp/champion.joblib",
        }))
        r.set("cfg:ml_confirm:challenger", json.dumps({
            "kind": "meta_lr", "model_path": "/tmp/challenger.joblib",
        }))
        out = dict(_discover_kinds(r))
        assert out["meta_lr"] == "/tmp/champion.joblib"

    def test_empty_when_no_cfgs(self):
        r = FakeRedis()
        assert _discover_kinds(r) == []

    def test_skips_malformed_payload(self):
        r = FakeRedis()
        r.set("cfg:ml_confirm:champion", "not-json")
        r.set("cfg:ml_confirm:challenger", json.dumps({
            "kind": "edge_stack_v1", "model_path": "/tmp/x.joblib",
        }))
        out = _discover_kinds(r)
        assert out == [("edge_stack_v1", "/tmp/x.joblib")]


# ---------------------------------------------------------------------------
# _decide_target_version: bootstrap → auto-switch → sticky lock
# ---------------------------------------------------------------------------

class TestDecideTargetVersion:
    def test_bootstrap_returns_empty_when_n_below_threshold(self):
        r = FakeRedis()
        target, locked, reason = _decide_target_version(
            r, kind="meta_lr", pairs_kind_matched=50, switch_after_n=300,
        )
        assert target == ""
        assert locked is False
        assert reason == "broad_bootstrap"

    def test_auto_switches_when_n_meets_threshold(self):
        r = FakeRedis()
        target, locked, reason = _decide_target_version(
            r, kind="meta_lr", pairs_kind_matched=350, switch_after_n=300,
        )
        assert target == "meta_lr"
        assert locked is True
        assert reason == "auto_switch"
        # State persisted
        state = _load_state(r, "meta_lr")
        assert state["locked"] is True
        assert state["lock_n"] == 350

    def test_sticky_lock_holds_even_if_n_drops(self):
        r = FakeRedis()
        _save_state(r, "meta_lr", {"locked": True, "lock_ts_ms": 1, "lock_n": 500})
        target, locked, reason = _decide_target_version(
            r, kind="meta_lr", pairs_kind_matched=10, switch_after_n=300,
        )
        assert target == "meta_lr"
        assert locked is True
        assert reason == "sticky_lock"

    def test_env_override_wins(self, monkeypatch):
        r = FakeRedis()
        monkeypatch.setenv("ML_AUTOPILOT_FORCE_TARGET_VER_META_LR", "frozen_meta_lr_v42")
        target, locked, reason = _decide_target_version(
            r, kind="meta_lr", pairs_kind_matched=10, switch_after_n=300,
        )
        assert target == "frozen_meta_lr_v42"
        assert locked is True
        assert reason == "env_override"


# ---------------------------------------------------------------------------
# _count_kind_matched
# ---------------------------------------------------------------------------

class TestKindMatching:
    def test_substring_case_insensitive(self):
        pairs = [
            (0.5, 1, "meta_lr_blend_20260516"),
            (0.6, 0, "META_LR_BLEND_20260517"),
            (0.7, 1, "edge_stack_v1"),
            (0.8, 0, ""),
        ]
        assert _count_kind_matched(pairs, "meta_lr_blend") == 2
        assert _count_kind_matched(pairs, "meta_lr") == 2  # substring of blend
        assert _count_kind_matched(pairs, "edge_stack") == 1
        assert _count_kind_matched(pairs, "nonexistent") == 0


# ---------------------------------------------------------------------------
# Integration: _run_once with fake Redis + filesystem
# ---------------------------------------------------------------------------

class TestRunOnceIntegration:
    def test_skipped_kind_does_not_run(self, monkeypatch, tmp_path):
        from orderflow_services.ml_calibrator_autopilot_v1 import _run_once

        r = FakeRedis()
        model_path = tmp_path / "meta_lr.joblib"
        model_path.write_text("{}")
        r.set("cfg:ml_confirm:champion", json.dumps({
            "kind": "meta_lr", "model_path": str(model_path),
        }))
        monkeypatch.setenv("ML_AUTOPILOT_SKIP_KINDS", "meta_lr")
        monkeypatch.setenv("ML_AUTOPILOT_LOOKBACK_H", "1")
        with mock.patch(
            "orderflow_services.ml_calibrator_autopilot_v1._read_trades_closed_with_version",
            return_value=[],
        ):
            _run_once(r)
        # No calibrator.json written
        assert not (tmp_path / "calibrator.json").exists()

    def test_missing_model_path_does_not_crash(self, monkeypatch, tmp_path):
        from orderflow_services.ml_calibrator_autopilot_v1 import _run_once

        r = FakeRedis()
        r.set("cfg:ml_confirm:champion", json.dumps({
            "kind": "meta_lr", "model_path": str(tmp_path / "nonexistent.joblib"),
        }))
        monkeypatch.setenv("ML_AUTOPILOT_LOOKBACK_H", "1")
        with mock.patch(
            "orderflow_services.ml_calibrator_autopilot_v1._read_trades_closed_with_version",
            return_value=[],
        ):
            _run_once(r)
        # Should complete without exception

    def test_writes_calibrator_on_saturated_data(self, monkeypatch, tmp_path):
        """End-to-end: discover meta_lr → fit on saturated data → write artifact."""
        from orderflow_services.ml_calibrator_autopilot_v1 import _run_once

        r = FakeRedis()
        model_path = tmp_path / "meta_lr.joblib"
        model_path.write_text("{}")
        r.set("cfg:ml_confirm:champion", json.dumps({
            "kind": "meta_lr", "model_path": str(model_path),
        }))

        rng = random.Random(42)
        fake_pairs = [
            (0.95 + rng.random() * 0.05,
             1 if rng.random() < 0.05 else 0,
             "")  # empty ml_version → broad mode
            for _ in range(500)
        ]
        monkeypatch.setenv("ML_AUTOPILOT_MIN_N", "300")
        monkeypatch.setenv("ML_AUTOPILOT_BRIER_DELTA", "0.001")
        monkeypatch.setenv("ML_AUTOPILOT_ECE_DELTA", "0.001")
        with mock.patch(
            "orderflow_services.ml_calibrator_autopilot_v1._read_trades_closed_with_version",
            return_value=fake_pairs,
        ):
            _run_once(r)

        # BOTH artifacts must exist: model-specific (per-kind safety when
        # multiple kinds share a dir) AND generic (back-compat with the
        # meta_lr_blend refit script).
        cal_specific = tmp_path / "calibrator_meta_lr.json"
        cal_generic = tmp_path / "calibrator.json"
        assert cal_specific.exists(), "calibrator_meta_lr.json should have been written"
        assert cal_generic.exists(), "calibrator.json should have been written (back-compat)"

        cal = json.loads(cal_specific.read_text())
        assert cal["type"] == "isotonic"
        assert cal["meta"]["kind"] == "meta_lr_posterior"
        assert cal["meta"]["service"] == "ml_calibrator_autopilot_v1"
        assert cal["meta"]["target_version_mode"] == "broad"

    def test_two_kinds_in_same_dir_do_not_overwrite_each_other(
        self, monkeypatch, tmp_path,
    ):
        """Regression guard for the 2026-05-23 finding: champion (meta_lr)
        and challenger (edge_stack_v1) shared /var/lib/trade/of_reports/
        models/ → both wrote to the same calibrator.json and the second
        kind silently overwrote the first.
        """
        from orderflow_services.ml_calibrator_autopilot_v1 import _run_once

        r = FakeRedis()
        meta_lr_path = tmp_path / "meta_lr_v15.json"
        edge_path = tmp_path / "edge_stack_v15.joblib"
        meta_lr_path.write_text("{}")
        edge_path.write_text("{}")

        r.set("cfg:ml_confirm:champion", json.dumps({
            "kind": "meta_lr", "model_path": str(meta_lr_path),
        }))
        r.set("cfg:ml_confirm:challenger", json.dumps({
            "kind": "edge_stack_v1", "model_path": str(edge_path),
        }))

        rng = random.Random(7)
        fake_pairs = [
            (0.95 + rng.random() * 0.05,
             1 if rng.random() < 0.05 else 0,
             "")
            for _ in range(500)
        ]
        monkeypatch.setenv("ML_AUTOPILOT_MIN_N", "300")
        monkeypatch.setenv("ML_AUTOPILOT_BRIER_DELTA", "0.001")
        monkeypatch.setenv("ML_AUTOPILOT_ECE_DELTA", "0.001")
        with mock.patch(
            "orderflow_services.ml_calibrator_autopilot_v1._read_trades_closed_with_version",
            return_value=fake_pairs,
        ):
            _run_once(r)

        # Each kind must have its own model-specific calibrator file
        meta_lr_cal = tmp_path / "calibrator_meta_lr_v15.json"
        edge_cal = tmp_path / "calibrator_edge_stack_v15.json"
        assert meta_lr_cal.exists(), "meta_lr-specific calibrator missing"
        assert edge_cal.exists(), "edge_stack_v1-specific calibrator missing"

        meta_lr_meta = json.loads(meta_lr_cal.read_text())["meta"]
        edge_meta = json.loads(edge_cal.read_text())["meta"]
        assert meta_lr_meta["kind"] == "meta_lr_posterior"
        assert edge_meta["kind"] == "edge_stack_v1_posterior"

    def test_auto_switches_to_kind_locked_when_ml_version_populated(
        self, monkeypatch, tmp_path,
    ):
        from orderflow_services.ml_calibrator_autopilot_v1 import _run_once

        r = FakeRedis()
        model_path = tmp_path / "meta_lr.joblib"
        model_path.write_text("{}")
        r.set("cfg:ml_confirm:champion", json.dumps({
            "kind": "meta_lr", "model_path": str(model_path),
        }))

        rng = random.Random(99)
        # All rows tagged with ml_version containing "meta_lr"
        fake_pairs = [
            (0.95 + rng.random() * 0.05,
             1 if rng.random() < 0.05 else 0,
             "meta_lr_20260523")
            for _ in range(400)
        ]
        monkeypatch.setenv("ML_AUTOPILOT_MIN_N", "300")
        monkeypatch.setenv("ML_AUTOPILOT_AUTO_SWITCH_AFTER_N", "300")
        monkeypatch.setenv("ML_AUTOPILOT_BRIER_DELTA", "0.001")
        monkeypatch.setenv("ML_AUTOPILOT_ECE_DELTA", "0.001")
        with mock.patch(
            "orderflow_services.ml_calibrator_autopilot_v1._read_trades_closed_with_version",
            return_value=fake_pairs,
        ):
            _run_once(r)

        # State should now be locked
        state = _load_state(r, "meta_lr")
        assert state.get("locked") is True
        assert state.get("lock_n", 0) >= 300

        # Artifact meta should reflect kind-locked mode
        cal_path = tmp_path / "calibrator.json"
        cal = json.loads(cal_path.read_text())
        assert cal["meta"]["target_version_mode"] == "kind_locked"
        assert cal["meta"]["target_version"] == "meta_lr"
