"""Tests for tools/promote_v14_of_to_live.py — the v14_of challenger→live
promotion script.

These tests cover the promotion logic in isolation (no live Redis) by patching
the `_redis` helper to return a stub.
"""
from __future__ import annotations

import json
import os
from unittest.mock import patch

import pytest

from tools.promote_v14_of_to_live import (
    _atomic_copy,
    _read_candidate,
    _read_live_meta,
    _write_live_meta,
    main,
)


class _FakeRedis:
    def __init__(self, store: dict[str, str] | None = None):
        self.store = dict(store or {})
        self.last_set: tuple[str, str, int] | None = None

    def ping(self):
        return True

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None):
        self.last_set = (key, value, ex or 0)
        self.store[key] = value


class TestCandidateRead:
    def test_read_candidate_returns_dict(self):
        r = _FakeRedis({
            "cfg:ml_confirm:v14_of:gbdt_candidate": json.dumps({
                "run_id": "x", "model_path": "/tmp/x.joblib",
                "metrics": {"roc_auc_oof": 0.85}, "mode": "SHADOW",
            })
        })
        out = _read_candidate(r, "cfg:ml_confirm:v14_of:gbdt_candidate")
        assert out is not None
        assert out["run_id"] == "x"
        assert out["metrics"]["roc_auc_oof"] == 0.85

    def test_read_candidate_missing_returns_none(self):
        r = _FakeRedis()
        assert _read_candidate(r, "missing:key") is None

    def test_read_candidate_invalid_json_returns_none(self):
        r = _FakeRedis({"bad:key": "not-json-{{"})
        assert _read_candidate(r, "bad:key") is None


class TestAtomicCopy:
    def test_atomic_copy_creates_destination(self, tmp_path):
        src = tmp_path / "src.joblib"
        src.write_bytes(b"\x80\x04model-bytes")
        dst = tmp_path / "subdir" / "scorer_v14_of.joblib"
        _atomic_copy(str(src), str(dst))
        assert dst.exists()
        assert dst.read_bytes() == b"\x80\x04model-bytes"
        # No leftover .tmp file
        assert not (tmp_path / "subdir" / "scorer_v14_of.joblib.tmp").exists()


class TestLiveMeta:
    def test_write_and_read_meta_roundtrip(self, tmp_path):
        live = tmp_path / "live.joblib"
        live.write_bytes(b"")
        cand = {
            "run_id": "rid-1",
            "model_path": "/tmp/src.joblib",
            "feature_schema_ver": "v14_of",
            "model_signature": "deadbeef",
            "metrics": {"roc_auc_oof": 0.87},
        }
        _write_live_meta(str(live), cand)
        out = _read_live_meta(str(live))
        assert out is not None
        assert out["run_id"] == "rid-1"
        assert out["feature_schema_ver"] == "v14_of"
        assert out["metrics"]["roc_auc_oof"] == 0.87

    def test_read_missing_meta_returns_none(self, tmp_path):
        live = tmp_path / "missing.joblib"
        assert _read_live_meta(str(live)) is None


class TestMainPromotion:
    def _setup_candidate(self, tmp_path, *, roc_auc: float, mode: str = "SHADOW") -> dict:
        src = tmp_path / "challenger.joblib"
        src.write_bytes(b"model-payload")
        cand = {
            "run_id": "test-run-1",
            "model_path": str(src),
            "feature_schema_ver": "v14_of",
            "mode": mode,
            "metrics": {"roc_auc_oof": roc_auc},
        }
        return cand

    def test_promote_happy_path(self, tmp_path, monkeypatch):
        cand = self._setup_candidate(tmp_path, roc_auc=0.80)
        live_path = str(tmp_path / "live" / "scorer_v14_of.joblib")
        fake_r = _FakeRedis({"cfg:ml_confirm:v14_of:gbdt_candidate": json.dumps(cand)})

        monkeypatch.setattr("tools.promote_v14_of_to_live._redis", lambda: fake_r)
        monkeypatch.setattr("sys.argv", ["prog", "--live-path", live_path, "--min-roc-auc", "0.65"])

        rc = main()
        assert rc == 0
        assert os.path.isfile(live_path)
        # Audit metric written
        assert fake_r.last_set is not None
        assert fake_r.last_set[0] == "metrics:promotion:v14_of:last"
        audit = json.loads(fake_r.last_set[1])
        assert audit["run_id"] == "test-run-1"
        assert audit["roc_auc_oof"] == 0.80
        # Meta sidecar
        meta = _read_live_meta(live_path)
        assert meta is not None
        assert meta["run_id"] == "test-run-1"

    def test_skip_when_roc_auc_below_threshold(self, tmp_path, monkeypatch):
        cand = self._setup_candidate(tmp_path, roc_auc=0.55)
        live_path = str(tmp_path / "live" / "scorer_v14_of.joblib")
        fake_r = _FakeRedis({"cfg:ml_confirm:v14_of:gbdt_candidate": json.dumps(cand)})
        monkeypatch.setattr("tools.promote_v14_of_to_live._redis", lambda: fake_r)
        monkeypatch.setattr("sys.argv", ["prog", "--live-path", live_path, "--min-roc-auc", "0.65"])

        rc = main()
        assert rc == 0
        assert not os.path.isfile(live_path)  # SKIPPED
        assert fake_r.last_set is None  # No audit metric written

    def test_idempotent_same_run_id(self, tmp_path, monkeypatch):
        cand = self._setup_candidate(tmp_path, roc_auc=0.80)
        live_path = str(tmp_path / "live" / "scorer_v14_of.joblib")
        # Pre-create live model + meta with the SAME run_id
        os.makedirs(os.path.dirname(live_path), exist_ok=True)
        with open(live_path, "wb") as f:
            f.write(b"existing")
        _write_live_meta(live_path, cand)
        existing_mtime = os.path.getmtime(live_path)

        fake_r = _FakeRedis({"cfg:ml_confirm:v14_of:gbdt_candidate": json.dumps(cand)})
        monkeypatch.setattr("tools.promote_v14_of_to_live._redis", lambda: fake_r)
        monkeypatch.setattr("sys.argv", ["prog", "--live-path", live_path])

        rc = main()
        assert rc == 0
        # File untouched (idempotent skip)
        assert os.path.getmtime(live_path) == existing_mtime

    def test_force_bypasses_guards(self, tmp_path, monkeypatch):
        cand = self._setup_candidate(tmp_path, roc_auc=0.40)  # below threshold
        live_path = str(tmp_path / "live" / "scorer_v14_of.joblib")
        fake_r = _FakeRedis({"cfg:ml_confirm:v14_of:gbdt_candidate": json.dumps(cand)})
        monkeypatch.setattr("tools.promote_v14_of_to_live._redis", lambda: fake_r)
        monkeypatch.setattr("sys.argv", ["prog", "--live-path", live_path,
                                        "--min-roc-auc", "0.65", "--force"])
        rc = main()
        assert rc == 0
        assert os.path.isfile(live_path)

    def test_dry_run_does_not_write(self, tmp_path, monkeypatch):
        cand = self._setup_candidate(tmp_path, roc_auc=0.80)
        live_path = str(tmp_path / "live" / "scorer_v14_of.joblib")
        fake_r = _FakeRedis({"cfg:ml_confirm:v14_of:gbdt_candidate": json.dumps(cand)})
        monkeypatch.setattr("tools.promote_v14_of_to_live._redis", lambda: fake_r)
        monkeypatch.setattr("sys.argv", ["prog", "--live-path", live_path, "--dry-run"])
        rc = main()
        assert rc == 0
        assert not os.path.isfile(live_path)

    def test_skip_when_mode_not_shadow(self, tmp_path, monkeypatch):
        cand = self._setup_candidate(tmp_path, roc_auc=0.80, mode="ENFORCE")
        live_path = str(tmp_path / "live" / "scorer_v14_of.joblib")
        fake_r = _FakeRedis({"cfg:ml_confirm:v14_of:gbdt_candidate": json.dumps(cand)})
        monkeypatch.setattr("tools.promote_v14_of_to_live._redis", lambda: fake_r)
        monkeypatch.setattr("sys.argv", ["prog", "--live-path", live_path])
        rc = main()
        assert rc == 0
        assert not os.path.isfile(live_path)
