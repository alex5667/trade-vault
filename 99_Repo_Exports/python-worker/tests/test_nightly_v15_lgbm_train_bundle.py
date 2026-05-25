"""Tests for nightly_v15_lgbm_train_bundle and promote_v15_lgbm_to_live."""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from tools.nightly_v15_lgbm_train_bundle import (
    acquire_lock, release_lock, preflight, count_positives_per_regime,
)
from tools.promote_v15_lgbm_to_live import _validate_pack


# ─── Lock primitives ──────────────────────────────────────────────────────────


class TestLockPrimitives:
    def test_acquire_lock_first_winner(self):
        r = MagicMock()
        r.set.return_value = True
        token = acquire_lock(r, "lock:test", 60)
        assert token is not None
        # Verify SET NX EX was used
        r.set.assert_called_once()
        kwargs = r.set.call_args.kwargs
        assert kwargs.get("nx") is True
        assert kwargs.get("ex") == 60

    def test_acquire_lock_contention(self):
        r = MagicMock()
        r.set.return_value = False  # someone else holds it
        token = acquire_lock(r, "lock:test", 60)
        assert token is None

    def test_release_lock_only_if_owner(self):
        r = MagicMock()
        r.eval.return_value = 1
        ok = release_lock(r, "lock:test", "tok-abc")
        assert ok is True
        # Args: lua_script, numkeys, key, token
        args = r.eval.call_args.args
        assert args[1] == 1
        assert args[2] == "lock:test"
        assert args[3] == "tok-abc"

    def test_release_lock_not_owner(self):
        r = MagicMock()
        r.eval.return_value = 0
        ok = release_lock(r, "lock:test", "stale-token")
        assert ok is False

    def test_release_lock_redis_error_safe(self):
        r = MagicMock()
        r.eval.side_effect = RuntimeError("redis down")
        ok = release_lock(r, "lock:test", "tok")
        assert ok is False


# ─── Preflight ────────────────────────────────────────────────────────────────


class TestPreflight:
    def test_preflight_skip_below_min_positives(self, monkeypatch):
        monkeypatch.setattr(
            "tools.nightly_v15_lgbm_train_bundle.count_positives_per_regime",
            lambda *a, **k: {"range": 30, "trending_bull": 20},
        )
        monkeypatch.setattr("tools.nightly_v15_lgbm_train_bundle.MIN_POSITIVES", 100)
        proceed, info = preflight("postgresql://stub")
        assert proceed is False
        assert "insufficient_positives" in info["reason"]
        assert info["total_positives"] == 50

    def test_preflight_proceeds_with_enough(self, monkeypatch):
        monkeypatch.setattr(
            "tools.nightly_v15_lgbm_train_bundle.count_positives_per_regime",
            lambda *a, **k: {"range": 80, "trending_bull": 50, "expansion": 30},
        )
        monkeypatch.setattr("tools.nightly_v15_lgbm_train_bundle.MIN_POSITIVES", 100)
        monkeypatch.setattr("tools.nightly_v15_lgbm_train_bundle.N_POSITIVE_DEGRADATION_FLOOR", 40)
        proceed, info = preflight("postgresql://stub")
        assert proceed is True
        assert info["total_positives"] == 160
        # 'expansion' has 30 < 40 floor → degraded
        assert "expansion" in info.get("degraded_regimes", {})

    def test_preflight_proceeds_no_degraded(self, monkeypatch):
        monkeypatch.setattr(
            "tools.nightly_v15_lgbm_train_bundle.count_positives_per_regime",
            lambda *a, **k: {"range": 80, "trending_bull": 80},
        )
        monkeypatch.setattr("tools.nightly_v15_lgbm_train_bundle.MIN_POSITIVES", 100)
        monkeypatch.setattr("tools.nightly_v15_lgbm_train_bundle.N_POSITIVE_DEGRADATION_FLOOR", 30)
        proceed, info = preflight("postgresql://stub")
        assert proceed is True
        assert "degraded_regimes" not in info

    def test_preflight_na_excluded_from_degradation_check(self, monkeypatch):
        # 'na' is not a real regime — degradation floor should not flag it
        monkeypatch.setattr(
            "tools.nightly_v15_lgbm_train_bundle.count_positives_per_regime",
            lambda *a, **k: {"na": 5, "range": 100},
        )
        monkeypatch.setattr("tools.nightly_v15_lgbm_train_bundle.MIN_POSITIVES", 100)
        monkeypatch.setattr("tools.nightly_v15_lgbm_train_bundle.N_POSITIVE_DEGRADATION_FLOOR", 30)
        proceed, info = preflight("postgresql://stub")
        assert proceed is True
        assert info.get("degraded_regimes", {}) == {}  # 'na' filtered out

    def test_preflight_empty_regime_dict_skips(self, monkeypatch):
        monkeypatch.setattr(
            "tools.nightly_v15_lgbm_train_bundle.count_positives_per_regime",
            lambda *a, **k: {},
        )
        monkeypatch.setattr("tools.nightly_v15_lgbm_train_bundle.MIN_POSITIVES", 100)
        proceed, info = preflight("postgresql://stub")
        assert proceed is False
        assert info["total_positives"] == 0


# ─── Promote validate_pack ────────────────────────────────────────────────────


def _make_pack(**overrides):
    base = {
        "kind": "edge_stack_v1",
        "gbdt": object(),
        "feature_cols": ["f_1", "f_2"],
        "feature_schema_ver": "v15_lgbm",
        "metrics": {"roc_auc_oof": 0.60},
    }
    base.update(overrides)
    return base


class TestValidatePack:
    def test_valid_pack(self):
        ok, reason = _validate_pack(_make_pack(), min_auc=0.55)
        assert ok is True
        assert reason == "ok"

    def test_missing_model_rejected(self):
        pack = _make_pack()
        del pack["gbdt"]
        ok, reason = _validate_pack(pack, min_auc=0.55)
        assert ok is False
        assert reason == "missing_model"

    def test_missing_feature_cols_rejected(self):
        pack = _make_pack()
        del pack["feature_cols"]
        ok, reason = _validate_pack(pack, min_auc=0.55)
        assert ok is False
        assert reason == "missing_feature_cols"

    def test_low_auc_rejected(self):
        pack = _make_pack(metrics={"roc_auc_oof": 0.40})
        ok, reason = _validate_pack(pack, min_auc=0.55)
        assert ok is False
        assert "roc_auc_below_threshold" in reason

    def test_wrong_schema_rejected(self):
        pack = _make_pack(feature_schema_ver="v14_of")
        ok, reason = _validate_pack(pack, min_auc=0.55)
        assert ok is False
        assert "schema_mismatch" in reason

    def test_alt_model_key_accepted(self):
        """Some packs use 'model' instead of 'gbdt' — also valid."""
        pack = _make_pack()
        del pack["gbdt"]
        pack["model"] = object()
        ok, reason = _validate_pack(pack, min_auc=0.55)
        assert ok is True

    def test_alt_feature_key_accepted(self):
        pack = _make_pack()
        del pack["feature_cols"]
        pack["feature_names"] = ["f"]
        ok, reason = _validate_pack(pack, min_auc=0.55)
        assert ok is True

    def test_v15_in_schema_name(self):
        pack = _make_pack(feature_schema_ver=None, schema_name="v15_lgbm_calibrated")
        ok, reason = _validate_pack(pack, min_auc=0.55)
        assert ok is True


# ─── count_positives_per_regime (integration-style with mock conn) ─────────────


class TestCountPositivesPerRegime:
    def test_fetches_and_aggregates(self):
        cur = MagicMock()
        cur.fetchall.return_value = [
            ("range", 120),
            ("trending_bull", 80),
            ("na", 5),
        ]
        cur.__enter__ = lambda self: cur
        cur.__exit__ = lambda *a: None
        conn = MagicMock()
        conn.cursor.return_value = cur
        with patch("psycopg2.connect", return_value=conn):
            out = count_positives_per_regime("postgresql://stub", lookback_days=30, label_thr_r=0.3)
        assert out == {"range": 120, "trending_bull": 80, "na": 5}

    def test_db_error_returns_empty(self):
        with patch("psycopg2.connect", side_effect=RuntimeError("conn refused")):
            out = count_positives_per_regime("postgresql://stub", lookback_days=30, label_thr_r=0.3)
        assert out == {}
