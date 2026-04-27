"""
Unit tests for tools/ml_scorer_calibrator.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

# Ensure tools directory is in path
tools_path = Path(__file__).parent.parent / "tools"
if str(tools_path) not in sys.path:
    sys.path.insert(0, str(tools_path))

# ── Import module under test ──────────────────────────────────────────────── #
from ml_scorer_calibrator import (
    _should_propose,
    _build_proposal_bundle,
    _holddown_ok,
    _load_current_mode,
    _extract_shadow_fields,
    _spearman_rank_corr,
    main,
)


# ─────────────────────────────── _spearman_rank_corr tests ───────────────── #

class TestSpearmanRankCorr:
    def test_perfect_positive(self):
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [10.0, 20.0, 30.0, 40.0, 50.0]
        corr = _spearman_rank_corr(x, y)
        assert abs(corr - 1.0) < 1e-6

    def test_perfect_negative(self):
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [50.0, 40.0, 30.0, 20.0, 10.0]
        corr = _spearman_rank_corr(x, y)
        assert abs(corr - (-1.0)) < 1e-6

    def test_no_correlation(self):
        """Two unrelated sequences should have low correlation."""
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [3.0, 1.0, 5.0, 2.0, 4.0]
        corr = _spearman_rank_corr(x, y)
        assert abs(corr) < 0.5  # loose check

    def test_too_few_values(self):
        assert _spearman_rank_corr([1.0, 2.0], [3.0, 4.0]) == 0.0
        assert _spearman_rank_corr([], []) == 0.0


# ────────────────────────────── _extract_shadow_fields tests ─────────────── #

class TestExtractShadowFields:
    def test_valid_payload(self):
        payload = json.dumps({
            "sid": "test_sid_1",
            "indicators": {
                "confidence_v1": 0.65,
                "ml_shadow_conf01": 0.72,
                "ml_shadow_predicted_r": 1.5,
                "ml_shadow_veto": 1,
                "confidence_breakdown": {
                    "scorer_mode": "shadow",
                },
            },
        })
        result = _extract_shadow_fields(payload)
        assert result is not None
        assert result["sid"] == "test_sid_1"
        assert abs(result["ml_shadow_conf01"] - 0.72) < 1e-6
        assert result["ml_shadow_veto"] == 1
        assert abs(result["rule_conf01"] - 0.65) < 1e-6

    def test_no_shadow_data(self):
        payload = json.dumps({
            "sid": "test_sid_2",
            "indicators": {
                "confidence_v1": 0.65,
            },
        })
        result = _extract_shadow_fields(payload)
        assert result is None

    def test_invalid_json(self):
        assert _extract_shadow_fields("not json") is None

    def test_empty_payload(self):
        assert _extract_shadow_fields("") is None


# ─────────────────────────────────── _should_propose tests ───────────────── #

def _good_stats(
    scored=60,
    spearman=0.10,
    veto_count=30,
    veto_with_outcome=20,
    veto_negative=14,
    veto_r_sum=-3.0,
) -> dict:
    veto_precision = veto_negative / max(veto_with_outcome, 1)
    pnl_impact = -veto_r_sum
    return {
        "total_decisions": 500,
        "shadow_scored_count": 100,
        "scored_with_outcome": scored,
        "spearman_corr": spearman,
        "rmse_rule": 0.5,
        "rmse_ml": 0.45,
        "rmse_improvement_pct": 10.0,
        "shadow_veto_count": veto_count,
        "shadow_veto_with_outcome": veto_with_outcome,
        "shadow_veto_negative": veto_negative,
        "shadow_veto_precision": veto_precision,
        "shadow_veto_r_sum": veto_r_sum,
        "pnl_impact_r": pnl_impact,
        "pass_count": 80,
        "pass_r_sum": 5.0,
    }


class TestShouldPropose:
    def test_happy_path(self):
        stats = _good_stats()
        ok, reason = _should_propose(
            stats=stats, holddown_ok=True, min_scored_trades=50,
            min_spearman=0.05, min_veto_precision=0.55,
        )
        assert ok is True
        assert reason == "ok"

    def test_low_spearman(self):
        stats = _good_stats(spearman=0.01)
        ok, reason = _should_propose(
            stats=stats, holddown_ok=True, min_scored_trades=50,
            min_spearman=0.05, min_veto_precision=0.55,
        )
        assert ok is False
        assert "spearman" in reason

    def test_low_scored_trades(self):
        stats = _good_stats(scored=10)
        ok, reason = _should_propose(
            stats=stats, holddown_ok=True, min_scored_trades=50,
            min_spearman=0.05, min_veto_precision=0.55,
        )
        assert ok is False
        assert "scored_trades" in reason

    def test_low_veto_precision(self):
        stats = _good_stats(veto_negative=5, veto_with_outcome=20)  # 25%
        ok, reason = _should_propose(
            stats=stats, holddown_ok=True, min_scored_trades=50,
            min_spearman=0.05, min_veto_precision=0.55,
        )
        assert ok is False
        assert "veto_precision" in reason

    def test_negative_pnl_impact(self):
        stats = _good_stats(veto_r_sum=+2.0)  # pnl_impact = -2.0
        ok, reason = _should_propose(
            stats=stats, holddown_ok=True, min_scored_trades=50,
            min_spearman=0.05, min_veto_precision=0.55,
        )
        assert ok is False
        assert "pnl_impact" in reason

    def test_holddown_not_expired(self):
        stats = _good_stats()
        ok, reason = _should_propose(
            stats=stats, holddown_ok=False, min_scored_trades=50,
            min_spearman=0.05, min_veto_precision=0.55,
        )
        assert ok is False
        assert "holddown" in reason

    def test_no_veto_data_still_passes(self):
        """If no vetoes happened, should still pass (vetoes are optional)."""
        stats = _good_stats(veto_count=0, veto_with_outcome=0, veto_negative=0, veto_r_sum=0.0)
        ok, reason = _should_propose(
            stats=stats, holddown_ok=True, min_scored_trades=50,
            min_spearman=0.05, min_veto_precision=0.55,
        )
        assert ok is True
        assert reason == "ok"


# ────────────────────────────── build_proposal_bundle tests ──────────────── #

class TestBuildProposalBundle:
    def test_bundle_structure(self):
        bid, sig, bundle = _build_proposal_bundle("cfg:ml_scorer:mode", "test_secret")
        assert len(bid) == 12  # secrets.token_hex(6)
        assert len(sig) == 8
        assert bundle["who"] == "ml_scorer_calibrator"
        assert len(bundle["ops"]) == 1
        assert bundle["ops"][0]["op"] == "SET"
        assert bundle["ops"][0]["key"] == "cfg:ml_scorer:mode"
        assert bundle["ops"][0]["value"] == "enforce"

    def test_different_key(self):
        bid, sig, bundle = _build_proposal_bundle("cfg:custom:key", "secret")
        assert bundle["ops"][0]["key"] == "cfg:custom:key"


# ─────────────────────────────────────── holddown tests ──────────────────── #

class TestHolddown:
    def test_no_key_ok(self):
        mock_r = Mock()
        mock_r.get.return_value = None
        ok, elapsed = _holddown_ok(mock_r, "meta:ml_scorer_cal:last_step_ms", 72.0)
        assert ok is True
        assert elapsed > 900

    def test_expired(self):
        import time
        mock_r = Mock()
        past_ms = int((time.time() - 80 * 3600) * 1000)  # 80h ago
        mock_r.get.return_value = str(past_ms)
        ok, elapsed = _holddown_ok(mock_r, "meta:ml_scorer_cal:last_step_ms", 72.0)
        assert ok is True
        assert elapsed > 70

    def test_not_expired(self):
        import time
        mock_r = Mock()
        recent_ms = int((time.time() - 24 * 3600) * 1000)  # 24h ago
        mock_r.get.return_value = str(recent_ms)
        ok, elapsed = _holddown_ok(mock_r, "meta:ml_scorer_cal:last_step_ms", 72.0)
        assert ok is False
        assert elapsed < 30


# ─────────────────────────────── load_current_mode tests ─────────────────── #

class TestLoadCurrentMode:
    def test_from_redis(self):
        mock_r = Mock()
        mock_r.get.return_value = "shadow"
        assert _load_current_mode(mock_r, "cfg:ml_scorer:mode") == "shadow"

    def test_enforce_from_redis(self):
        mock_r = Mock()
        mock_r.get.return_value = "enforce"
        assert _load_current_mode(mock_r, "cfg:ml_scorer:mode") == "enforce"

    def test_fallback_to_env(self, monkeypatch):
        mock_r = Mock()
        mock_r.get.return_value = None
        monkeypatch.setenv("ML_SCORER_MODE", "shadow")
        assert _load_current_mode(mock_r, "cfg:ml_scorer:mode") == "shadow"


# ──────────────────────────────────── integration: main() tests ───────────── #

def _make_decision_event(sid: str, ml_shadow_conf: float, ml_shadow_veto: int = 0) -> dict:
    payload = json.dumps({
        "sid": sid,
        "indicators": {
            "confidence_v1": 0.60,
            "ml_shadow_conf01": ml_shadow_conf,
            "ml_shadow_predicted_r": 0.5,
            "ml_shadow_veto": ml_shadow_veto,
            "confidence_breakdown": {
                "scorer_mode": "shadow",
            },
        },
    })
    return {"sid": sid, "symbol": "BTCUSDT", "ts_ms": "1700000000000", "payload": payload}


def _make_trade(sid: str, r_mult: float) -> dict:
    return {"sid": sid, "r_mult": str(r_mult), "ts_ms": str(int(1700000000000))}


class TestMainDryRun:
    @patch("ml_scorer_calibrator._get_redis")
    def test_dry_run_no_bundle_write(self, mock_get_redis):
        mock_r = MagicMock()

        def get_side(k):
            if k == "cfg:ml_scorer:mode":
                return "shadow"
            return None

        mock_r.get.side_effect = get_side
        mock_r.exists.return_value = 0

        # 60 decision events with shadow data
        decision_events = [
            (f"1700000000{i:03d}-0", _make_decision_event(f"sid_{i}", 0.5 + i * 0.005))
            for i in range(60)
        ]

        def xrange_side(stream, min="-", max="+", count=None):
            if "decisions" in stream:
                return decision_events[:count or len(decision_events)]
            return []

        def xrevrange_side(stream, max="+", count=None):
            if "trades" in stream:
                return [
                    (f"1700000000{i:03d}-0", _make_trade(f"sid_{i}", -0.3 if i % 3 == 0 else 0.5))
                    for i in range(60)
                ]
            return []

        mock_r.xrange.side_effect = xrange_side
        mock_r.xrevrange.side_effect = xrevrange_side
        mock_get_redis.return_value = mock_r

        with patch("sys.argv", ["cal.py", "--dry-run", "--hours", "168"]):
            with pytest.raises(SystemExit) as exc:
                main()
        assert exc.value.code == 0

        # Must NOT write bundle in dry-run
        mock_r.set.assert_not_called()
        mock_r.xadd.assert_not_called()


class TestMainPendingGuard:
    @patch("ml_scorer_calibrator._get_redis")
    def test_pending_key_exists_skip(self, mock_get_redis):
        mock_r = MagicMock()
        mock_r.get.return_value = "shadow"
        mock_r.exists.return_value = 1  # pending key exists

        mock_get_redis.return_value = mock_r

        with patch("sys.argv", ["cal.py", "--hours", "168"]):
            with pytest.raises(SystemExit) as exc:
                main()
        assert exc.value.code == 0
        # Should not have read any streams
        mock_r.xrange.assert_not_called()


class TestMainAlreadyEnforce:
    @patch("ml_scorer_calibrator._get_redis")
    def test_already_enforce(self, mock_get_redis):
        mock_r = MagicMock()
        mock_r.get.return_value = "enforce"
        mock_r.exists.return_value = 0
        mock_get_redis.return_value = mock_r

        with patch("sys.argv", ["cal.py", "--dry-run"]):
            with pytest.raises(SystemExit) as exc:
                main()
        assert exc.value.code == 0
