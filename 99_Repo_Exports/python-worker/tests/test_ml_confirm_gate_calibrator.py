"""
Unit tests for tools/ml_confirm_gate_calibrator.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, call

import pytest

# Ensure tools directory is in path
tools_path = Path(__file__).parent.parent / "tools"
if str(tools_path) not in sys.path:
    sys.path.insert(0, str(tools_path))

# ── Import module under test ──────────────────────────────────────────────── #
from ml_confirm_gate_calibrator import (
    _ladder_next,
    _precision_threshold,
    _should_propose,
    _build_proposal_bundle,
    _load_champion_cfg,
    _save_champion_cfg,
    _holddown_ok,
    main,
    LADDER_LEVELS,
)


# ────────────────────────────────────────── ladder_next tests ─────────────── #

class TestLadderNext:
    def test_from_zero(self):
        assert _ladder_next(0.0) == 0.05

    def test_from_005(self):
        assert _ladder_next(0.05) == 0.20

    def test_from_02(self):
        assert abs(_ladder_next(0.20) - 0.50) < 1e-9

    def test_from_05(self):
        assert abs(_ladder_next(0.50) - 1.00) < 1e-9

    def test_from_10_at_top(self):
        assert _ladder_next(1.0) is None

    def test_from_099(self):
        """Just below 1.0 should still give 1.0."""
        assert abs(_ladder_next(0.99) - 1.00) < 1e-9


# ──────────────────────────────── precision threshold tests ───────────────── #

class TestPrecisionThreshold:
    def test_l1_default(self):
        assert abs(_precision_threshold(0.05) - 0.55) < 1e-9

    def test_l2_default(self):
        assert abs(_precision_threshold(0.20) - 0.57) < 1e-9

    def test_l3_default(self):
        assert abs(_precision_threshold(0.50) - 0.60) < 1e-9

    def test_l4_default(self):
        assert abs(_precision_threshold(1.00) - 0.62) < 1e-9

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("ML_CAL_PRECISION_L1", "0.65")
        assert abs(_precision_threshold(0.05) - 0.65) < 1e-9


# ─────────────────────────────────── _should_propose tests ───────────────── #

def _good_stats(precision=0.60, veto_count=50, veto_with_outcome=40, veto_negative=24, veto_r_sum=-5.0) -> dict:
    pnl_impact = -veto_r_sum
    return {
        "total_events": 200,
        "shadow_veto_count": veto_count,
        "enforce_veto_count": 0,
        "veto_with_outcome": veto_with_outcome,
        "veto_negative": veto_negative,
        "veto_r_sum": veto_r_sum,
        "precision_veto": precision,
        "mean_r_vetoed": veto_r_sum / max(veto_with_outcome, 1),
        "pnl_impact_r": pnl_impact,
        "pass_with_outcome": 100,
        "pass_r_sum": 10.0,
    }


class TestShouldPropose:
    def test_step0_to_005_happy_path(self):
        stats = _good_stats(precision=0.60, veto_count=50, veto_r_sum=-5.0)
        ok, reason = _should_propose(next_share=0.05, stats=stats, holddown_ok=True, min_veto_hits=30)
        assert ok is True
        assert reason == "ok"

    def test_step0_low_precision(self):
        stats = _good_stats(precision=0.40, veto_count=50, veto_r_sum=-5.0)
        ok, reason = _should_propose(next_share=0.05, stats=stats, holddown_ok=True, min_veto_hits=30)
        assert ok is False
        assert "precision" in reason

    def test_step0_low_veto_count(self):
        stats = _good_stats(precision=0.60, veto_count=5, veto_r_sum=-5.0)
        ok, reason = _should_propose(next_share=0.05, stats=stats, holddown_ok=True, min_veto_hits=30)
        assert ok is False
        assert "veto_count" in reason

    def test_step0_positive_r_sum(self):
        """If vetoed trades were winners (r_sum > 0), don't promote to 0.05."""
        stats = _good_stats(precision=0.60, veto_count=50, veto_r_sum=+2.0)
        ok, reason = _should_propose(next_share=0.05, stats=stats, holddown_ok=True, min_veto_hits=30)
        assert ok is False
        assert "veto_r_sum" in reason

    def test_holddown_not_expired(self):
        stats = _good_stats(precision=0.60, veto_count=50, veto_r_sum=-5.0)
        ok, reason = _should_propose(next_share=0.05, stats=stats, holddown_ok=False, min_veto_hits=30)
        assert ok is False
        assert "holddown" in reason

    def test_step_005_to_02_happy(self):
        """Step from 0.05→0.20: require pnl_impact>0 (pnl_impact = -veto_r_sum)."""
        stats = _good_stats(precision=0.60, veto_count=50, veto_r_sum=-5.0)  # pnl_impact=5.0
        ok, reason = _should_propose(next_share=0.20, stats=stats, holddown_ok=True, min_veto_hits=30)
        assert ok is True

    def test_step_005_to_02_neg_pnl(self):
        stats = _good_stats(precision=0.60, veto_count=50, veto_r_sum=+2.0)  # pnl_impact=-2.0
        ok, reason = _should_propose(next_share=0.20, stats=stats, holddown_ok=True, min_veto_hits=30)
        assert ok is False
        assert "pnl_impact" in reason

    def test_step_to_10_high_precision(self):
        stats = _good_stats(precision=0.65, veto_count=60, veto_r_sum=-10.0)
        ok, reason = _should_propose(next_share=1.00, stats=stats, holddown_ok=True, min_veto_hits=30)
        assert ok is True

    def test_step_to_10_low_precision(self):
        stats = _good_stats(precision=0.60, veto_count=60, veto_r_sum=-10.0)
        ok, reason = _should_propose(next_share=1.00, stats=stats, holddown_ok=True, min_veto_hits=30)
        assert ok is False  # precision 0.60 < 0.62

    def test_insufficient_veto_with_outcome(self):
        stats = _good_stats(precision=0.60, veto_count=50, veto_with_outcome=5, veto_r_sum=-5.0)
        ok, reason = _should_propose(next_share=0.05, stats=stats, holddown_ok=True, min_veto_hits=30)
        assert ok is False
        assert "veto_with_outcome" in reason


# ────────────────────────────── build_proposal_bundle tests ──────────────── #

class TestBuildProposalBundle:
    def test_bundle_structure(self):
        cfg = {"kind": "edge_stack_v1", "enforce_share": 0.0, "model_path": "/some/model.joblib"}
        bid, sig, bundle = _build_proposal_bundle("cfg:ml_confirm:champion", cfg, 0.05, "test_secret")
        assert len(bid) == 12  # secrets.token_hex(6)
        assert len(sig) == 8
        assert bundle["who"] == "ml_confirm_calibrator"
        assert len(bundle["ops"]) == 1
        assert bundle["ops"][0]["op"] == "SET"
        # Verify the serialized newcfg has enforce_share=0.05
        new_cfg = json.loads(bundle["ops"][0]["value"])
        assert abs(new_cfg["enforce_share"] - 0.05) < 1e-9
        assert new_cfg["updated_by"] == "ml_confirm_calibrator"

    def test_original_cfg_unchanged(self):
        """Building bundle must NOT mutate the original cfg dict."""
        cfg = {"enforce_share": 0.0, "kind": "edge_stack_v1"}
        _build_proposal_bundle("key", cfg, 0.05, "secret")
        assert cfg["enforce_share"] == 0.0


# ─────────────────────────────────────── holddown tests ──────────────────── #

class TestHolddown:
    def test_no_key_ok(self):
        mock_r = Mock()
        mock_r.get.return_value = None
        ok, elapsed = _holddown_ok(mock_r, "meta:ml_cal:last_step_ms", 72.0)
        assert ok is True
        assert elapsed > 900

    def test_expired(self):
        import time
        mock_r = Mock()
        past_ms = int((time.time() - 80 * 3600) * 1000)  # 80h ago
        mock_r.get.return_value = str(past_ms)
        ok, elapsed = _holddown_ok(mock_r, "meta:ml_cal:last_step_ms", 72.0)
        assert ok is True
        assert elapsed > 70

    def test_not_expired(self):
        import time
        mock_r = Mock()
        recent_ms = int((time.time() - 24 * 3600) * 1000)  # 24h ago
        mock_r.get.return_value = str(recent_ms)
        ok, elapsed = _holddown_ok(mock_r, "meta:ml_cal:last_step_ms", 72.0)
        assert ok is False
        assert elapsed < 30


# ──────────────────────────────────── integration: main() tests ───────────── #

def _make_champion(enforce_share: float = 0.0) -> str:
    return json.dumps({
        "kind": "edge_stack_v1",
        "model_path": "/var/lib/trade/ml_models/edge_stack_v1/champions/model.joblib",
        "run_id": "test_run",
        "mode": "SHADOW",
        "enforce_share": enforce_share,
    })


def _make_ml_event(allow: int, sid: str, mode: str = "SHADOW") -> dict:
    return {"allow": str(allow), "sid": sid, "mode": mode, "score": "0.1", "bucket": "trend"}


def _make_trade(sid: str, r_mult: float) -> dict:
    return {"sid": sid, "r_mult": str(r_mult), "ts_ms": str(int(1700000000000))}


class TestMainDryRun:
    @patch("ml_confirm_gate_calibrator._get_redis")
    def test_dry_run_no_bundle_write(self, mock_get_redis):
        mock_r = MagicMock()
        mock_r.get.side_effect = lambda k: (
            _make_champion(0.0) if k == "cfg:ml_confirm:champion" else None
        )
        mock_r.exists.return_value = 0
        # Mock xrange to return 50 shadow-veto events
        veto_events = [(f"1700000000{i:03d}-0", _make_ml_event(0, f"sid_{i}")) for i in range(50)]
        passed_events = [(f"1700000001{i:03d}-0", _make_ml_event(1, f"sid_pass_{i}")) for i in range(100)]
        all_events = veto_events + passed_events

        def xrange_side(stream, min="-", max="+", count=None):
            if "ml_confirm" in stream:
                return all_events[:count or len(all_events)]
            return []

        def xrevrange_side(stream, max="+", count=None):
            if "trades" in stream:
                # Return matching trades for vetoed sids with negative r_mult
                return [(f"1700000000{i:03d}-0", _make_trade(f"sid_{i}", -0.5)) for i in range(40)]
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
    @patch("ml_confirm_gate_calibrator._get_redis")
    def test_pending_key_exists_skip(self, mock_get_redis):
        mock_r = MagicMock()
        mock_r.get.return_value = _make_champion(0.0)
        mock_r.exists.return_value = 1  # pending key exists

        mock_get_redis.return_value = mock_r

        with patch("sys.argv", ["cal.py", "--hours", "168"]):
            with pytest.raises(SystemExit) as exc:
                main()
        assert exc.value.code == 0
        # Should not have read any streams
        mock_r.xrange.assert_not_called()


class TestMainNoChampion:
    @patch("ml_confirm_gate_calibrator._get_redis")
    def test_no_champion_cfg(self, mock_get_redis):
        mock_r = MagicMock()
        mock_r.get.return_value = None
        mock_r.exists.return_value = 0
        mock_get_redis.return_value = mock_r

        with patch("sys.argv", ["cal.py", "--dry-run"]):
            with pytest.raises(SystemExit) as exc:
                main()
        assert exc.value.code == 0


class TestMainAlreadyAtTop:
    @patch("ml_confirm_gate_calibrator._get_redis")
    def test_enforce_share_at_10(self, mock_get_redis):
        mock_r = MagicMock()
        mock_r.get.return_value = _make_champion(1.0)
        mock_r.exists.return_value = 0
        mock_get_redis.return_value = mock_r

        with patch("sys.argv", ["cal.py", "--dry-run"]):
            with pytest.raises(SystemExit) as exc:
                main()
        assert exc.value.code == 0
