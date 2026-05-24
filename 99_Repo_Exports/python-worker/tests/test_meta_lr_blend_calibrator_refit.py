"""Tests for meta_lr_blend posterior isotonic calibrator (2026-05-23 stop-bleed)."""
from __future__ import annotations

import json
import os
import random
import tempfile

import pytest

from common.isotonic_calibration import IsotonicCalibrator, fit_isotonic_pav
from tools.refit_meta_lr_blend_calibrator import (
    _atomic_write_json,
    _brier,
    _ece,
    fit_and_evaluate,
)


# ---------------------------------------------------------------------------
# IsotonicCalibrator: apply_one / to_dict / from_dict round-trip
# ---------------------------------------------------------------------------

class TestIsotonicCalibratorAdapter:
    def test_apply_one_matches_predict(self):
        cal = IsotonicCalibrator(x=[0.0, 0.5, 1.0], p=[0.05, 0.30, 0.85])
        for xq in [0.0, 0.25, 0.5, 0.75, 1.0]:
            assert cal.apply_one(xq) == pytest.approx(cal.predict(xq))

    def test_to_dict_includes_type_isotonic(self):
        cal = IsotonicCalibrator(x=[0.0, 1.0], p=[0.1, 0.9])
        d = cal.to_dict()
        assert d["type"] == "isotonic"
        assert d["x"] == [0.0, 1.0]
        assert d["p"] == [0.1, 0.9]
        assert d["mode"] == "linear"

    def test_from_dict_roundtrip(self):
        original = IsotonicCalibrator(x=[0.1, 0.4, 0.9], p=[0.05, 0.25, 0.85])
        d = original.to_dict()
        restored = IsotonicCalibrator.from_dict(d)
        for xq in [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]:
            assert restored.predict(xq) == pytest.approx(original.predict(xq), abs=1e-9)

    def test_from_dict_empty_safe(self):
        cal = IsotonicCalibrator.from_dict({"type": "isotonic"})
        # Empty → predict returns 0.5 (fail-open neutral)
        assert cal.apply_one(0.7) == 0.5


# ---------------------------------------------------------------------------
# PAV fit produces sane breakpoints
# ---------------------------------------------------------------------------

class TestPAVFit:
    def test_recovers_base_rate_for_saturated_model(self):
        """The pathological 2026-05-23 case: model outputs p_raw ≈ 1.0 for
        everything but actual WR ≈ 5%. PAV should flatten the top to ~5%.
        """
        rng = random.Random(42)
        pairs = []
        for _ in range(500):
            p = 0.95 + rng.random() * 0.05  # saturated 0.95-1.0
            win = 1 if rng.random() < 0.05 else 0  # actual WR 5%
            pairs.append((p, win))
        samples = [(p, float(w), 1.0) for p, w in pairs]
        cal = fit_isotonic_pav(samples)
        # Highest-bin calibrated probability should reflect actual base rate
        cal_at_top = cal.predict(0.99)
        assert 0.02 <= cal_at_top <= 0.12, (
            f"calibrated p at 0.99 should be near 0.05, got {cal_at_top:.3f}"
        )

    def test_monotonic_breakpoints(self):
        rng = random.Random(7)
        pairs = []
        # Well-behaved signal: p_raw ~ probability of win
        for _ in range(400):
            p = rng.random()
            win = 1 if rng.random() < p else 0
            pairs.append((p, win))
        samples = [(p, float(w), 1.0) for p, w in pairs]
        cal = fit_isotonic_pav(samples)
        # Output must be non-decreasing in input
        prev = -1.0
        for xq in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
            v = cal.predict(xq)
            assert v >= prev - 1e-9, f"non-monotonic at xq={xq}: {prev} → {v}"
            prev = v


# ---------------------------------------------------------------------------
# fit_and_evaluate: acceptance gate logic
# ---------------------------------------------------------------------------

class TestAcceptanceGate:
    def test_rejects_below_min_n(self):
        pairs = [(0.5, 0)] * 100
        out = fit_and_evaluate(pairs, min_n=300)
        assert out["accepted"] is False
        assert "insufficient_n" in out["reason"]

    def test_accepts_when_brier_improves(self):
        """Saturated raw → calibrator strongly improves Brier."""
        rng = random.Random(1)
        pairs = [(0.95 + rng.random() * 0.05,
                  1 if rng.random() < 0.05 else 0) for _ in range(500)]
        out = fit_and_evaluate(pairs, min_n=300,
                               require_brier_improvement=0.001,
                               require_ece_improvement=0.001)
        assert out["accepted"] is True, f"reason={out['reason']}"
        assert out["brier_cal"] < out["brier_raw"]
        assert out["ece_cal"] < out["ece_raw"]
        # Returned calibrator dict must be loader-compatible
        cal_dict = out["calibrator"]
        assert cal_dict["type"] == "isotonic"
        restored = IsotonicCalibrator.from_dict(cal_dict)
        assert 0.02 <= restored.apply_one(0.99) <= 0.12

    def test_rejects_when_improvement_too_small(self):
        """Already-calibrated model: tight requirement → rejection."""
        rng = random.Random(2)
        pairs = []
        for _ in range(500):
            p = rng.random()
            win = 1 if rng.random() < p else 0
            pairs.append((p, win))
        out = fit_and_evaluate(
            pairs, min_n=300,
            require_brier_improvement=0.05,  # very high bar
            require_ece_improvement=0.05,
        )
        assert out["accepted"] is False
        assert "insufficient_improvement" in out["reason"]


# ---------------------------------------------------------------------------
# Atomic write artifact + loader sibling discovery
# ---------------------------------------------------------------------------

class TestSiblingDiscovery:
    def test_atomic_write_creates_valid_json(self, tmp_path):
        target = tmp_path / "calibrator.json"
        payload = {"type": "isotonic", "x": [0.0, 1.0], "p": [0.1, 0.9], "mode": "linear"}
        _atomic_write_json(str(target), payload)
        assert target.exists()
        with open(target, encoding="utf-8") as f:
            reloaded = json.load(f)
        assert reloaded == payload

    def test_loader_helpers_handle_isotonic(self):
        """Round-trip via _build_calibrator_from_dict + _read_calibrator_file."""
        from services.ml_confirm.config_loader import (
            _build_calibrator_from_dict,
            _read_calibrator_file,
        )

        with tempfile.TemporaryDirectory() as d:
            cal_path = os.path.join(d, "calibrator.json")
            payload = {
                "type": "isotonic",
                "x": [0.0, 0.5, 1.0],
                "p": [0.05, 0.30, 0.85],
                "mode": "linear",
            }
            with open(cal_path, "w", encoding="utf-8") as f:
                json.dump(payload, f)

            loaded = _read_calibrator_file(cal_path)
            assert loaded == payload
            cal = _build_calibrator_from_dict(loaded, logger=None, src="test")
            assert cal is not None
            assert hasattr(cal, "apply_one")
            assert cal.apply_one(0.5) == pytest.approx(0.30, abs=1e-6)

    def test_loader_rejects_unknown_type(self):
        from services.ml_confirm.config_loader import _build_calibrator_from_dict

        out = _build_calibrator_from_dict(
            {"type": "nonexistent_kind"}, logger=None, src="test",
        )
        assert out is None


# ---------------------------------------------------------------------------
# Brier / ECE helpers
# ---------------------------------------------------------------------------

class TestMetrics:
    def test_brier_zero_for_perfect_predictions(self):
        probs = [0.0, 0.0, 1.0, 1.0]
        wins = [0, 0, 1, 1]
        assert _brier(probs, wins) == 0.0

    def test_brier_quarter_for_constant_half(self):
        probs = [0.5, 0.5, 0.5, 0.5]
        wins = [1, 0, 1, 0]
        assert _brier(probs, wins) == pytest.approx(0.25)

    def test_ece_zero_when_perfectly_calibrated(self):
        # All predictions land at 0.5 with empirical accuracy 0.5
        probs = [0.5] * 20
        wins = [1, 0] * 10
        assert _ece(probs, wins) == pytest.approx(0.0)
