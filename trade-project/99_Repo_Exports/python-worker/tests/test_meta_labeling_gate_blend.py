"""Tests for the EnsembleWeightsReader blend wiring in MetaLabelGate."""
from __future__ import annotations

import math
from unittest.mock import MagicMock, patch

import pytest

from services.meta_labeling_gate import MetaLabelGate, reset_gate


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.delenv("ENSEMBLE_WEIGHTS_READ_ENABLED", raising=False)
    reset_gate()
    yield
    reset_gate()


def _gate(enabled: bool = False) -> MetaLabelGate:
    rc = MagicMock()
    rc.get.return_value = None  # no model
    return MetaLabelGate(rc=rc, enabled=enabled, model_ttl_sec=1)


# ─── _maybe_blend boundary cases ────────────────────────────────────────────


def test_blend_skipped_when_symbol_empty():
    g = _gate()
    p, sources = g._maybe_blend(0.6, {"p_edge": 0.7}, symbol="")
    assert p == 0.6
    assert sources == {}


def test_blend_skipped_when_no_extra_sources():
    g = _gate()
    p, sources = g._maybe_blend(0.6, {"foo": "bar"}, symbol="BTCUSDT")
    assert p == 0.6
    assert sources == {}


def test_blend_skipped_when_reader_disabled(monkeypatch):
    # Reader exists but ENSEMBLE_WEIGHTS_READ_ENABLED=0 → blend returns equal-weight
    g = _gate()
    p, sources = g._maybe_blend(0.6, {"p_edge": 0.7}, symbol="BTCUSDT")
    # equal-weight blend of 0.6 + 0.7 in logit space
    # logit(.6)=0.405, logit(.7)=0.847 → mean=0.626 → inv_logit ≈ 0.652
    assert math.isclose(p, 0.652, abs_tol=0.01)
    assert set(sources.keys()) == {"meta_label", "p_edge"}


def test_blend_uses_reader_weights_when_enabled(monkeypatch):
    monkeypatch.setenv("ENSEMBLE_WEIGHTS_READ_ENABLED", "1")
    g = _gate()
    g.rc.hgetall.return_value = {"meta_label": "0.9", "p_edge": "0.1"}
    p, sources = g._maybe_blend(0.6, {"p_edge": 0.95}, symbol="BTCUSDT")
    # With 90% weight on meta_label (0.6), result should be much closer to 0.6 than 0.95
    assert 0.6 <= p < 0.75
    assert set(sources.keys()) == {"meta_label", "p_edge"}


def test_blend_filters_invalid_probs():
    g = _gate()
    p, sources = g._maybe_blend(
        0.6,
        {"p_edge": 1.5, "garbage_prob": "junk", "good_prob": 0.4},
        symbol="BTCUSDT",
    )
    # p_edge=1.5 invalid (>=1.0), garbage_prob invalid (non-numeric), good_prob valid
    # → blend over meta_label + good_prob
    assert "good_prob" in sources
    assert "p_edge" not in sources
    assert "garbage_prob" not in sources
    assert "meta_label" in sources


def test_blend_picks_up_arbitrary_prob_keys():
    g = _gate()
    p, sources = g._maybe_blend(
        0.6,
        {"of_score_prob": 0.7, "ml_confirm_prob": 0.8},
        symbol="BTCUSDT",
    )
    assert set(sources.keys()) == {"meta_label", "of_score_prob", "ml_confirm_prob"}


def test_blend_handles_reader_init_failure():
    # First call sets reader to False sentinel — subsequent calls noop
    g = _gate()
    with patch(
        "services.ensemble_weights_reader.EnsembleWeightsReader.__init__",
        side_effect=Exception("init boom"),
    ):
        p, sources = g._maybe_blend(0.6, {"p_edge": 0.7}, symbol="BTCUSDT")
    assert p == 0.6
    assert sources == {}
    # Second call should not retry init
    p2, _ = g._maybe_blend(0.6, {"p_edge": 0.7}, symbol="BTCUSDT")
    assert p2 == 0.6


def test_blend_handles_reader_blend_exception():
    g = _gate()
    bad_reader = MagicMock()
    bad_reader.blend.side_effect = RuntimeError("blend exploded")
    g._ensemble_reader = bad_reader
    p, sources = g._maybe_blend(0.6, {"p_edge": 0.7}, symbol="BTCUSDT")
    # Falls back to original meta_prob on exception
    assert p == 0.6


# ─── evaluate() integration ─────────────────────────────────────────────────


@patch("calibration.meta_labeling_model.predict_prob", return_value=0.6)
@patch("calibration.meta_labeling_model.get_threshold", return_value=0.45)
def test_evaluate_passes_symbol_to_blend(_thr, _pred):
    g = _gate()
    # Inject mock state so _load_model returns non-None
    g._state = {"n_samples": 100, "roc_auc_oos": 0.7}
    g._state_loaded_ms = 1e18  # block reload
    decision, prob, reason = g.evaluate(
        {"p_edge": 0.55}, regime="momentum", symbol="BTCUSDT"
    )
    # blend with p_edge → blended prob should differ from raw 0.6
    assert decision in ("PASS", "SHADOW_VETO", "VETO")
    assert reason in (None, "META_LOW_PROB")


@patch("calibration.meta_labeling_model.predict_prob", return_value=0.6)
@patch("calibration.meta_labeling_model.get_threshold", return_value=0.45)
def test_evaluate_backward_compatible_without_symbol(_thr, _pred):
    g = _gate()
    g._state = {"n_samples": 100, "roc_auc_oos": 0.7}
    g._state_loaded_ms = 1e18
    decision, prob, reason = g.evaluate({"p_edge": 0.55}, regime="momentum")
    # Without symbol arg, blend short-circuits → meta_prob unchanged
    assert math.isclose(prob, 0.6, abs_tol=1e-6)


def test_should_veto_forwards_symbol():
    g = _gate(enabled=True)
    with patch.object(g, "evaluate", return_value=("VETO", 0.2, "META_LOW_PROB")) as ev:
        v, p, r = g.should_veto({}, regime="r", symbol="BTCUSDT")
    ev.assert_called_once_with({}, "r", symbol="BTCUSDT")
    assert v is True
