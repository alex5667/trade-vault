import types

import pytest


def test_norm_side_no_pass_and_bool_guard():
    from handlers.signal_scoring.score_model import _norm_side
    assert _norm_side(None) == "*"
    assert _norm_side(True) == "*"      # bool must not be treated as int
    assert _norm_side(False) == "*"
    assert _norm_side(1) == "LONG"
    assert _norm_side(-1) == "SHORT"
    assert _norm_side("buy") == "LONG"
    assert _norm_side("SELL") == "SHORT"
    assert _norm_side("weird") == "*"


def test_score_parts_numeric_only_and_meta_has_calib_key(monkeypatch):
    from handlers.signal_scoring import score_model as m

    # Force sigmoid mode (no file IO)
    monkeypatch.setenv("CONF_CAL_MODE", "sigmoid")
    sm = m.ScoreModel()
    ctx = types.SimpleNamespace(symbol="BTCUSDT", side="LONG")

    out = sm.score(raw_score=2.0, conf_factor01=0.5, kind="breakout", ctx=ctx, parts_in={"x": 1.0})

    assert out.final_score == pytest.approx(1.0)
    assert 0.0 <= out.confidence_pct <= 99.0
    # parts numeric only
    assert isinstance(out.parts, dict)
    assert all(isinstance(v, (int, float)) for v in out.parts.values())
    # meta exists and contains strings only
    assert isinstance(out.meta, dict)
    assert all(isinstance(v, str) for v in out.meta.values())


def test_isotonic_failure_falls_back_to_sigmoid_without_raise(monkeypatch):
    from handlers.signal_scoring import score_model as m

    class BadStore:
        def __init__(self, *a, **kw): ...
        def maybe_reload(self): raise RuntimeError("boom")
        def get_group(self, *a, **kw): return None, ""

    monkeypatch.setenv("CONF_CAL_MODE", "isotonic")
    monkeypatch.setenv("CONF_CAL_PATH", "/tmp/does-not-matter.json")
    # patch store to avoid filesystem and force exception
    m.CalibStore = BadStore

    sm = m.ScoreModel()
    ctx = types.SimpleNamespace(symbol="ETHUSDT", side="SHORT")
    out = sm.score(raw_score=1.0, conf_factor01=1.0, kind="x", ctx=ctx, parts_in={})
    assert 0.0 <= out.confidence_pct <= 99.0
