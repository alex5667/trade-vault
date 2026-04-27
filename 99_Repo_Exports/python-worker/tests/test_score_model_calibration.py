from __future__ import annotations

import json
import os
import tempfile
from types import SimpleNamespace

from handlers.signal_scoring.score_model import ScoreModel


def test_score_model_uses_isotonic_when_available(monkeypatch):
    with tempfile.TemporaryDirectory() as d:
        cal_path = os.path.join(d, "cal.json")
        obj = {
            "version": 1,
            "trained_at": 123,
            "groups": {
                "kind:k1|symbol:BTC|side:LONG": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.2, 0.9], "n": 999}
            },
        }
        with open(cal_path, "w", encoding="utf-8") as f:
            json.dump(obj, f)

        monkeypatch.setenv("CONF_CAL_MODE", "isotonic")
        monkeypatch.setenv("CONF_CAL_PATH", cal_path)
        monkeypatch.setenv("CONF_CAL_MIN_SAMPLES", "10")
        monkeypatch.setenv("CONF_CAL_RELOAD_SEC", "0")

        m = ScoreModel()
        ctx = SimpleNamespace(symbol="BTC", side="LONG")
        out = m.score(raw_score=1.0, conf_factor01=1.0, kind="k1", ctx=ctx, parts_in={})
        assert out.confidence_pct > 50.0
        assert out.parts.get("confidence_calibration_isotonic", 0.0) == 1.0
        assert out.parts.get("confidence_p_win", 0.0) == out.confidence_pct / 100.0
