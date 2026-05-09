from __future__ import annotations

import json
import os
import tempfile
import unittest


class DummyCtx:
    def __init__(self, symbol: str, side: str) -> None:
        self.symbol = symbol
        self.side = side


class TestScoreModelIsotonicIntegration(unittest.TestCase):
    def test_score_uses_isotonic(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "cal.json")
            obj = {
                "version": 1,
                "trained_at": 1730000000,
                "groups": {
                    "kind:absorption|symbol:BTCUSDT|side:LONG": {"type": "isotonic", "x": [0.0, 10.0], "p": [0.10, 0.90], "n": 1000},
                },
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(obj, f)

            os.environ["CONF_CAL_MODE"] = "isotonic"
            os.environ["CONF_CAL_PATH"] = path
            os.environ["CONF_CAL_MIN_SAMPLES"] = "300"
            os.environ["CONF_CAL_RELOAD_SEC"] = "0"
            os.environ["CONF_PCT_CAP"] = "99.0"

            from handlers.signal_scoring.score_model import ScoreModel  # type: ignore

            m = ScoreModel()
            out = m.score(
                raw_score=10.0,
                conf_factor01=1.0,
                kind="absorption",
                ctx=DummyCtx("BTCUSDT", "LONG"),
                parts_in={},
            )
            self.assertAlmostEqual(out.confidence_pct, 90.0, places=6)

    def test_score_fallback_sigmoid_when_missing(self) -> None:
        os.environ["CONF_CAL_MODE"] = "isotonic"
        os.environ["CONF_CAL_PATH"] = "/tmp/no_such_file.json"
        os.environ["CONF_CAL_RELOAD_SEC"] = "0"
        os.environ["CONF_CAL_K"] = "2.0"
        os.environ["CONF_CAL_B"] = "0.0"

        from handlers.signal_scoring.score_model import ScoreModel  # type: ignore

        m = ScoreModel()
        out = m.score(
            raw_score=1.0,
            conf_factor01=1.0,
            kind="absorption",
            ctx=DummyCtx("BTCUSDT", "LONG"),
            parts_in={},
        )
        self.assertTrue(out.confidence_pct > 80.0)
