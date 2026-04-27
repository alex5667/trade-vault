import json
import os
import tempfile
import unittest

from common.calibration_store import CalibStore
from handlers.signal_scoring.score_model import ScoreModel


class DummyCtx:
    def __init__(self, symbol: str, side: str):
        self.symbol = symbol
        self.side = side


class TestCalibStoreSide(unittest.TestCase):
    def test_prefers_side_specific_group(self):
        obj = {
            "version": 2,
            "trained_at": 1730000000,
            "groups": {
                "global": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.5, 0.5], "n": 999999},
                "kind:absorption|symbol:BTCUSDT|side:LONG": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.1, 0.9], "n": 1000},
                "kind:absorption|symbol:BTCUSDT|side:SHORT": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.2, 0.8], "n": 1000},
            },
        }
        with tempfile.NamedTemporaryFile("w+", delete=False) as f:
            json.dump(obj, f)
            f.flush()
            path = f.name
        try:
            st = CalibStore(path, min_samples=300, reload_sec=0.0)
            g = st.get_group(kind="absorption", symbol="BTCUSDT", side="LONG")
            self.assertIsNotNone(g)
            self.assertAlmostEqual(g.calibrator.predict(1.0), 0.9, places=8)
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass

    def test_legacy_fallback_without_side(self):
        obj = {
            "version": 1,
            "trained_at": 1730000000,
            "groups": {
                "kind:absorption|symbol:ETHUSDT": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.3, 0.7], "n": 1000},
            },
        }
        with tempfile.NamedTemporaryFile("w+", delete=False) as f:
            json.dump(obj, f)
            f.flush()
            path = f.name
        try:
            st = CalibStore(path, min_samples=300, reload_sec=0.0)
            g = st.get_group(kind="absorption", symbol="ETHUSDT", side="SHORT")
            self.assertIsNotNone(g)
            self.assertAlmostEqual(g.calibrator.predict(1.0), 0.7, places=8)
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass


class TestScoreModelUsesSide(unittest.TestCase):
    def test_score_is_sign_invariant_and_uses_isotonic(self):
        obj = {
            "version": 2,
            "trained_at": 1730000000,
            "groups": {
                "kind:absorption|symbol:BTCUSDT|side:LONG": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.05, 0.95], "n": 999},
            },
        }
        with tempfile.NamedTemporaryFile("w+", delete=False) as f:
            json.dump(obj, f)
            f.flush()
            path = f.name

        # сохраняем env, чтобы не ломать другие тесты
        old = dict(os.environ)
        try:
            os.environ["CONF_CAL_MODE"] = "isotonic"
            os.environ["CONF_CAL_PATH"] = path
            os.environ["CONF_CAL_MIN_SAMPLES"] = "300"
            os.environ["CONF_CAL_RELOAD_SEC"] = "0"

            m = ScoreModel()
            ctx = DummyCtx("BTCUSDT", "LONG")

            out_pos = m.score(raw_score=1.0, conf_factor01=1.0, kind="absorption", ctx=ctx, parts_in={})
            out_neg = m.score(raw_score=-1.0, conf_factor01=1.0, kind="absorption", ctx=ctx, parts_in={})

            # confidence зависит от |final| => знак не должен влиять
            self.assertAlmostEqual(out_pos.confidence_pct, out_neg.confidence_pct, places=10)

            # isotonic применился
            self.assertEqual(out_pos.parts.get("confidence_calibration_isotonic", 0.0), 1.0)
        finally:
            os.environ.clear()
            os.environ.update(old)
            try:
                os.unlink(path)
            except Exception:
                pass
