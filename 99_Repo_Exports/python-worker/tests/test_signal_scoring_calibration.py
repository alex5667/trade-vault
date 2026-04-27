import json
import os
import tempfile
import unittest

from handlers.signal_scoring.isotonic import fit_isotonic_pav
from handlers.signal_scoring.calibration_store import CalibStore
from handlers.signal_scoring.score_model import ScoreModel


class DummyCtx:
    def __init__(self, symbol: str, side: str):
        self.symbol = symbol
        self.side = side


class TestIsotonicPAV(unittest.TestCase):
    def test_pav_monotonic(self):
        samples = [
            (0.1, 0, 1.0),
            (0.2, 1, 1.0),
            (0.3, 0, 1.0),
            (0.4, 1, 1.0),
            (0.5, 1, 1.0),
        ]
        cal = fit_isotonic_pav(samples)
        self.assertTrue(len(cal.x) == len(cal.p))
        for i in range(1, len(cal.p)):
            self.assertLessEqual(cal.p[i - 1], cal.p[i])

    def test_predict_bounds(self):
        cal = fit_isotonic_pav([(0.1, 0, 1.0), (1.0, 1, 1.0)])
        self.assertGreaterEqual(cal.predict(-1.0), 0.0)
        self.assertLessEqual(cal.predict(999.0), 1.0)


class TestCalibStore(unittest.TestCase):
    def test_store_side_keys_and_legacy_fallback(self):
        obj = {
            "version": 2,
            "trained_at": 1730000000,
            "groups": {
                # новый ключ с side
                "kind:absorption|symbol:BTCUSDT|side:LONG": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.4, 0.7], "n": 500},
                # legacy ключ без side (на случай старых файлов)
                "kind:absorption|symbol:ETHUSDT": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.45, 0.65], "n": 500},
                "global": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.48, 0.52], "n": 999999},
            },
        }
        with tempfile.NamedTemporaryFile("w+", delete=False) as f:
            json.dump(obj, f)
            f.flush()
            path = f.name

        try:
            st = CalibStore(path, min_samples=300, reload_sec=0.0)
            st.load()
            gg = st.get_group(kind="absorption", symbol="BTCUSDT", side="LONG")
            self.assertIsNotNone(gg)
            cal, n = gg
            self.assertEqual(n, 500)
            self.assertAlmostEqual(cal.predict(0.0), 0.4, places=6)

            # legacy fallback: side не задан, ключ без side должен отработать
            gg2 = st.get_group(kind="absorption", symbol="ETHUSDT", side="SHORT")
            self.assertIsNotNone(gg2)
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass


class TestScoreModel(unittest.TestCase):
    def test_score_uses_isotonic_when_available(self):
        obj = {
            "version": 2,
            "trained_at": 1730000000,
            "groups": {
                "kind:absorption|symbol:BTCUSDT|side:LONG": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.1, 0.9], "n": 999},
            },
        }
        with tempfile.NamedTemporaryFile("w+", delete=False) as f:
            json.dump(obj, f)
            f.flush()
            path = f.name

        try:
            os.environ["CONF_CAL_MODE"] = "isotonic"
            os.environ["CONF_CAL_PATH"] = path
            os.environ["CONF_CAL_MIN_SAMPLES"] = "300"
            os.environ["CONF_CAL_RELOAD_SEC"] = "0"

            m = ScoreModel()
            ctx = DummyCtx("BTCUSDT", "LONG")

            out_pos = m.score(raw_score=1.0, conf_factor01=1.0, kind="absorption", ctx=ctx, parts_in={})
            out_neg = m.score(raw_score=-1.0, conf_factor01=1.0, kind="absorption", ctx=ctx, parts_in={})
            # confidence по |final| должна совпадать при смене знака
            self.assertAlmostEqual(out_pos.confidence_pct, out_neg.confidence_pct, places=8)
            # isotonic p(1.0)=0.9 => pct ~= 90 (cap может ограничить, но по умолчанию 99)
            self.assertGreater(out_pos.confidence_pct, 80.0)
            # key names as documented in score_model.py
            self.assertEqual(out_pos.parts.get("confidence_calibration_isotonic", 0.0), 1.0)
            self.assertGreater(out_pos.parts.get("confidence_calib_n", 0.0), 0.0)
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass

    def test_score_fallbacks_to_sigmoid_if_no_group(self):
        obj = {"version": 2, "trained_at": 1730000000, "groups": {"global": {"type": "isotonic", "x": [0.0, 1.0], "p": [0.5, 0.5], "n": 999}}}
        with tempfile.NamedTemporaryFile("w+", delete=False) as f:
            json.dump(obj, f)
            f.flush()
            path = f.name

        try:
            os.environ["CONF_CAL_MODE"] = "isotonic"
            os.environ["CONF_CAL_PATH"] = path
            os.environ["CONF_CAL_MIN_SAMPLES"] = "300"
            os.environ["CONF_CAL_RELOAD_SEC"] = "0"

            m = ScoreModel()
            ctx = DummyCtx("NOPE", "SHORT")
            out = m.score(raw_score=0.01, conf_factor01=1.0, kind="absorption", ctx=ctx, parts_in={})
            self.assertGreaterEqual(out.confidence_pct, 0.0)
            self.assertLessEqual(out.confidence_pct, 99.0)
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass
