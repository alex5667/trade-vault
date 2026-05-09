import unittest

from domain.handlers import create_position
from domain.models import SignalNorm


class FakeSpec:
    def risk_money(self, entry, sl, lot, side, symbol):
        # deterministic stub: simple value to verify call
        return abs(entry - sl) * lot * 10.0

class TestRiskUsdSpecMoney(unittest.TestCase):
    def test_create_position_sets_risk_usd_via_spec(self):
        sig = SignalNorm(
            sid="sid1",
            symbol="BTCUSDT",
            direction="LONG",
            entry_price=100.0,
            sl=99.0,         # needed if SignalNorm validation requires it
            lot=2.0,
            entry_ts_ms=1000,
            strategy="test",
            payload={"sl": 99.0, "lot": 2.0},
            source="test",
            tf="1m",
            tp_levels=[101.0, 102.0],
        )
        spec = FakeSpec()
        pos = create_position(sig, spec)

        # entry=100, sl=99, lot=2. diff=1. stub multiplier=10. expected=20.
        expected = 20.0
        val = getattr(pos, "risk_usd", 0.0)
        self.assertAlmostEqual(val, expected, places=5)
        # Also check persistence
        sp = getattr(pos, "signal_payload", {})
        self.assertAlmostEqual(sp.get("risk_usd"), expected, places=5)

if __name__ == "__main__":
    unittest.main()
