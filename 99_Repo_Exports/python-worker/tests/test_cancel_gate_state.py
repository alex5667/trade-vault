import unittest

# Ensure repo root is on sys.path (namespace packages)
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.cancellation_spike_gate import CancellationSpikeGate


class TestCancellationSpikeGateState(unittest.TestCase):
    def test_snapshot_restore_roundtrip(self):
        gate = CancellationSpikeGate()
        sym = "BTCUSDT"
        cfg = {
            "cancel_gate_enable": 1,
            "cancel_gate_alpha": 0.2,
            "cancel_gate_ratio_max": 2.0,
            "cancel_gate_z_min": 1.0,
            "cancel_gate_min_samples": 5,
        }

        # Warmup with stable ratios
        for i in range(10):
            gd = gate.check(
                symbol=sym,
                direction="LONG",
                cancel_bid_rate_ema=1.0,
                cancel_ask_rate_ema=1.0,
                taker_buy_rate_ema=1.0,
                taker_sell_rate_ema=1.0,
                bucket_id=i,
                cfg2=cfg,
            )
            self.assertTrue(gd.allow)

        snap = gate.snapshot_state(sym)
        gate2 = CancellationSpikeGate()
        # New API: restore_state takes snapshot dict directly
        if isinstance(snap, dict) and "symbols" in snap:
            gate2.restore_state(snap)
        else:
            # Legacy format support
            gate2.restore_state({"symbols": {sym: snap}} if "symbol" in snap else snap)

        # Same next input must yield same decision/meta (deterministic)
        gd1 = gate.check(
            symbol=sym,
            direction="LONG",
            cancel_bid_rate_ema=5.0,
            cancel_ask_rate_ema=1.0,
            taker_buy_rate_ema=1.0,
            taker_sell_rate_ema=1.0,
            bucket_id=999,
            cfg2=cfg,
        )
        gd2 = gate2.check(
            symbol=sym,
            direction="LONG",
            cancel_bid_rate_ema=5.0,
            cancel_ask_rate_ema=1.0,
            taker_buy_rate_ema=1.0,
            taker_sell_rate_ema=1.0,
            bucket_id=999,
            cfg2=cfg,
        )
        self.assertEqual(gd1.allow, gd2.allow)
        self.assertEqual(gd1.reason, gd2.reason)
        self.assertAlmostEqual(float(gd1.meta.get("ratio_support") or 0.0), float(gd2.meta.get("ratio_support") or 0.0), places=12)


if __name__ == "__main__":
    unittest.main()

