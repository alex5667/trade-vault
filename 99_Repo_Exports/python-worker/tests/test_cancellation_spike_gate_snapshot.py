import unittest


class TestCancellationSpikeGateSnapshot(unittest.TestCase):
    def test_snapshot_restore_roundtrip(self) -> None:
        from services.cancellation_spike_gate import CancellationSpikeGate

        cfg2 = {
            "cancel_spike_warmup_n": 3,
            "cancel_spike_ratio_max": 1.20,
            "cancel_spike_z_max": 0.25,
            "cancel_spike_fail_closed": 1,
        }

        g1 = CancellationSpikeGate()

        # warmup with monotonic bucket_id
        for b in range(1, 6):
            g1.check(
                symbol="BTCUSDT",
                direction="LONG",
                cancel_bid_rate_ema=2.0 + 0.10 * b,
                cancel_ask_rate_ema=0.5,
                taker_buy_rate_ema=1.0,
                taker_sell_rate_ema=1.0,
                bucket_id=b,
                cfg2=cfg2,
            )

        snap = g1.snapshot_state()
        self.assertIsInstance(snap, dict)
        self.assertIn("symbols", snap)

        g2 = CancellationSpikeGate()
        g2.restore_state(snap)

        # Same input on same bucket_id should produce identical decision
        d1 = g1.check(
            symbol="BTCUSDT",
            direction="LONG",
            cancel_bid_rate_ema=3.0,
            cancel_ask_rate_ema=0.5,
            taker_buy_rate_ema=1.0,
            taker_sell_rate_ema=1.0,
            bucket_id=10,
            cfg2=cfg2,
        )
        d2 = g2.check(
            symbol="BTCUSDT",
            direction="LONG",
            cancel_bid_rate_ema=3.0,
            cancel_ask_rate_ema=0.5,
            taker_buy_rate_ema=1.0,
            taker_sell_rate_ema=1.0,
            bucket_id=10,
            cfg2=cfg2,
        )

        self.assertEqual(bool(d1.allow), bool(d2.allow))
        self.assertEqual(str(d1.reason), str(d2.reason))


if __name__ == "__main__":
    unittest.main()

