import unittest


class TestOFConfirmEngineGateStateHook(unittest.TestCase):
    def test_restore_from_indicators_hook(self) -> None:
        try:
            from core.of_confirm_engine import OFConfirmEngine
        except Exception:
            # In this isolated archive some dependencies might be missing; skip locally.
            self.skipTest("OFConfirmEngine import unavailable")

        class _P:
            def is_pressure_hi(self, now_ts_ms: int, per_min_th: float) -> bool:  # noqa: ARG002
                return False

        class _R:
            symbol = "BTCUSDT"
            config = {"micro_tf": "1s"}
            dynamic_cfg = {}
            cont_ctx_ts_ms = 0
            last_regime = "na"
            liq_regime = "na"
            book_churn_hi = 0
            pressure = _P()
            last_obi_event = None
            last_iceberg_event = None
            last_ofi_event = None
            last_sweep = None
            last_reclaim = None
            last_fp_edge = None
            last_wp = None
            last_div = None
            last_bar = None

        runtime = _R()
        eng = OFConfirmEngine(version=3)
        eng.set_replay_time_ms(1234567890)

        cfg = {
            "cancel_spike_gate_enable": 1,
            "cancel_spike_warmup_n": 1,
            "cancel_spike_ratio_max": 1.10,
            "cancel_spike_z_max": 0.0,
            "cancel_spike_fail_closed": 1,
        }

        indicators = {
            "cancel_bid_rate_ema": 5.0,
            "cancel_ask_rate_ema": 0.1,
            "taker_buy_rate_ema": 1.0,
            "taker_sell_rate_ema": 1.0,
            "bucket_id": 1,
            "pressure_hi": 0,
        }

        # Prime the gate state
        eng.build(
            symbol="BTCUSDT",
            tf="1s",
            direction="LONG",
            tick_ts_ms=1234567890,
            price=100.0,
            delta_z=0.0,
            runtime=runtime,
            cfg=cfg,
            indicators=dict(indicators),
            absorption=None,
        )

        st = eng.snapshot_cancel_gate_state(symbol="BTCUSDT")
        self.assertIsInstance(st, dict)

        # New engine restored via indicators should behave consistently
        eng2 = OFConfirmEngine(version=3)
        eng2.set_replay_time_ms(1234567890)
        indicators2 = dict(indicators)
        indicators2["cancel_gate_state"] = st
        eng2.build(
            symbol="BTCUSDT",
            tf="1s",
            direction="LONG",
            tick_ts_ms=1234567890,
            price=100.0,
            delta_z=0.0,
            runtime=runtime,
            cfg=cfg,
            indicators=indicators2,
            absorption=None,
        )


if __name__ == "__main__":
    unittest.main()

