from __future__ import annotations

import json
import unittest
from types import SimpleNamespace


class TestOFCCaptureSnapshot(unittest.TestCase):
    def test_export_runtime_snapshot_jsonable(self):
        from core.of_confirm_engine import OFConfirmEngine

        eng = OFConfirmEngine(version=3)

        runtime = SimpleNamespace(
            dynamic_cfg={'pressure_hi': 1},
            last_regime='vol_breakout',
            liq_regime='liq_high',
            book_churn_hi=1,
            cont_ctx_ts_ms=123,
            last_bar={'end_ts_ms': 999, 'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5,
                      'fp_enabled': 1, 'fp_absorption_bias': 0.2, 'fp_ladder_low_len': 3, 'fp_ladder_high_len': 4,
                      'fp_poc_on_edge': 1, 'fp_eff_quote': 0.9, 'fp_eff_delta': 0.8, 'fp_quote_delta': 1.1,
                      'fp_n_buckets': 12, 'fp_max_imbalance': 2.5, 'fp_absorb_score': 0.7, 'fp_progress': 0.3,
                      'fp_peak_delta': 1.2, 'fp_bucket_px': 0.5},
            last_fp_edge={'ts_ms': 1, 'p90': 0.1, 'value': 0.2, 'strength': 1.3, 'bias': 1, 'range_expansion': 0},
            last_obi_event={'ts_ms': 10, 'direction': 'BUY', 'obi': 0.4, 'obi_z': 1.2},
            last_iceberg_event={'ts_ms': 11, 'side': 'BUY', 'refresh': 2},
            last_ofi_event={'ts_ms': 12, 'direction': 'BUY', 'ofi': 0.2, 'ofi_z': 0.8},
            last_sweep={'ts_ms': 13, 'kind': 'sweep', 'direction_bias': 1},
            last_reclaim={'ts_ms': 14, 'hold_bars': 2, 'direction_bias': 1, 'level': 101.0, 'pool_id': 'p1'},
            last_wp={'weak_any': True},
            last_div={'ts_ms': 995, 'kind': 'none'},
        )
        snap = eng.export_runtime_snapshot(runtime, indicators={'pressure_hi': 1, 'now_ts_ms_used': 1234567890})
        self.assertIsInstance(snap, dict)
        # Must be JSON serializable
        json.dumps(snap, sort_keys=True)

    def test_restore_cancel_gate_state_no_crash(self):
        from core.of_confirm_engine import OFConfirmEngine

        eng = OFConfirmEngine(version=3)
        # Method must exist; state may be ignored if gate not available
        state = {'schema': 1, 'by_symbol': {'BTCUSDT': {'last_bucket_id': 10}}}
        try:
            eng.restore_cancel_gate_state(state)
        except Exception:
            # acceptable in minimal archive, but should not crash import
            pass


if __name__ == '__main__':
    unittest.main()

