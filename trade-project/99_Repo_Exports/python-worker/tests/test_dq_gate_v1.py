import unittest

# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.dq_gate_v1 import eval_dq_gate


class TestDQGateV1(unittest.TestCase):

    def test_off_by_default(self) -> None:
        out = eval_dq_gate(indicators={"data_health": 0.1}, cfg2={})
        self.assertEqual(int(out.get("dq_veto", -1)), 0)
        self.assertEqual(float(out.get("dq_pen", -1.0)), 0.0)
        self.assertEqual(out.get("dq_reason"), "disabled")

    def test_penalty_mode_degrades_score(self) -> None:
        cfg2 = {
            "dq_gate_enable": 1,
            "dq_gate_mode": "penalty",
            "dq_pen_max": 0.20,
            "dq_data_health_min": 0.90
        }
        # data_health = 0.5 -> health_score = 0.5
        # dq_pen = (1.0 - 0.5) * 0.20 = 0.10
        ind = {"data_health": 0.50}
        out = eval_dq_gate(ind, cfg2)
        self.assertAlmostEqual(float(out.get("dq_pen", 0.0)), 0.10)
        self.assertEqual(int(out.get("dq_veto", 1)), 0)
        self.assertEqual(out.get("dq_reason"), "low_data_health_hard")

    def test_enforce_veto_triggers(self) -> None:
        cfg2 = {
            "dq_gate_enable": 1,
            "dq_gate_mode": "enforce",
            "dq_data_health_hard_min": 0.60,
            "dq_data_health_min": 0.90
        }
        # data_health = 0.5 -> health_score = 0.5 < 0.60 -> veto
        ind = {"data_health": 0.50}
        out = eval_dq_gate(ind, cfg2)
        self.assertEqual(int(out.get("dq_veto", 0)), 1)
        self.assertEqual(out.get("dq_reason"), "low_data_health_hard")

    def test_latency_spike_veto(self) -> None:
        cfg2 = {
            "dq_gate_enable": 1,
            "dq_gate_mode": "both",
            "dq_tick_age_ms_max": 1000,
            "dq_data_health_hard_min": 0.50
        }
        # tick_time_age_ms = 5000 -> health_score *= 0.1 -> 1.0 * 0.1 = 0.1 < 0.5 -> veto
        ind = {"tick_time_age_ms": 5000}
        out = eval_dq_gate(ind, cfg2)
        self.assertEqual(int(out.get("dq_veto", 0)), 1)
        self.assertEqual(out.get("dq_reason"), "latency_spike")

    def test_skew_penalties(self) -> None:
        cfg2 = {
            "dq_gate_enable": 1,
            "dq_gate_mode": "penalty",
            "dq_pen_max": 0.10,
            "dq_skew_ema_ms_max": 500
        }
        # skew_now = 1000 -> health_score = 0.7
        # dq_pen = (1.0 - 0.7) * 0.10 = 0.03
        ind = {"tick_ts_source_now_ema": 1000}
        out = eval_dq_gate(ind, cfg2)
        self.assertAlmostEqual(float(out.get("dq_pen", 0.0)), 0.03)
        self.assertEqual(out.get("dq_reason"), "clock_skew_now")

    def test_fail_open_on_nan(self) -> None:
        cfg2 = {"dq_gate_enable": 1}
        ind = {"data_health": float('nan')}
        out = eval_dq_gate(ind, cfg2)
        self.assertEqual(float(out.get("dq_pen", -1.0)), 0.0)
        self.assertEqual(out.get("dq_reason"), "ok")

    def test_book_stale_penalty(self) -> None:
        cfg2 = {"dq_gate_enable": 1, "dq_pen_max": 0.10}
        # book_health_ok = 0 -> health_score = 0.5
        # dq_pen = (1.0 - 0.5) * 0.10 = 0.05
        ind = {"book_health_ok": 0}
        out = eval_dq_gate(ind, cfg2)
        self.assertAlmostEqual(float(out.get("dq_pen", 0.0)), 0.05)
        self.assertEqual(out.get("dq_reason"), "book_stale")

if __name__ == "__main__":
    unittest.main()
