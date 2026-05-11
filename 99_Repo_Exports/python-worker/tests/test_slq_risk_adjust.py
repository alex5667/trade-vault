import unittest

from services.slq_risk_adjust import maybe_apply_slq_to_risk_cfg


class DummyRedis:
    def __init__(self, payload: str): self.payload = payload
    def get(self, key): return self.payload

class Ctx:
    tp1_hit_prob: float = 0.0
    regime: str = "na"
    atr_bps: float = 0.0

class TestSlqRiskAdjust(unittest.TestCase):
    def test_slq_applies_atr_mult(self):
        # Setup Env (requires monkeypatch or dict patch, but unittest doesn't have monkeypatch fixture like pytest)
        # We will assume Env is defaulted or we patch os.environ.
        import os
        from unittest.mock import patch

        with patch.dict(os.environ, {
            "SLQ_ENABLE": "1",
            "SLQ_MIN_N": "200",
            "SLQ_TP1_PROB_MIN": "0.55",
            "SLQ_POSTSL_TP1_MIN": "0.25",
            "SLQ_K": "1.0",
            "SLQ_BUMP_ATR_CAP": "0.4",
            "SLQ_STOP_ATR_MIN": "0.5",
            "SLQ_STOP_ATR_MAX": "1.5"
        }):
            ctx = Ctx()
            ctx.tp1_hit_prob = 0.8
            ctx.regime = "na"

            r = DummyRedis('{"n":500,"sl_buffer_atr_q90":0.3,"post_sl_tp1_hit_rate":0.4,"ts_ms":9999999999999}')
            cfg = {"stop_mode":"atr","stop_atr_mult":0.8}

            out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
            self.assertEqual(out["slq_used"], 1)
            self.assertAlmostEqual(out["stop_atr_mult"], 1.1)
            self.assertEqual(out["slq_bump_atr"], 0.3)
            # Verify TP1 was scaled proportionally: 0.78 * (1.1 / 0.8) = 1.0725
            self.assertAlmostEqual(out["ROCKET_TP1_ATR_MULT"], 1.0725, places=4)

    def test_slq_disabled(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {"SLQ_ENABLE": "0"}):
             ctx = Ctx()
             r = DummyRedis('{"n":500}')
             cfg = {"stop_mode":"atr","stop_atr_mult":0.8}
             out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
             self.assertNotIn("slq_used", out)
             self.assertEqual(out["stop_atr_mult"], 0.8)

    def test_gate_tp1_prob(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {"SLQ_ENABLE": "1", "SLQ_TP1_PROB_MIN": "0.6"}):
             ctx = Ctx()
             ctx.tp1_hit_prob = 0.5 # Too low
             r = DummyRedis('{"n":500,"sl_buffer_atr_q90":0.3,"post_sl_tp1_hit_rate":0.4}')
             cfg = {"stop_mode":"atr","stop_atr_mult":0.8}
             out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
             self.assertNotIn("slq_used", out)

    def test_gate_postsl_tp1_hit_rate(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {"SLQ_ENABLE": "1", "SLQ_POSTSL_TP1_MIN": "0.5"}):
             ctx = Ctx()
             ctx.tp1_hit_prob = 0.9
             r = DummyRedis('{"n":500,"sl_buffer_atr_q90":0.3,"post_sl_tp1_hit_rate":0.2}') # Too low post-sl success
             cfg = {"stop_mode":"atr","stop_atr_mult":0.8}
             out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
             self.assertNotIn("slq_used", out)

    def test_slq_shadow_does_not_mutate_execution_cfg(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {
            "SLQ_ENABLE": "1",
            "SLQ_SHADOW_ONLY": "1",
            "SLQ_MIN_N": "200",
            "SLQ_TP1_PROB_MIN": "0.55",
            "SLQ_POSTSL_TP1_MIN": "0.25",
        }):
            ctx = Ctx()
            ctx.tp1_hit_prob = 0.9
            ctx.regime = "na"
            ctx.atr_bps = 100.0
            r = DummyRedis('{"n":500,"sl_buffer_atr_q90":0.5,"post_sl_tp1_hit_rate":0.8,"ts_ms":9999999999999}')
            cfg = {"stop_mode": "atr", "stop_atr_mult": 1.0}
            out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)

            self.assertEqual(out["stop_atr_mult"], 1.0)
            self.assertNotIn("ROCKET_TP1_ATR_MULT", out)
            self.assertEqual(out["slq_decision"], "shadow_computed")
            self.assertGreaterEqual(out["slq_shadow_final_mult"], 1.0)

    def test_slq_reject_too_wide_sets_sizing_false(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {
            "SLQ_ENABLE": "1",
            "SLQ_MIN_N": "200",
            "SLQ_MAX_STOP_BPS": "50.0",
        }):
            ctx = Ctx()
            ctx.tp1_hit_prob = 0.9
            ctx.regime = "na"
            ctx.atr_bps = 200.0
            r = DummyRedis('{"n":500,"sl_buffer_atr_q90":0.5,"post_sl_tp1_hit_rate":0.8,"ts_ms":9999999999999}')
            cfg = {"stop_mode": "atr", "stop_atr_mult": 1.0}
            out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)

            self.assertEqual(out.get("slq_decision"), "reject_too_wide")
            self.assertFalse(out.get("sizing_ok"))

    def test_slq_reject_ev_negative_sets_sizing_false(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {
            "SLQ_ENABLE": "1",
            "SLQ_MIN_N": "200",
            "SLQ_MIN_EV_AFTER_BPS": "1000.0",
        }):
            ctx = Ctx()
            ctx.tp1_hit_prob = 0.9
            ctx.regime = "na"
            ctx.atr_bps = 50.0
            r = DummyRedis('{"n":500,"sl_buffer_atr_q90":0.2,"post_sl_tp1_hit_rate":0.8,"ts_ms":9999999999999}')
            cfg = {"stop_mode": "atr", "stop_atr_mult": 1.0}
            out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)

            self.assertEqual(out.get("slq_decision"), "reject_ev_negative")
            self.assertFalse(out.get("sizing_ok"))

    def test_slq_never_tightens_below_base(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {
            "SLQ_ENABLE": "1",
            "SLQ_MIN_N": "200",
            "SLQ_K": "0.5",
        }):
            ctx = Ctx()
            ctx.tp1_hit_prob = 0.9
            ctx.regime = "na"
            ctx.atr_bps = 50.0
            r = DummyRedis('{"n":500,"sl_buffer_atr_q90":0.0,"post_sl_tp1_hit_rate":0.8,"ts_ms":9999999999999}')
            cfg = {"stop_mode": "atr", "stop_atr_mult": 1.5}
            out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)

            self.assertEqual(out.get("slq_used"), 1)
            self.assertGreaterEqual(out["stop_atr_mult"], 1.5)


if __name__ == "__main__":
    unittest.main()
