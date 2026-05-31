
from core.of_confirm_engine import OFConfirmEngine


class MockRuntime:
    def __init__(self):
        self.config = {}
        self.dynamic_cfg = {}
        self.last_regime = "bull_trend"
        self.last_wp = type("WP", (), {"weak_any": True})()
        self.last_obi_event = None
        self.last_iceberg_event = None
        self.last_ofi_event = None
        self.last_sweep = None
        self.last_reclaim = None
        self.last_fp_edge = None
        self.last_bar = None
        self.cont_ctx_ts_ms = 1000 # For Cont Ctx
        self.last_div = type("Div", (), {"ts_ms": 1000})() # For Hidden Ctx (Div)
        self.pressure = type("Press", (), {"is_pressure_hi": lambda *a: False})()

def test_hard_pass_logic_perfect_signal(monkeypatch):
    """
    Verify that a 'perfect' signal results in ok=1.
    """

    # Force Helpers
    monkeypatch.setattr("core.of_confirm_engine.compute_obi_flags", lambda **k: (True, True, 10.0, 100.0))
    monkeypatch.setattr("core.of_confirm_engine.compute_ofi_flags", lambda **k: (True, True, 10.0, 50.0, 5.0, 1.0))
    monkeypatch.setattr("core.of_confirm_engine.compute_absorption_flags", lambda **k: (True, 1000.0))
    monkeypatch.setattr("core.of_confirm_engine.compute_reclaim_recent", lambda **k: (True, 10))
    monkeypatch.setattr("core.of_confirm_engine.compute_fp_edge_absorb", lambda **k: (True, 1.0, 1, "LONG"))

    engine = OFConfirmEngine()

    cfg = {
        "strong_need_continuation": 3,
        "cont_ctx_valid_ms": 5000,
        "hidden_ctx_valid_ms": 5000,
    }

    runtime = MockRuntime()
    # Mock Hidden Ctx: last_div exists and is fresh.
    runtime.last_div.ts_ms = 1000

    # Mock Cont Ctx: cont_ctx_ts_ms exists and is fresh.
    runtime.cont_ctx_ts_ms = 1000

    indicators = {
        "spread_bps": 1.0,
        "expected_slippage_bps": 1.0,
    }

    # Direction LONG, Trend LONG (bull_trend) -> A=1 (Hidden Ctx + Dir match)

    ofc, dec = engine.build(
        symbol="TEST",
        tf="1s",
        direction="LONG",
        tick_ts_ms=2000, # 1000ms after ctx, valid
        price=100.0,
        delta_z=3.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators,
        absorption={"ok": 1}
    )

    # Expected Logic Legs:
    # A (Hidden): Div exists + fresh. Dir=LONG, Regime=BullTrend (implies trend_dir=LONG?).
    #   Wait, runtime.last_regime="bull_trend".
    #   of_confirm_engine L338: `trend_dir = "LONG" if "bull" in regime else ...`
    #   So Trend=LONG. OK.
    # B (Micro): OBI Stable (via patch). OK.
    # C (Cont Ctx): runtime.cont_ctx_ts_ms fresh. OK.

    # Total Have = 3.

    print(f"DEBUG: scenario={ofc.scenario} ok={ofc.ok} have={ofc.have} need={ofc.need} score={ofc.score} legs={ofc.evidence.get('legs')}")

    assert ofc.scenario == "continuation"
    assert ofc.have >= 3
    assert ofc.ok == 1
