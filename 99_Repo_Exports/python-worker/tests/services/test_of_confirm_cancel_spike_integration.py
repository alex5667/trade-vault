import types

import pytest


def test_build_vetoed_by_cancel_spike(monkeypatch):
    import core.of_confirm_engine as m

    # ---- Patch external dependencies to deterministic stubs ----
    monkeypatch.setattr(m, "compute_obi_flags", lambda **kw: (True, True, 10.0, 0.5))
    monkeypatch.setattr(m, "compute_iceberg_flags", lambda **kw: (True, True, 1, 1.0))
    monkeypatch.setattr(m, "compute_sweep_recent", lambda **kw: True)               # reversal
    monkeypatch.setattr(m, "compute_reclaim_recent", lambda **kw: (True, 1))
    monkeypatch.setattr(m, "compute_absorption_flags", lambda **kw: (True, 1.0))
    monkeypatch.setattr(m, "compute_strong_need_same_tick",
                        lambda **kw: types.SimpleNamespace(need_rev=False, need_cont=False, reason="na"))
    monkeypatch.setattr(m, "merged_cfg", lambda a, b: dict(a, **(b or {})))
    monkeypatch.setattr(m, "eval_reversal",
                        lambda **kw: types.SimpleNamespace(ok=True, have=2, need=2, gate_bits=0, reason="ok", scenario="reversal"))

    # runtime stub
    class Pressure:
        def is_pressure_hi(self, *_a, **_k):
            return False

    runtime = types.SimpleNamespace(
        last_obi_event=None,
        last_iceberg_event=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=None,
        last_div=None,
        last_regime="range",
        dynamic_cfg={},
        pressure=Pressure(),
        book_churn_hi=0,
        last_bar=None,
    )

    eng = m.OFConfirmEngine()  # assumes default init exists

    # Warmup calls: baseline cancel ~10
    indicators = {
        "now_ts_ms": 1,
        "cancel_bid_rate_ema": 10.0,
        "cancel_ask_rate_ema": 10.0,
        "taker_buy_rate_ema": 100.0,
        "taker_sell_rate_ema": 100.0,
        "bar_id": 1,
    }
    cfg = {
        "of_score_min": 0.0,               # avoid score veto in this test
        "cancel_spike_enable": 1,
        "cancel_spike_mode": "veto",
        "cancel_spike_ratio_th": 2.0,
        "cancel_spike_min_samples": 2,     # quick warmup
        "cancel_spike_min_baseline": 0.1,
        "cancel_spike_use_robust_z": 0,
    }

    for b in (1, 2, 3):
        indicators["bar_id"] = b
        ofc, _ = eng.build(symbol="BTCUSDT", tf="1m", direction="LONG", tick_ts_ms=1, price=1.0,
                           delta_z=3.0, runtime=runtime, cfg=cfg, indicators=dict(indicators))
        assert ofc is not None

    # Spike on bid cancellations => should veto ok==1
    indicators["bar_id"] = 4
    indicators["cancel_bid_rate_ema"] = 30.0
    ofc, _ = eng.build(symbol="BTCUSDT", tf="1m", direction="LONG", tick_ts_ms=1, price=1.0,
                       delta_z=3.0, runtime=runtime, cfg=cfg, indicators=dict(indicators))
    assert ofc is not None
    assert int(ofc.ok) == 0
    assert "cancel_spike_" in str(ofc.reason)
