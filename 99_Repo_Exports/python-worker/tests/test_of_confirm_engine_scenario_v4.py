import types

import core.of_confirm_engine as ofe


class _PressureStub:
    def is_pressure_hi(self, *_args, **_kwargs):
        return True


def test_range_meanrev_replaces_none(monkeypatch):
    # Force cancellation gate to be neutral
    class _CancelGateStub:
        def check(self, **_kwargs):
            return types.SimpleNamespace(allow=True, reason="ok", meta={"ready": 0, "veto_kind": "none"})

    monkeypatch.setattr(ofe, "CancellationSpikeGate", lambda: _CancelGateStub())
    monkeypatch.setattr(ofe, "veto_total", lambda *a, **k: None)
    monkeypatch.setattr(ofe, "dist", lambda *a, **k: None)

    # No sweep, no trend => base scenario is none; v4 should route to range_meanrev when enabled
    runtime = types.SimpleNamespace(
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event=None,
        last_fp_edge=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_bar=None,
        dynamic_cfg={"scenario_v4_enable": 1},
        pressure=_PressureStub(),
        book_churn_hi=0,
        last_regime="na",
        last_div=None,
        # v4 classifier now requires liq_regime and liq_score
        liq_regime="normal",
        liq_score=0.5,
    )
    indicators = {"book_health_ok": 1, "data_health": 1.0, "spread_bps": 2.0, "expected_slippage_bps": 1.0}

    eng = ofe.OFConfirmEngine()
    ofc, dec = eng.build(
        symbol="TEST",
        tf="1s",
        direction="LONG",
        tick_ts_ms=2_000,
        price=1.0,
        delta_z=2.0,
        runtime=runtime,
        cfg={"scenario_v4_enable": 1},
        indicators=indicators,
        absorption={"side": "LONG", "volume": 10.0},
    )
    assert ofc is not None
    # With scenario_v4_enable=1 and no sweep/no trend, classify_v4 produces "range_meanrev"
    sv4 = ofc.evidence.get("scenario_v4", "")
    assert sv4 == "range_meanrev", f"Expected 'range_meanrev' but got '{sv4}'"
