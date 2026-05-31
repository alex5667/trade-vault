import importlib
from types import SimpleNamespace

import pytest


def _imp_any(paths):
    last_err = None
    for p in paths:
        try:
            return importlib.import_module(p)
        except Exception as e:
            last_err = e
    raise last_err  # type: ignore[misc]


def _imp_components():
    return importlib.import_module("handlers.crypto_orderflow.core.crypto_orderflow_components")


def _imp_tick():
    m = _imp_any(["handlers.base_orderflow_handler", "signals.handlers.base_orderflow_handler"])
    return m.Tick


def test_tick_parser_normalizes_seconds_to_ms():
    c = _imp_components()
    Tick = _imp_tick()
    p = c.TickParser()

    # seconds ts -> ms
    fields = {"ts": "1700000000", "bid": "100", "ask": "100.1", "last": "100.05", "volume": "1", "flags": "1"}
    t = p.parse(fields)
    assert isinstance(t, Tick)
    assert t.ts == 1700000000 * 1000
    assert p.stats.total == 1
    assert p.stats.bad == 0


def test_tick_parser_extracts_is_buyer_maker_from_aliases():
    c = _imp_components()
    p = c.TickParser()

    # flat alias "m"
    fields = {"ts": 1700000000000, "bid": 100, "ask": 100.1, "last": 100.05, "volume": 1, "flags": 1, "m": "true"}
    t = p.parse(fields)
    assert t is not None
    assert t.is_buyer_maker is True

    # json-in-data alias isBuyerMaker
    fields2 = {"data": '{"ts":1700000000000,"bid":100,"ask":100.1,"last":100.05,"volume":1,"flags":1,"isBuyerMaker":false}'}
    t2 = p.parse(fields2)
    assert t2 is not None
    assert t2.is_buyer_maker is False


def test_microstructure_engine_detects_momentum_mode():
    c = _imp_components()
    Tick = _imp_tick()

    # horizon_ms=50 is min in tracker; keep small alpha for quick
    rs = c.RealizedSpreadTracker(horizon_ms=50, alpha=0.5, max_pending=1000)

    # taker side: +1 for buy when is_buyer_maker=False
    def taker_side(t: Tick) -> int:
        if getattr(t, "is_buyer_maker", None) is False:
            return +1
        if getattr(t, "is_buyer_maker", None) is True:
            return -1
        return 0

    eng = c.MicrostructureEngine(
        rs=rs,
        momo_thr_bps=0.1,
        meanrev_thr_bps=-0.1,
        momo_adverse_max=0.9,
        meanrev_adverse_min=0.1,
        taker_side_fn=taker_side,
        mode_ema_alpha=0.5,
    )

    # trade at t0
    t0 = Tick(ts=1000, bid=100.00, ask=100.02, last=100.02, volume=1.0, flags=1, is_buyer_maker=False)
    eng.on_tick(t0)

    # after horizon, mid up -> realized positive => momentum
    t1 = Tick(ts=1100, bid=100.10, ask=100.12, last=0.0, volume=0.0, flags=0, is_buyer_maker=None)
    snap = eng.on_tick(t1)
    assert snap.market_mode in ("momentum", "mixed")  # depends on when settle happens

    # force settle by moving further in time
    t2 = Tick(ts=2000, bid=100.20, ask=100.22, last=0.0, volume=0.0, flags=0, is_buyer_maker=None)
    snap2 = eng.on_tick(t2)
    assert snap2.market_mode == "momentum"
    assert snap2.realized_ema_bps > 0


def test_l2_confirm_breakout_reason_codes():
    c = _imp_components()
    conf = c.L2ConfirmBreakout(
        require_obi20=True,
        mp_min_bps=0.2,
        wall_max_dist_bps=10.0,
        dep_min=0.05,
        ref_max=0.05,
        impact_max=0.35,
        use_l3=False,
        l3_ctr_max=3.0,
        l3_rate_min=0.0,
        l3_eta_max=0.0,
    )
    ctx = SimpleNamespace(
        obi_sustained_20=False,
        obi_avg_20=+1.0,
        microprice_shift_bps_20=1.0,
        wall_ask=False,
        wall_bid=False,
        depletion_score=0.2,
        refill_score=0.0,
        impact_proxy=0.0,
    )
    r = conf.check(ctx, dir_up=True)
    assert r.ok is False
    assert r.code == "obi20_not_sustained"


def test_score_model_contract_raw_to_final_and_confidence_pct():
    c = _imp_components()

    def conf_scorer(_ctx, _kind: str):
        return 80.0, {"x": 1}  # pct (backward compat)

    m = c.ScoreModel(conf_scorer=conf_scorer, kind_normalizer=lambda k: str(k), confidence_pct_k=100.0)
    ctx = SimpleNamespace()

    out = m.score(ctx, raw_score=0.25, signal_kind="breakout")
    assert out.conf_factor == pytest.approx(0.8)
    assert out.final_score == pytest.approx(0.2)
    assert out.confidence_pct == pytest.approx(20.0)
    assert ctx.final_score == pytest.approx(0.2)
    assert ctx.confidence_parts == {"x": 1}


def test_emitter_shrinks_manual_payload_when_too_big():
    c = _imp_components()

    emitter = c.Emitter(
        manual_signal_enabled=True,
        manual_signal_stream="stream:manual-signals",
        audit_level="full",
        audit_max_bytes=300,  # very small
    )

    envelope = {}
    signal = SimpleNamespace(
        sid="s1",
        ts=123,
        symbol="BTCUSDT",
        side="LONG",
        entry=1,
        sl=0,
        tp_levels=[1, 2, 3],
        lot=1,
        reason="x" * 500,  # huge
        confidence=55,
        atr=10,
        trail_after_tp1=True,
        trail_profile="p",
        indicators={"a": "b" * 500},
        metadata={"m": "z" * 500},
    )
    ctx = SimpleNamespace(confidence_pct=55, atr=10)

    def audit_full(_ctx):
        return {"big": "y" * 500}

    def audit_compact(_ctx):
        return {"c": 1}

    emitter.extend_outbox_envelope(envelope, signal=signal, ctx=ctx, build_audit_full=audit_full, build_audit_compact=audit_compact)

    assert "targets" in envelope
    mp = envelope["targets"]["manual_payload"]
    assert mp["sid"] == "s1"
    # metadata should be removed by shrink
    assert "metadata" not in mp or mp["metadata"] == {} or mp.get("metadata") is None
    # compact audit should be used at worst
    assert "audit_context" in mp
    assert mp["audit_context"] in ({"big": "y" * 500}, {"c": 1})


def test_safe_float_filters_nan_inf():
    c = _imp_components()
    assert c._safe_float(float("nan"), 7.0) == 7.0
    assert c._safe_float(float("inf"), 7.0) == 7.0
    assert c._safe_float("-1.25", 0.0) == pytest.approx(-1.25)
