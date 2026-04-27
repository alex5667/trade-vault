from dataclasses import dataclass

from common.metrics2 import InMemoryMetrics
from common.signal_metrics import SignalMetrics


@dataclass
class Ctx:
    symbol: str = "BTCUSDT"
    timeframe: str = "1m"
    venue: str = "binance"
    family: str = "crypto"


def test_candidates_total_and_veto_reason():
    m = InMemoryMetrics()
    sm = SignalMetrics(m)
    ctx = Ctx()

    sm.candidate(ctx=ctx, kind="breakout")
    sm.veto(ctx=ctx, kind="breakout", reason="conf_below_min_veto")

    assert any(n == "candidates_total" and (t or {}).get("kind") == "breakout" for (n, v, t) in m.counters)
    assert any(n == "signals_veto" and (t or {}).get("reason") == "conf_below_min_veto" for (n, v, t) in m.counters)


def test_protective_metrics_from_reason():
    m = InMemoryMetrics()
    sm = SignalMetrics(m)
    ctx = Ctx()

    sm.veto(ctx=ctx, kind="breakout", reason="spread_too_wide_veto")
    sm.veto(ctx=ctx, kind="breakout", reason="cooldown_active")
    sm.veto(ctx=ctx, kind="breakout", reason="touch_suppressed")

    assert any(n == "spread_filter_drops" for (n, v, t) in m.counters)
    assert any(n == "cooldown_drops" for (n, v, t) in m.counters)
    assert any(n == "touch_suppressed_total" for (n, v, t) in m.counters)


def test_score_hist_observations():
    m = InMemoryMetrics()
    sm = SignalMetrics(m)
    ctx = Ctx()

    sm.observe_scores(ctx=ctx, kind="absorption", conf_factor01=0.73, final_score=1.46)
    assert any(n == "conf_factor_hist" and abs(v - 0.73) < 1e-9 for (n, v, t) in m.observations)
    assert any(n == "final_score_hist" and abs(v - 1.46) < 1e-9 for (n, v, t) in m.observations)
