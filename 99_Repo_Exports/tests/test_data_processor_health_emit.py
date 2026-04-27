from __future__ import annotations

import sys
from pathlib import Path

# Add python-worker to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python-worker"))


class HM:
    def __init__(self):
        self.calls = []

    def on_tick(self, *, symbol: str, l2_age_ms: float, l2_age_ms_tick: float, l2_is_stale: bool, l2_is_stale_now: bool, **kwargs):
        self.calls.append((symbol, l2_age_ms, l2_age_ms_tick, l2_is_stale, l2_is_stale_now))


class ST:
    l2_age_ms = 10.0
    l2_is_stale = False
    l2_is_stale_now = True


def test_emit_health_metrics_best_effort(monkeypatch):
    # импортируем класс после патча
    from handlers.data_processor import OrderFlowDataProcessor

    # Create minimal config
    class Config:
        delta_window_ticks = 100

    hm = HM()
    dp = OrderFlowDataProcessor("BTCUSDT", specs=None, config=Config(), health_metrics=hm)
    # подменим bucket_state минимальным объектом
    dp._bucket_state = ST()

    dp._emit_health_metrics_best_effort()
    assert len(hm.calls) == 1
    sym, l2_age, l2_age_tick, stale, stale_now = hm.calls[0]
    assert sym == "BTCUSDT"
    assert l2_age == 10.0
    assert l2_age_tick == 10.0
    assert stale is False
    assert stale_now is True
