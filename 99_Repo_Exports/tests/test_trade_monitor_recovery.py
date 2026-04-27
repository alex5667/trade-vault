import pytest

import sys
from pathlib import Path

# Add python-worker to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python-worker"))

from services.trade_monitor import TradeMonitorService


class DummyLogger:
    def warning(self, *a, **k):
        pass


class DummyTM:
    def _to_int_ms(self, v, default):
        try:
            if v is None:
                return int(default)
            return int(float(v))
        except Exception:
            return int(default)


def test_position_from_hash_restores_profile_and_tp_fills_and_normalizes_side():
    tm = TradeMonitorService.__new__(TradeMonitorService)
    tm._to_int_ms = DummyTM()._to_int_ms
    tm.logger = DummyLogger()

    h = {
        "status": "open",
        "id": "o1",
        "sid": "s1",
        "strategy": "strat",
        "source": "src",
        "symbol": "BTCUSDT",
        "tf": "60",
        "direction": "short",
        "entry_price": "100",
        "entry_time": "1700000000000",
        "lot": "1",
        "remaining_qty": "1",
        "sl": "110",
        "tp_levels": "[90,80,70]",
        "trailing_profile": "rocket_v1",  # alias
        "tp1_fill_price": "90.5",
        "tp1_fill_ts": "1700000001000",
    }

    pos = TradeMonitorService._position_from_hash(tm, h)
    assert pos is not None
    assert pos.direction == "SHORT"
    assert pos.trail_profile == "rocket_v1"
    assert pos.tp_fill_prices[1] == 90.5
    assert pos.tp_fill_times[1] == 1700000001000