from __future__ import annotations

import json


def test_finalize_trade_adds_realized_slippage_spread_and_adverse(monkeypatch):
    """
    Интеграционный тест "закрывающего" уровня:
      - имитируем закрытие сделки и проверяем, что finalize_trade переносит
        execution-quality поля в TradeClosed:
          realized_slippage_bps
          realized_spread_bps
          adverse_bps_t (json)
    """
    import domain.handlers as handlers
    from domain.models import PositionState

    class FakeSpec:
        contract_size = 1.0
        def pnl_money(self, entry_price: float, price: float, lot: float, direction: str, symbol="") -> float:
            sign = 1.0 if str(direction).upper() == "LONG" else -1.0
            return (float(price) - float(entry_price)) * sign * lot

    spec = FakeSpec()

    pos = PositionState(
        id="pos1",
        sid="sid1",
        strategy="CryptoOrderFlow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",  # type: ignore
        entry_price=100.0,
        entry_ts_ms=1000,
        lot=1.0,
        remaining_qty=0.0,
        sl=90.0,
        tp_levels=[101.0, 102.0, 103.0],
    )

    # имитируем "срез рынка" на тике закрытия
    pos.exit_mid_price = 99.0
    pos.exit_spread_bps = 50.0
    # имитируем adverse_bps_t накопленное в process_tick
    pos.adverse_bps_t = {500: 12.0, 2000: 25.0}

    closed = handlers.finalize_trade(
        pos,
        spec,
        exit_price=98.5,
        exit_ts_ms=4000,
        close_reason_raw="TP3",
        tp_ratios=[0.3, 0.3, 0.4],
    )

    # realized_slippage_bps = |98.5-99|/99*1e4 ≈ 50.505...
    assert hasattr(closed, "realized_slippage_bps")
    assert float(closed.realized_slippage_bps) > 0

    assert hasattr(closed, "realized_spread_bps")
    assert abs(float(closed.realized_spread_bps) - 50.0) < 1e-9

    assert hasattr(closed, "adverse_bps_t")
    adv = json.loads(closed.adverse_bps_t)
    assert adv["500"] == 12.0 or adv.get(500) == 12.0
    assert adv["2000"] == 25.0 or adv.get(2000) == 25.0
