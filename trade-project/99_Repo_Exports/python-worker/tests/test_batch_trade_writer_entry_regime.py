"""BatchTradeWriter row builder parity with analytics_db.save_trade_closed."""
from __future__ import annotations

from types import SimpleNamespace

from services.batch_trade_writer import (
    _build_main_row,
    _entry_regime_db_value,
    _policy_mode_raw_from_payload,
)


def test_entry_regime_db_value_filters_sentinels():
    closed = SimpleNamespace(entry_regime="na", regime="trend")
    assert _entry_regime_db_value(closed) == "trend"
    closed2 = SimpleNamespace(entry_regime="unknown", regime=None)
    assert _entry_regime_db_value(closed2) is None


def test_policy_mode_raw_from_risk_surface_shadow():
    sp = {
        "config_snapshot": {
            "meta": {
                "risk_surface_shadow": {"mode": "shadow", "atr_pct": 1.2},
                "policy_mode": "enforce",
            }
        }
    }
    mode, raw = _policy_mode_raw_from_payload(sp)
    assert mode == "shadow"
    assert raw is not None
    assert "shadow" in raw


def test_build_main_row_includes_entry_regime_column():
    closed = SimpleNamespace(
        order_id="oid-1",
        sid="sid-1",
        strategy="s",
        source="cryptoorderflow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_ts_ms=1,
        exit_ts_ms=2,
        entry_price=100.0,
        exit_price=101.0,
        lot=0.01,
        notional_usd=1.0,
        pnl_net=0.1,
        pnl_gross=0.2,
        fees=0.01,
        pnl_pct=0.01,
        pnl_if_fixed_exit=0.0,
        tp1_hit=False,
        tp2_hit=False,
        tp3_hit=False,
        tp_hits=0,
        tp_before_sl=0,
        trailing_started=False,
        trailing_active=False,
        trailing_moves=0,
        mfe_pnl=0.0,
        mae_pnl=0.0,
        giveback=0.0,
        missed_profit=0.0,
        one_r_money=1.0,
        r_multiple=0.1,
        duration_ms=1000,
        close_reason="TP",
        entry_regime="trending_bull",
        signal_payload={},
        is_final_close=True,
        is_virtual=False,
    )
    row = _build_main_row(closed)
    assert row[-1] == "trending_bull"
