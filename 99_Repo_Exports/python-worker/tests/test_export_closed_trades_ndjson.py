import json

from tools.export_closed_trades_ndjson import _extract_closed


def test_extract_closed_basic():
    fields = {
        "event_type": "POSITION_CLOSED",
        "sid": "signal-1",
        "symbol": "BTCUSDT",
        "ts": "1700000000000",
        "pnl": "100.0",
        "risk_usd": "50.0",
        "scenario": "continuation",
        "regime": "thin",
        "abs_lvl_tier": "2",
        "of_confirm_ok": "1",
        "ab_arm": "B",
        "ab_group": "thin",
        "arm_ver": "3",
        "meta": json.dumps({"close_reason": "tp1"}),
    }
    row = _extract_closed(fields)
    assert row is not None
    assert row["symbol"] == "BTCUSDT"
    assert row["scenario"] == "continuation"
    assert row["regime"] == "thin"
    assert row["abs_lvl_tier"] == 2
    assert row["of_confirm_ok"] == 1
    assert abs(row["r_mult"] - 2.0) < 1e-9
    assert row["close_reason"] == "tp1"


def test_extract_closed_filters_non_closed():
    fields = {"event_type": "TP1_HIT", "sid": "x", "symbol": "BTCUSDT"}
    assert _extract_closed(fields) is None
