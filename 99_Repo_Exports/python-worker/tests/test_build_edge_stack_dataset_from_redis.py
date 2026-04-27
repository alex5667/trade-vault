import json
import sys
from pathlib import Path

# Ensure repo root is on sys.path even under pytest --import-mode=importlib
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_normalize_sid_canonical_and_legacy():
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import _normalize_sid

    sid1 = _normalize_sid("crypto-of:BTCUSDT:1700000000000", symbol="BTCUSDT", ts_ms=1700000000000)
    assert sid1 == "crypto-of:BTCUSDT:1700000000000"

    sid2 = _normalize_sid("crypto-of:BTCUSDT:1700000000000:BUY", symbol="BTCUSDT", ts_ms=1)
    assert sid2 == "crypto-of:BTCUSDT:1700000000000"

    sid3 = _normalize_sid("ETHUSDT|170|SELL", symbol="ETHUSDT", ts_ms=170)
    assert sid3 == "crypto-of:ETHUSDT:170"


def test_parse_replay_signal_from_payload():
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import parse_replay_signal

    payload = {
        "ts_ms": 1700000000000,
        "symbol": "BTCUSDT",
        "direction": "BUY",
        "scenario_v4": "trend",
        "sid": "crypto-of:BTCUSDT:1700000000000",
        "indicators": {"spread_bps": 1.2, "expected_slippage_bps": 0.8, "exec_risk_norm": 0.1, "delta_z": 0.5},
    }
    fields = {"payload": json.dumps(payload)}

    s = parse_replay_signal(fields)
    assert s is not None
    assert s.sid == "crypto-of:BTCUSDT:1700000000000"
    assert s.symbol == "BTCUSDT"
    assert s.direction == "BUY"
    assert s.scenario == "trend"
    assert "delta_z" in s.indicators


def test_parse_trade_closed_requires_sid():
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import parse_trade_closed

    c = parse_trade_closed({"symbol": "BTCUSDT", "pnl": 10, "risk_usd": 5, "exit_ts_ms": 1700000010000})
    assert c is None

    c2 = parse_trade_closed(
        {"symbol": "BTCUSDT", "sid": "crypto-of:BTCUSDT:1700000000000", "pnl": 10, "risk_usd": 5, "exit_ts_ms": 1700000010000}
    )
    assert c2 is not None
    assert c2.sid == "crypto-of:BTCUSDT:1700000000000"
    assert c2.pnl == 10.0
    assert c2.risk_usd == 5.0


def test_join_signals_with_closes_and_label():
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import SignalRow, CloseRow, join_signals_with_closes

    s = SignalRow(
        sid="crypto-of:BTCUSDT:1700000000000",
        ts_ms=1700000000000,
        symbol="BTCUSDT",
        direction="BUY",
        scenario="trend",
        indicators={"spread_bps": 1.2, "expected_slippage_bps": 0.8, "exec_risk_norm": 0.2, "delta_z": 0.5},
    )
    c = CloseRow(
        sid="crypto-of:BTCUSDT:1700000000000",
        close_ts_ms=1700000100000,
        symbol="BTCUSDT",
        pnl=10.0,
        risk_usd=20.0,
    )

    rows = join_signals_with_closes([s], [c], y_min_r=0.4)
    assert len(rows) == 1
    assert rows[0]["y"] == 1  # 10/20 = 0.5 >= 0.4
    assert abs(rows[0]["r_mult"] - 0.5) < 1e-12


def test_infer_feature_cols():
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import infer_feature_cols

    rows = [
        {
            "ts_ms": 1,
            "sid": "crypto-of:BTCUSDT:1",
            "symbol": "BTCUSDT",
            "direction": "BUY",
            "scenario": "trend",
            "indicators": {"spread_bps": 1.2, "expected_slippage_bps": 0.8, "delta_z": 0.5, "liq_regime": "hi"},
            "y": 1,
        },
        {
            "ts_ms": 2,
            "sid": "crypto-of:BTCUSDT:2",
            "symbol": "BTCUSDT",
            "direction": "SELL",
            "scenario": "range",
            "indicators": {"spread_bps": 1.0, "expected_slippage_bps": 0.7, "delta_z": -0.2},
            "y": 0,
        },
    ]

    cols = infer_feature_cols(rows, max_numeric=10, include_direction=True, include_scenario=True)
    assert "f_spread_bps" in cols
    assert "f_expected_slippage_bps" in cols
    assert "f_delta_z" in cols
    assert "f_liq_regime" not in cols
    assert "direction_BUY" in cols and "direction_SELL" in cols
    assert "scenario_v4_trend" in cols and "scenario_v4_range" in cols








def test_parse_trade_closed_from_payload_json():
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import parse_trade_closed

    payload = {
        "symbol": "ETHUSDT",
        "sid": "crypto-of:ETHUSDT:1700000000000",
        "pnl": 12.5,
        "risk_usd": 25,
        "exit_ts_ms": 1700000100000,
    }
    c = parse_trade_closed({"payload": json.dumps(payload)})
    assert c is not None
    assert c.sid == "crypto-of:ETHUSDT:1700000000000"
    assert c.pnl == 12.5
    assert c.risk_usd == 25.0
    assert c.close_ts_ms == 1700000100000


def test_filter_by_time_reads_payload_ts():
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import _filter_by_time

    items = [
        ("1-0", {"payload": json.dumps({"exit_ts_ms": 1700000000000})}),
        ("2-0", {"payload": json.dumps({"exit_ts_ms": 1700000001000})}),
        ("3-0", {"payload": json.dumps({"exit_ts_ms": 1700000002000})}),
    ]
    out = _filter_by_time(items, ts_field_candidates=("exit_ts_ms",), start_ms=1700000000500, end_ms=1700000001500)
    assert [x[0] for x in out] == ["2-0"]


def test_read_archive_items_basic(tmp_path):
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import _read_archive_items

    # one-day archive file
    d = tmp_path / "signals"
    d.mkdir()
    fp = d / "2023-11-14.ndjson"
    fp.write_text(
        "\n".join(
            [
                json.dumps({"stream_id": "1-0", "payload": {"ts_ms": 1700000000000, "sid": "a"}}),
                json.dumps({"stream_id": "2-0", "payload": {"ts_ms": 1700000005000, "sid": "b"}}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    items, st = _read_archive_items(str(d), start_ms=1700000001000, end_ms=1700000009000, lookback_days=365, max_records=10)
    assert len(items) == 1
    assert items[0][0] in ("2-0", "file:2023-11-14.ndjson:2")
    assert st["parsed"] >= 1
