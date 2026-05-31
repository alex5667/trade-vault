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
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import CloseRow, SignalRow, join_signals_with_closes

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
    # Default scenario_prefix="bucket:" produces bucket:trend / bucket:range columns
    assert "bucket:trend" in cols and "bucket:range" in cols
    # Legacy scenario_v4_ prefix must NOT appear with default prefix
    assert not any(c.startswith("scenario_v4_") for c in cols)








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


def test_filter_by_time_no_start_passes_all():
    """start_ms=None must NOT filter out items older than the close window.

    P0 root cause 2: signals emitted before the 72h window but whose
    corresponding close falls inside the window were dropped because
    _filter_by_time applied the same since_ms to both signals and closes.
    With sig_start_ms=None the signal stream is fully scanned.
    """
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import _filter_by_time

    CLOSE_WINDOW_START = 1700000000000  # 72h boundary (used for closes)
    SIGNAL_TS_OLD = CLOSE_WINDOW_START - 1_000  # 1 s before window — would be dropped by old code
    SIGNAL_TS_IN  = CLOSE_WINDOW_START + 1_000  # inside window

    items = [
        ("old-signal", {"ts_ms": str(SIGNAL_TS_OLD)}),
        ("new-signal", {"ts_ms": str(SIGNAL_TS_IN)}),
    ]

    # Old behaviour: start_ms applied to signals → old-signal dropped
    out_old = _filter_by_time(items, ts_field_candidates=("ts_ms",), start_ms=CLOSE_WINDOW_START, end_ms=None)
    assert [x[0] for x in out_old] == ["new-signal"]

    # New behaviour: start_ms=None → both signals kept
    out_new = _filter_by_time(items, ts_field_candidates=("ts_ms",), start_ms=None, end_ms=None)
    assert [x[0] for x in out_new] == ["old-signal", "new-signal"]


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


# ── Dual-index join (alt_sid via ts_emit_ms) ────────────────────────────────

def test_parse_replay_signal_builds_alt_sid_from_ts_emit_ms():
    """Signal with of: SID (bar_ts) gets alt_sid using ts_emit_ms (tick_ts)."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import parse_replay_signal

    payload = {
        "ts_ms": 1779665158000,          # bar_ts, rounded to seconds
        "ts_emit_ms": 1779665155939,     # tick_ts, ms precision
        "symbol": "ETHUSDT",
        "direction": "SHORT",
        "sid": "of:ETHUSDT:1779665158000:S",
        "indicators": {},
    }
    fields = {"payload": json.dumps(payload)}
    s = parse_replay_signal(fields)

    assert s is not None
    assert s.sid == "crypto-of:ETHUSDT:1779665158000"
    assert s.alt_sid == "crypto-of:ETHUSDT:1779665155939"


def test_parse_replay_signal_iceberg_alt_sid_equals_sid():
    """Iceberg signal: ts_emit_ms equals embedded ts → alt_sid == sid (no duplicate key)."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import parse_replay_signal

    payload = {
        "ts_ms": 1779662342000,
        "ts_emit_ms": 1779662341941,
        "symbol": "ETHUSDT",
        "direction": "LONG",
        "sid": "iceberg:ETHUSDT:1779662341941:L",
        "indicators": {},
    }
    fields = {"payload": json.dumps(payload)}
    s = parse_replay_signal(fields)

    assert s is not None
    assert s.sid == "crypto-of:ETHUSDT:1779662341941"
    assert s.alt_sid == "crypto-of:ETHUSDT:1779662341941"


def test_build_signal_map_dual_index_allows_ts_emit_ms_join():
    """trades:closed records ts_emit_ms in signal_id → join succeeds via alt_sid."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import (
        parse_replay_signal, parse_trade_closed, _build_signal_map,
    )

    # Signal with of: SID (bar_ts) and ts_emit_ms
    sig_payload = {
        "ts_ms": 1779665158000,
        "ts_emit_ms": 1779665155939,
        "symbol": "ETHUSDT",
        "direction": "SHORT",
        "sid": "of:ETHUSDT:1779665158000:S",
        "indicators": {"delta_z": -2.5, "spread_bps": 0.5},
    }
    s = parse_replay_signal({"payload": json.dumps(sig_payload)})
    assert s is not None

    smap = _build_signal_map([s], dedup_signals="latest")

    # Close uses ts_emit_ms in signal_id (as trades:closed does)
    close_fields = {
        "symbol": "ETHUSDT",
        "signal_id": "of:ETHUSDT:1779665155939:S",  # ts_emit_ms-based
        "exit_ts_ms": "1779674830503",
        "pnl": "10.0",
        "risk_usd": "5.0",
    }
    c = parse_trade_closed(close_fields)
    assert c is not None
    assert c.sid == "crypto-of:ETHUSDT:1779665155939"

    # Join succeeds via alt_sid
    assert c.sid in smap, f"Expected {c.sid} in smap, keys={list(smap)[:5]}"
