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


def test_parse_replay_signal_nested_decision_payload():
    """Backward-compat: older joiner emitted {sid, decision:{ts_ms,symbol,...}, close:{...}, label:{...}}.
    parse_replay_signal must extract ts_ms/symbol/direction/scenario from the nested decision dict
    when they are absent at the top level of the payload."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import parse_replay_signal

    payload = {
        "sid": "crypto-of:BTCUSDT:1700000000000",
        "decision": {
            "ts_ms": 1700000000000,
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "scenario_v4": "trend",
        },
        "close": {"exit_ts_ms": 1700000100000},
        "label": {"r_mult": 0.5},
    }
    fields = {"payload": json.dumps(payload)}
    s = parse_replay_signal(fields)
    assert s is not None
    assert s.sid == "crypto-of:BTCUSDT:1700000000000"
    assert s.symbol == "BTCUSDT"
    assert s.direction == "BUY"
    assert s.scenario == "trend"


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
        }
    ]

    # --- default: bucket: prefix + time one-hots ---
    cols = infer_feature_cols(rows, max_numeric=10, include_direction=True, include_scenario=True)
    assert "f_spread_bps" in cols
    assert "f_expected_slippage_bps" in cols
    assert "f_delta_z" in cols
    assert "f_liq_regime" not in cols  # non-numeric key should be absent
    assert "direction_BUY" in cols and "direction_SELL" in cols
    # Commit 8 bucket: taxonomy
    assert "bucket:trend" in cols and "bucket:range" in cols and "bucket:other" in cols
    # Commit 8 time one-hots
    assert "hour:0" in cols and "hour:23" in cols
    assert "dow:0" in cols and "dow:6" in cols
    # legacy scenario_v4_ prefix must NOT be present when using default bucket:
    assert "scenario_v4_trend" not in cols

    # --- legacy: scenario_v4_ prefix, no time one-hots ---
    cols2 = infer_feature_cols(
        rows, max_numeric=4, include_direction=True, include_scenario=True,
        scenario_prefix="scenario_v4_", include_time_onehot=False,
    )
    assert "f_spread_bps" in cols2
    assert "direction_BUY" in cols2 and "direction_SELL" in cols2
    assert "scenario_v4_trend" in cols2 and "scenario_v4_range" in cols2
    assert "hour:0" not in cols2
    assert "bucket:trend" not in cols2


def test_infer_feature_cols_excludes_dq_policy_and_runtime_meta():
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import infer_feature_cols

    rows = [
        {
            "ts_ms": 1,
            "sid": "crypto-of:BTCUSDT:1",
            "symbol": "BTCUSDT",
            "direction": "BUY",
            "scenario": "trend",
            "indicators": {
                "spread_bps": 1.2,
                "tick_gap_p95_ms": 1500.0,
                "dq_pen": 0.1,
                "dq_level": 2,
                "runtime_start_ts_ms": 1700000000000,
                "dq_policy_dq_book_seq_ema_alpha": 0.10,
                "dq_policy_book_stream_interval_ms": 100,
            },
            "y": 1,
        }
    ]

    cols = infer_feature_cols(rows, max_numeric=32, include_direction=True, include_scenario=True)
    assert "f_spread_bps" in cols
    assert "f_tick_gap_p95_ms" in cols
    assert "f_dq_pen" not in cols
    assert "f_dq_level" not in cols
    assert "f_runtime_start_ts_ms" not in cols
    assert "f_dq_policy_dq_book_seq_ema_alpha" not in cols
    assert "f_dq_policy_book_stream_interval_ms" not in cols


def test_infer_feature_cols_strict_bucket_only():
    """strict_feature_cols=True + forbid_scenario_v4_onehot=True: only bucket: taxonomy allowed, no scenario_v4_*."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import infer_feature_cols

    rows = [
        {
            "ts_ms": 1,
            "sid": "crypto-of:BTCUSDT:1",
            "symbol": "BTCUSDT",
            "direction": "BUY",
            "scenario": "range_meanrev:v2|x",
            "indicators": {"spread_bps": 1.2, "expected_slippage_bps": 0.8, "delta_z": 0.5},
            "y": 1,
        },
        {
            "ts_ms": 2,
            "sid": "crypto-of:BTCUSDT:2",
            "symbol": "BTCUSDT",
            "direction": "SELL",
            "scenario": "trend_continuation",
            "indicators": {"spread_bps": 1.0, "expected_slippage_bps": 0.7, "delta_z": -0.2},
            "y": 0,
        }
    ]

    cols = infer_feature_cols(
        rows,
        max_numeric=10,
        include_direction=True,
        include_scenario=True,
        # default scenario_prefix="bucket:" - overrides don't matter in strict mode
        strict_feature_cols=True,
        forbid_scenario_v4_onehot=True,
    )
    # strict mode: bucket taxonomy always present
    assert "bucket:trend" in cols
    assert "bucket:range" in cols
    assert "bucket:other" in cols
    # strict mode: no legacy one-hots allowed
    assert not any(c.startswith("scenario_v4_") for c in cols)


def test_validate_feature_cols_strict_raises():
    """validate_feature_cols_strict raises ValueError when scenario_v4_* cols are present in strict mode."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import validate_feature_cols_strict

    # no-op when strict_feature_cols=False
    validate_feature_cols_strict(
        ["scenario_v4_trend", "f_spread_bps"],
        strict_feature_cols=False,
        forbid_scenario_v4_onehot=True,
    )  # must not raise

    # raises when strict and forbid both True
    try:
        validate_feature_cols_strict(
            ["scenario_v4_trend", "f_spread_bps"],
            strict_feature_cols=True,
            forbid_scenario_v4_onehot=True,
        )
        assert False, "expected ValueError for forbidden feature cols"
    except ValueError as exc:
        assert "scenario_v4_" in str(exc)

    # no-op when strict=True but forbid=False (explicit override)
    validate_feature_cols_strict(
        ["scenario_v4_trend"],
        strict_feature_cols=True,
        forbid_scenario_v4_onehot=False,
    )  # must not raise


def test_parse_replay_signal_from_joiner_payload_decision_nested():
    """trade_close_joiner_worker_v5 emits {sid, decision{...}, close{...}, ...}.
    parse_replay_signal must extract ts_ms/symbol/direction/scenario/indicators from nested decision."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import parse_replay_signal

    payload = {
        "sid": "BTCUSDT:1700000000000",
        "decision": {
            "sid": "BTCUSDT:1700000000000",
            "ts_ms": 1700000000000,
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "rule": {"scenario_v4": "trend"},
            "features": {"spread_bps": 5.0, "obi": 0.12},
        },
        "close": {"close_ts_ms": 1700000300000, "r_mult": 0.5},
    }
    row = parse_replay_signal({"payload": json.dumps(payload)})
    assert row is not None
    assert row.symbol == "BTCUSDT"
    assert row.ts_ms == 1700000000000
    assert row.direction == "BUY"  # LONG normalized
    assert row.scenario == "trend"
    assert isinstance(row.indicators, dict)
    assert row.indicators.get("spread_bps") == 5.0


def test_parse_replay_signal_minimal():
    """Direct flat payload (non-joiner) must still parse correctly."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import parse_replay_signal

    payload = {
        "sid": "BTCUSDT:1700000000000",
        "ts_ms": 1700000000000,
        "symbol": "BTCUSDT",
        "direction": "BUY",
        "scenario_v4": "trend",
        "indicators": {"spread_bps": 5.0},
        "label": {"r_mult": 0.5},
    }
    row = parse_replay_signal({"payload": json.dumps(payload)})
    assert row is not None
    assert row.symbol == "BTCUSDT"
    assert row.direction == "BUY"
    assert row.scenario == "trend"
    assert row.indicators["spread_bps"] == 5.0





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


# --- Commit 11: bucket: / hour: / dow: one-hots, adapted to current infer_feature_cols API ---

def test_infer_feature_cols_bucket_hour_dow():
    """Commit 11: infer_feature_cols with scenario_prefix='bucket:' + include_time_onehot=True
    must produce bucket:trend/range/other, hour:0..23, dow:0..6 columns.
    scenario_v4_* must be absent when using bucket: prefix."""
    from ml_analysis.tools.build_edge_stack_dataset_from_redis import infer_feature_cols

    rows = [
        {
            "ts_ms": 1700000000000,
            "sid": "crypto-of:BTCUSDT:1700000000000",
            "symbol": "BTCUSDT",
            "direction": "BUY",
            "scenario": "trend",
            "indicators": {"spread_bps": 1.2, "expected_slippage_bps": 0.8, "delta_z": 0.5},
            "y": 1,
        }
    ]

    # bucket:/hour:/dow: via current API: scenario_prefix="bucket:", include_time_onehot=True
    cols = infer_feature_cols(
        rows,
        max_numeric=32,
        include_direction=True,
        include_scenario=True,
        scenario_prefix="bucket:",
        include_time_onehot=True,
    )
    # bucket: taxonomy (low-cardinality)
    assert "bucket:trend" in cols and "bucket:range" in cols and "bucket:other" in cols
    # hour one-hots (UTC, 0..23)
    assert "hour:0" in cols and "hour:23" in cols and "hour:22" in cols
    # dow one-hots (UTC Mon=0, 0..6)
    assert "dow:0" in cols and "dow:6" in cols and "dow:1" in cols
    # legacy scenario_v4_ must be absent
    assert all(not c.startswith("scenario_v4_") for c in cols)


# --- Commit 12: strict mode rejects scenario_v4_* via validate_feature_cols_strict ---

def test_infer_feature_cols_strict_rejects_scenario_v4():
    """Commit 12: strict_feature_cols=True + forbid_scenario_v4_onehot=True must raise ValueError
    when scenario_v4_ prefix is requested, preventing unbounded cardinality in prod."""
    import pytest

    from ml_analysis.tools.build_edge_stack_dataset_from_redis import infer_feature_cols

    rows = [
        {
            "ts_ms": 1700000000000,
            "sid": "crypto-of:BTCUSDT:1700000000000",
            "symbol": "BTCUSDT",
            "direction": "BUY",
            "scenario": "trend",
            "indicators": {"spread_bps": 1.2, "expected_slippage_bps": 0.8},
            "y": 1,
        }
    ]

    with pytest.raises(ValueError, match="scenario_v4_"):
        infer_feature_cols(
            rows,
            max_numeric=32,
            include_direction=True,
            include_scenario=True,
            scenario_prefix="scenario_v4_",  # legacy prefix triggers strict check
            include_time_onehot=True,
            strict_feature_cols=True,
            forbid_scenario_v4_onehot=True,
        )
