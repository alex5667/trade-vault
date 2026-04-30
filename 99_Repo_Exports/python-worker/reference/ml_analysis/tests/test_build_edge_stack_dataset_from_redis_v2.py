import json
import os
import tempfile

from ml_analysis.tools.build_edge_stack_dataset_from_redis import (
    DropStats
    QuarantineWriter
    SignalRow
    CloseRow
    diagnose_unmatched_closes
    _make_sid
    join_signals_with_closes_v2
)


def test_dropstats_counts_and_examples_limit():
    ds = DropStats(max_examples=2)
    ds.add("r1", {"a": 1})
    ds.add("r1", {"a": 2})
    ds.add("r1", {"a": 3})
    ds.add("r2", {"b": 1})
    out = ds.to_dict()
    assert out["counts"]["r1"] == 3
    assert out["counts"]["r2"] == 1
    assert len(out["examples"]["r1"]) == 2
    assert out["examples"]["r1"][0]["a"] == 1
    assert out["examples"]["r1"][1]["a"] == 2


def test_quarantine_writer_writes_jsonl():
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "q.jsonl")
        q = QuarantineWriter(p)
        q.write("close", "close_parse_none", stream="trades:closed", msg_id="1-0", data={"x": 1})
        q.write("signal", "signal_parse_none", stream="signals:of:inputs", msg_id="2-0", data={"y": 2})
        q.close()

        lines = open(p, "r", encoding="utf-8").read().strip().splitlines()
        assert len(lines) == 2
        r0 = json.loads(lines[0])
        assert r0["kind"] == "close"
        assert r0["reason"] == "close_parse_none"
        assert r0["stream"] == "trades:closed"
        assert r0["id"] == "1-0"
        assert r0["data"]["x"] == 1


def test_diagnose_unmatched_closes_buckets_and_examples():
    # Signals at 10s and 20s
    s1 = SignalRow(sid=_make_sid("BTCUSDT", 10_000), ts_ms=10_000, symbol="BTCUSDT", direction="BUY", scenario="trend", indicators={"a": 1})
    s2 = SignalRow(sid=_make_sid("BTCUSDT", 20_000), ts_ms=20_000, symbol="BTCUSDT", direction="SELL", scenario="range", indicators={"a": 2})
    index = {"BTCUSDT": [(10_000, s1.sid), (20_000, s2.sid)]}

    # Close near 20_000: delta 500ms => <=1s
    c1 = CloseRow(sid=_make_sid("BTCUSDT", 20_500), close_ts_ms=20_500, symbol="BTCUSDT", pnl=1.0, risk_usd=1.0)
    # Close far: delta 70s => <=5m bucket
    c2 = CloseRow(sid=_make_sid("BTCUSDT", 90_000), close_ts_ms=90_000, symbol="BTCUSDT", pnl=1.0, risk_usd=1.0)

    out = diagnose_unmatched_closes([c1, c2], signal_index_by_symbol=index, max_examples=10)
    assert out["counts"]["<=1s"] == 1
    # 90_000 nearest is 20_000 => delta 70_000ms => <=5m
    assert out["counts"]["<=5m"] == 1
    assert len(out["examples"]) == 2
    assert out["examples"][0]["symbol"] == "BTCUSDT"
    assert "nearest_signal_sid" in out["examples"][0]
    assert "delta_ms" in out["examples"][0]


def test_join_signals_with_closes_v2_returns_unmatched_strict_sid():
    s = SignalRow(sid=_make_sid("ETHUSDT", 1000), ts_ms=1000, symbol="ETHUSDT", direction="BUY", scenario="trend", indicators={"x": 1})
    c_ok = CloseRow(sid=_make_sid("ETHUSDT", 1000), close_ts_ms=2000, symbol="ETHUSDT", pnl=0.2, risk_usd=1.0)
    c_miss = CloseRow(sid=_make_sid("ETHUSDT", 3000), close_ts_ms=4000, symbol="ETHUSDT", pnl=0.2, risk_usd=1.0)

    rows, unmatched = join_signals_with_closes_v2(
        [s]
        [c_ok, c_miss]
        y_min_r=0.1
        dedup_signals="latest"
        join_strategy="sid"
    )
    assert len(rows) == 1
    assert len(unmatched) == 1
    assert unmatched[0].sid == c_miss.sid


def test_join_signals_with_closes_v2_nearest_join_within_tolerance():
    # Signal at 1000ms, close SID at 1500ms => SID mismatch; should join via nearest within tolerance=1000ms
    s = SignalRow(sid=_make_sid("ETHUSDT", 1000), ts_ms=1000, symbol="ETHUSDT", direction="BUY", scenario="trend", indicators={"x": 1})
    c = CloseRow(sid=_make_sid("ETHUSDT", 1500), close_ts_ms=1500, symbol="ETHUSDT", pnl=0.2, risk_usd=1.0)

    rows, unmatched = join_signals_with_closes_v2(
        [s]
        [c]
        y_min_r=0.1
        dedup_signals="latest"
        join_strategy="sid_or_nearest"
        join_tolerance_ms=1000
    )
    assert len(unmatched) == 0
    assert len(rows) == 1
    r0 = rows[0]
    assert r0["sid"] == s.sid
    assert r0["sid_close"] == c.sid
    assert r0["join_method"] == "nearest"
    assert int(r0["join_delta_ms"]) == 500


def test_join_signals_with_closes_v2_nearest_too_far_records_drop_and_unmatched():
    s = SignalRow(sid=_make_sid("BTCUSDT", 1000), ts_ms=1000, symbol="BTCUSDT", direction="BUY", scenario="trend", indicators={"x": 1})
    c = CloseRow(sid=_make_sid("BTCUSDT", 70_000), close_ts_ms=70_000, symbol="BTCUSDT", pnl=0.2, risk_usd=1.0)
    ds = DropStats(max_examples=5)

    rows, unmatched = join_signals_with_closes_v2(
        [s]
        [c]
        y_min_r=0.1
        dedup_signals="latest"
        join_strategy="sid_or_nearest"
        join_tolerance_ms=1000
        drop=ds
    )
    assert len(rows) == 0
    assert len(unmatched) == 1
    out = ds.to_dict()
    assert out["counts"].get("join_nearest_too_far", 0) == 1
    assert out["counts"].get("join_no_signal", 0) == 1


def test_join_nearest_bucket_tiebreak_prefers_bucket_match_on_equal_delta():
    # Two signals equidistant to the close; bucket-key match should win deterministically.
    s1 = SignalRow(
        sid=_make_sid("ETHUSDT", 1000)
        ts_ms=1000
        symbol="ETHUSDT"
        direction="BUY"
        scenario="trend"
        indicators={"session_bucket": "eu"}
    )
    s2 = SignalRow(
        sid=_make_sid("ETHUSDT", 1100)
        ts_ms=1100
        symbol="ETHUSDT"
        direction="BUY"
        scenario="trend"
        indicators={"session_bucket": "us"}
    )
    c = CloseRow(
        sid=_make_sid("ETHUSDT", 1050)
        close_ts_ms=1050
        symbol="ETHUSDT"
        pnl=0.2
        risk_usd=1.0
        buckets={"session_bucket": "us"}
    )

    jd = {}
    rows, unmatched = join_signals_with_closes_v2(
        [s1, s2]
        [c]
        y_min_r=0.1
        join_strategy="nearest"
        join_tolerance_ms=500
        join_secondary="none"
        nearest_max_scan=10
        join_bucket_keys=["session_bucket"]
        join_debug=jd
    )
    assert len(unmatched) == 0
    assert len(rows) == 1
    r0 = rows[0]
    assert r0["join_method"] == "nearest"
    assert r0["sid"] == s2.sid  # bucket match wins
    assert int(r0["join_bucket_score"]) == 1
    assert int(r0["join_candidate2_n"]) == 2
    assert jd.get("nearest_join", {}).get("ambiguous", 0) == 1


def test_join_nearest_secondary_dir_scenario_strict_filters_candidates():
    # Two candidates within tolerance, but only one matches direction+scenario.
    s1 = SignalRow(
        sid=_make_sid("BTCUSDT", 1000)
        ts_ms=1000
        symbol="BTCUSDT"
        direction="BUY"
        scenario="trend"
        indicators={"x": 1}
    )
    s2 = SignalRow(
        sid=_make_sid("BTCUSDT", 1200)
        ts_ms=1200
        symbol="BTCUSDT"
        direction="SELL"
        scenario="trend"
        indicators={"x": 2}
    )
    c = CloseRow(
        sid=_make_sid("BTCUSDT", 1100)
        close_ts_ms=1100
        symbol="BTCUSDT"
        pnl=0.2
        risk_usd=1.0
        direction="BUY"
        scenario="trend"
    )

    rows, unmatched = join_signals_with_closes_v2(
        [s1, s2]
        [c]
        y_min_r=0.1
        join_strategy="nearest"
        join_tolerance_ms=500
        join_secondary="dir_scenario"
        nearest_max_scan=10
    )
    assert len(unmatched) == 0
    assert len(rows) == 1
    assert rows[0]["sid"] == s1.sid
    assert rows[0]["join_method"] == "nearest"



