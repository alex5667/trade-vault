from __future__ import annotations

"""Tests for build_confirm_train_v7_from_redis."""

import json
import os
import tempfile

from ml_analysis.tools.build_confirm_train_v7_from_redis import (
    _normalize_sid,
    _write_json_atomic,
    _write_jsonl_atomic,
    build_confirm_train_v7,
    parse_decision,
    parse_outcome,
)

# ─── SID normalization ────────────────────────────────────────────────────────

class TestNormalizeSid:
    def test_canonical(self):
        assert _normalize_sid("crypto-of:BTCUSDT:1710000000000") == "crypto-of:BTCUSDT:1710000000000"

    def test_pipe_format(self):
        assert _normalize_sid("ETHUSDT|1710000000000|BUY") == "crypto-of:ETHUSDT:1710000000000"

    def test_from_parts(self):
        assert _normalize_sid("", symbol="SOLUSDT", ts_ms=123456) == "crypto-of:SOLUSDT:123456"

    def test_extra_suffix(self):
        assert _normalize_sid("crypto-of:BTCUSDT:999:extra") == "crypto-of:BTCUSDT:999"

    def test_of_prefix_long(self):
        # Current SID format from decisions:final and trades:closed
        assert _normalize_sid("of:BTCUSDT:1779380746000:L") == "crypto-of:BTCUSDT:1779380746000"

    def test_of_prefix_short(self):
        assert _normalize_sid("of:1000PEPEUSDT:1779297753648:S") == "crypto-of:1000PEPEUSDT:1779297753648"

    def test_of_prefix_matches_crypto_of(self):
        # Decisions (of:) and outcomes (of:) must normalize to the same SID
        decision_sid = _normalize_sid("of:ETHUSDT:1779408884000:S")
        outcome_sid = _normalize_sid("of:ETHUSDT:1779408884000:S")
        assert decision_sid == outcome_sid == "crypto-of:ETHUSDT:1779408884000"


# ─── Decision parsing ────────────────────────────────────────────────────────

class TestParseDecision:
    def test_full_payload(self):
        fields = {
            "sid": "crypto-of:BTCUSDT:1000",
            "payload": json.dumps({
                "sid": "crypto-of:BTCUSDT:1000",
                "symbol": "BTCUSDT",
                "ts_ms": 1000,
                "direction": "BUY",
                "of_confirm": {
                    "score": 0.75,
                    "indicators": {"spread_bps": 1.5},
                    "evidence": {"ctx_key": "global"},
                },
                "rule": {"scenario_v4": "trend", "score": 0.75},
                "inputs": {"spread_bps": 1.5, "expected_slippage_bps": 0.3},
            }),
        }
        rec = parse_decision(fields)
        assert rec is not None
        assert rec["sid"] == "crypto-of:BTCUSDT:1000"
        assert rec["symbol"] == "BTCUSDT"
        assert rec["direction"] == "BUY"
        assert rec["of_confirm"]["score"] == 0.75
        assert rec["spread_bps"] == 1.5
        assert rec["scenario_v4"] == "trend"

    def test_missing_sid(self):
        fields = {"payload": json.dumps({"symbol": "X", "ts_ms": 100, "direction": "BUY"})}
        assert parse_decision(fields) is None

    def test_missing_symbol(self):
        fields = {"payload": json.dumps({"sid": "crypto-of:X:100", "ts_ms": 100, "direction": "BUY"})}
        assert parse_decision(fields) is None

    def test_of_confirm_from_indicators(self):
        """If of_confirm is nested inside indicators, promote it."""
        fields = {
            "payload": json.dumps({
                "sid": "crypto-of:BTCUSDT:2000",
                "symbol": "BTCUSDT",
                "ts_ms": 2000,
                "direction": "SELL",
                "indicators": {
                    "of_confirm": {"score": 0.88, "indicators": {"atr_bps": 5.0}, "evidence": {}},
                },
            }),
        }
        rec = parse_decision(fields)
        assert rec is not None
        assert rec["of_confirm"]["score"] == 0.88

    def test_promotes_decision_time_liqmap_features(self):
        fields = {
            "payload": json.dumps({
                "sid": "crypto-of:BTCUSDT:3000",
                "symbol": "BTCUSDT",
                "ts_ms": 3000,
                "direction": "BUY",
                "indicators_small": {
                    "liqmap_5m_total_usd": 1000.0,
                    "liqmap_5m_near_imb": -0.2,
                    "liqmap_gate_rr": 1.7,
                },
            }),
        }
        rec = parse_decision(fields)
        assert rec is not None
        assert rec["liqmap_5m_total_usd"] == 1000.0
        assert rec["liqmap_5m_near_imb"] == -0.2
        assert rec["liqmap_gate_rr"] == 1.7

    def test_promotes_legacy_nested_liqmap_record(self):
        fields = {
            "payload": json.dumps({
                "sid": "crypto-of:ETHUSDT:4000",
                "symbol": "ETHUSDT",
                "ts_ms": 4000,
                "direction": "SELL",
                "liqmap": {
                    "gate": {"shadow_veto": 1, "veto": 0, "rr": 2.1, "risk_bps": 8.0, "reward_bps": 16.8},
                    "w5m": {"age_ms": 500.0, "near_total_usd": 2000.0, "dist_up_bps": 4.5},
                    "w1h": {"age_ms": 1500.0, "peak_dn1_usd": 9000.0},
                },
            }),
        }
        rec = parse_decision(fields)
        assert rec is not None
        assert rec["liqmap_gate_shadow_veto"] == 1
        assert rec["liqmap_gate_rr"] == 2.1
        assert rec["liqmap_5m_near_total_usd"] == 2000.0
        assert rec["liqmap_5m_dist_up_bps"] == 4.5
        assert rec["liqmap_1h_peak_dn1_usd"] == 9000.0


# ─── Outcome parsing ─────────────────────────────────────────────────────────

class TestParseOutcome:
    def test_basic(self):
        fields = {
            "sid": "crypto-of:BTCUSDT:1000",
            "symbol": "BTCUSDT",
            "pnl": "1.5",
            "risk_usd": "10.0",
            "exit_ts_ms": "9999",
            "direction": "BUY",
        }
        rec = parse_outcome(fields)
        assert rec is not None
        assert rec["sid"] == "crypto-of:BTCUSDT:1000"
        assert rec["pnl"] == 1.5
        assert rec["risk_usd"] == 10.0
        assert rec["exit_ts_ms"] == 9999

    def test_nested_payload(self):
        fields = {
            "payload": json.dumps({
                "sid": "crypto-of:ETHUSDT:2000",
                "symbol": "ETHUSDT",
                "pnl": -0.5,
                "risk_usd": 5.0,
                "exit_ts_ms": 8888,
                "direction": "SELL",
            }),
        }
        rec = parse_outcome(fields)
        assert rec is not None
        assert rec["sid"] == "crypto-of:ETHUSDT:2000"
        assert rec["pnl"] == -0.5

    def test_missing_sid(self):
        fields = {"symbol": "X", "pnl": "1.0"}
        assert parse_outcome(fields) is None


# ─── Full pipeline (file-based, no Redis) ─────────────────────────────────────

class TestBuildPipeline:
    def test_no_redis_no_archive_empty(self):
        """With no data sources, produces empty files."""
        with tempfile.TemporaryDirectory() as tmp:
            dec_path = os.path.join(tmp, "decisions.ndjson")
            out_path = os.path.join(tmp, "outcomes.ndjson")
            rep_path = os.path.join(tmp, "report.json")
            report = build_confirm_train_v7(
                redis_url="",
                decisions_archive_dir="",
                closes_archive_dir="",
                out_decisions=dec_path,
                out_outcomes=out_path,
                out_report=rep_path,
            )
            assert report["joined_sids"] == 0
            assert report["decisions_written"] == 0
            assert report["outcomes_written"] == 0
            assert os.path.isfile(dec_path)
            assert os.path.isfile(out_path)
            assert os.path.isfile(rep_path)


# ─── Atomic writes ───────────────────────────────────────────────────────────

class TestAtomicWrites:
    def test_write_jsonl(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "test.jsonl")
            rows = [{"a": 1}, {"b": 2}]
            n = _write_jsonl_atomic(path, rows)
            assert n == 2
            with open(path) as f:
                lines = f.readlines()
            assert len(lines) == 2
            assert json.loads(lines[0])["a"] == 1

    def test_write_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "test.json")
            _write_json_atomic(path, {"x": 42})
            with open(path) as f:
                d = json.load(f)
            assert d["x"] == 42
