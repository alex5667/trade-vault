"""
Tests for news_pipeline.classifier and news_pipeline.reco_builder.

Covers:
- Deterministic classification (no LLM, no wall-clock)
- Grade/action/reason_code correctness for known event types
- Unknown titles return grade_id=0 / matched=False
- reco_builder builds correct entries, expires_ms, risk_factor_bps
- merge: higher grade wins, lower grade never downgrades active block
- stale entries are evicted on merge
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from news_pipeline.classifier import (
    ClassifyResult,
    classify,
    risk_factor_bps_for_action,
)
from news_pipeline.reco_builder import (
    RecoMapWriter,
    build_reco_entries,
    merge_reco_map,
)


# ── classifier ────────────────────────────────────────────────────────────────

class TestClassifier:
    def test_cpi_grade_5_block(self):
        r = classify("US CPI comes in hotter than expected")
        assert r.event_type == "macro_cpi"
        assert r.grade_id == 5
        assert r.default_action == "block"
        assert r.matched is True

    def test_fomc_grade_5_block(self):
        r = classify("FOMC rate decision: Fed holds rates steady")
        assert r.event_type == "macro_fomc"
        assert r.grade_id == 5
        assert r.default_action == "block"

    def test_nfp_grade_5(self):
        r = classify("NFP beats expectations: 250k jobs added")
        assert r.event_type == "macro_jobs"
        assert r.grade_id == 5

    def test_fed_speech_grade_4_tighten(self):
        r = classify("Powell speech: Fed ready to hike if needed")
        assert r.event_type == "macro_fed_speech"
        assert r.grade_id == 4
        assert r.default_action == "tighten"

    def test_hack_grade_5_block(self):
        r = classify("$150M stolen in DeFi exploit, funds at risk")
        assert r.event_type == "crypto_security"
        assert r.grade_id == 5
        assert r.default_action == "block"

    def test_binance_outage_grade_5_block(self):
        r = classify("Binance API down for maintenance, trading halt")
        assert r.event_type == "exchange_status"
        assert r.grade_id == 5
        assert r.default_action == "block"

    def test_etf_approval_grade_5_protective_only(self):
        r = classify("SEC approves spot bitcoin ETF — historic decision")
        assert r.event_type == "crypto_etf"
        assert r.grade_id == 5
        assert r.default_action == "protective_only"

    def test_sec_crypto_regulation_tighten(self):
        r = classify("SEC files lawsuit against crypto exchange for token sales")
        assert r.event_type == "crypto_regulation"
        assert r.grade_id == 4
        assert r.default_action == "tighten"

    def test_geopolitics_tighten(self):
        r = classify("US imposes new sanctions following military invasion")
        assert r.event_type == "geopolitics"
        assert r.grade_id == 4
        assert r.default_action == "tighten"

    def test_unknown_title_grade_0_allow(self):
        r = classify("Bitcoin price analysis for the week ahead")
        assert r.event_type == "unknown"
        assert r.grade_id == 0
        assert r.default_action == "allow"
        assert r.matched is False

    def test_deterministic_same_input_same_output(self):
        title = "US CPI data release scheduled for Tuesday"
        r1 = classify(title)
        r2 = classify(title)
        assert r1 == r2

    def test_highest_grade_wins_when_multiple_match(self):
        # Both CPI (grade 5) and fed_speech (grade 4) patterns present
        r = classify("Powell comments on CPI — inflation still elevated")
        assert r.grade_id == 5  # CPI wins

    def test_summary_used_for_classification(self):
        # Title alone may not match, but summary does
        r = classify("Breaking economic news", summary="NFP beats forecasts significantly")
        assert r.event_type == "macro_jobs"
        assert r.grade_id == 5


# ── risk_factor_bps_for_action ────────────────────────────────────────────────

class TestRiskFactorBps:
    def test_block_returns_zero(self):
        assert risk_factor_bps_for_action("block", 5) == 0

    def test_protective_only_returns_zero(self):
        assert risk_factor_bps_for_action("protective_only", 5) == 0

    def test_allow_returns_full(self):
        assert risk_factor_bps_for_action("allow", 0) == 10000

    def test_tighten_grade5_lower_than_grade3(self):
        bps5 = risk_factor_bps_for_action("tighten", 5)
        bps3 = risk_factor_bps_for_action("tighten", 3)
        assert bps5 < bps3
        assert 0 < bps5 < 10000
        assert 0 < bps3 < 10000


# ── reco_builder ──────────────────────────────────────────────────────────────

NOW_MS = 1_710_000_000_000


def _cpi_result() -> ClassifyResult:
    return classify("US CPI comes in hotter than expected")


class TestBuildRecoEntries:
    def test_grade0_returns_empty(self):
        r = classify("nothing interesting here at all")
        assert r.grade_id == 0
        entries = build_reco_entries(result=r, now_ts_ms=NOW_MS)
        assert entries == {}

    def test_block_reco_has_zero_risk_factor(self):
        entries = build_reco_entries(result=_cpi_result(), now_ts_ms=NOW_MS)
        assert len(entries) > 0
        for sym, entry in entries.items():
            assert entry["action"] == "block"
            assert entry["risk_factor_bps"] == 0
            assert entry["expires_ms"] > NOW_MS
            assert entry["symbol"] == sym

    def test_expires_ms_uses_post_sec(self):
        r = _cpi_result()
        entries = build_reco_entries(result=r, now_ts_ms=NOW_MS, event_ts_ms=NOW_MS)
        for entry in entries.values():
            assert entry["expires_ms"] >= NOW_MS + r.post_sec * 1000

    def test_sentinel_all_crypto_expanded(self):
        r = classify("Binance API down for maintenance, trading halt")
        active = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
        entries = build_reco_entries(result=r, now_ts_ms=NOW_MS, active_symbols=active)
        for sym in active:
            assert sym in entries

    def test_source_event_id_propagated(self):
        entries = build_reco_entries(
            result=_cpi_result(), now_ts_ms=NOW_MS, source_event_id="uid_abc"
        )
        for entry in entries.values():
            assert entry["source_event_id"] == "uid_abc"


class TestMergeRecoMap:
    def test_new_higher_grade_overwrites_lower(self):
        existing = {
            "BTCUSDT": {
                "action": "tighten",
                "grade_id": 3,
                "expires_ms": NOW_MS + 60_000,
                "risk_factor_bps": 4000,
            }
        }
        new = {
            "BTCUSDT": {
                "action": "block",
                "grade_id": 5,
                "expires_ms": NOW_MS + 1800_000,
                "risk_factor_bps": 0,
            }
        }
        merged = merge_reco_map(existing, new, now_ts_ms=NOW_MS)
        assert merged["BTCUSDT"]["grade_id"] == 5
        assert merged["BTCUSDT"]["action"] == "block"

    def test_lower_grade_does_not_downgrade_active_block(self):
        existing = {
            "BTCUSDT": {
                "action": "block",
                "grade_id": 5,
                "expires_ms": NOW_MS + 1800_000,
                "risk_factor_bps": 0,
            }
        }
        new = {
            "BTCUSDT": {
                "action": "tighten",
                "grade_id": 3,
                "expires_ms": NOW_MS + 60_000,
                "risk_factor_bps": 4000,
            }
        }
        merged = merge_reco_map(existing, new, now_ts_ms=NOW_MS)
        assert merged["BTCUSDT"]["grade_id"] == 5
        assert merged["BTCUSDT"]["action"] == "block"

    def test_expired_entries_evicted(self):
        existing = {
            "BTCUSDT": {
                "action": "block",
                "grade_id": 5,
                "expires_ms": NOW_MS - 1,  # already expired
                "risk_factor_bps": 0,
            }
        }
        merged = merge_reco_map(existing, {}, now_ts_ms=NOW_MS)
        assert "BTCUSDT" not in merged

    def test_new_symbol_added(self):
        existing: dict = {}
        new = {
            "ETHUSDT": {
                "action": "tighten",
                "grade_id": 4,
                "expires_ms": NOW_MS + 900_000,
                "risk_factor_bps": 2500,
            }
        }
        merged = merge_reco_map(existing, new, now_ts_ms=NOW_MS)
        assert "ETHUSDT" in merged


# ── RecoMapWriter ─────────────────────────────────────────────────────────────

class TestRecoMapWriter:
    def _mock_redis(self, existing_json: str | None = None) -> MagicMock:
        r = MagicMock()
        r.get.return_value = existing_json
        r.set.return_value = True
        return r

    def test_grade0_noop(self):
        r = self._mock_redis()
        writer = RecoMapWriter(redis_client=r)
        result = classify("nothing interesting")
        n = writer.apply(result=result, now_ts_ms=NOW_MS)
        assert n == 0
        r.set.assert_not_called()

    def test_grade5_writes_reco(self):
        r = self._mock_redis()
        writer = RecoMapWriter(redis_client=r)
        result = _cpi_result()
        n = writer.apply(result=result, now_ts_ms=NOW_MS, source_event_id="cpi_uid")
        assert n > 0
        r.set.assert_called_once()
        written_json = r.set.call_args[0][1]
        obj = json.loads(written_json)
        assert obj["schema_ver"] == "news_reco_map_v1"
        assert obj["producer"] == "news-analyzer"
        assert "BTCUSDT" in obj["reco"]
        assert obj["reco"]["BTCUSDT"]["action"] == "block"
        assert obj["reco"]["BTCUSDT"]["reason_code"] == "macro_high_impact_cpi"

    def test_redis_error_returns_zero(self):
        r = self._mock_redis()
        r.set.side_effect = Exception("Redis timeout")
        writer = RecoMapWriter(redis_client=r)
        result = _cpi_result()
        n = writer.apply(result=result, now_ts_ms=NOW_MS)
        assert n == 0  # fail-open, no raise

    def test_merges_with_existing_map(self):
        existing = {
            "schema_ver": "news_reco_map_v1",
            "ts_ms": NOW_MS - 60_000,
            "producer": "news-analyzer",
            "reco": {
                "SOLUSDT": {
                    "symbol": "SOLUSDT",
                    "action": "tighten",
                    "grade_id": 3,
                    "expires_ms": NOW_MS + 600_000,
                    "risk_factor_bps": 4000,
                    "reason_code": "existing",
                    "source_event_id": "old",
                    "sentiment": "mixed",
                    "asof_ts_ms": NOW_MS - 60_000,
                    "confidence": 0.8,
                }
            },
        }
        r = self._mock_redis(json.dumps(existing))
        writer = RecoMapWriter(redis_client=r)
        result = _cpi_result()
        writer.apply(result=result, now_ts_ms=NOW_MS)
        written = json.loads(r.set.call_args[0][1])
        # CPI-affected symbols should be block
        assert written["reco"]["BTCUSDT"]["action"] == "block"
        # existing SOLUSDT tighten should be preserved (CPI also adds SOLUSDT as block)
        # CPI is grade 5 > existing grade 3, so it wins
        assert written["reco"]["SOLUSDT"]["grade_id"] == 5
