"""Unit tests for services/observability/latency_semconv.py and latency_contract.py."""
from __future__ import annotations

import sys, os
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
from services.observability.latency_semconv import (
    as_int_ms,
    non_negative_delta_ms,
    ensure_epoch_ms_fields,
    compute_contract_deltas,
    parse_allowlist,
    label_symbol,
    STAGE_REDIS_TO_FEATURE,
    STAGE_FEATURE_TO_EMIT,
    STAGE_END_TO_END_EVENT,
    FIELD_TS_EVENT_MS,
    FIELD_TS_REDIS_READ_MS,
    FIELD_TS_FEATURE_MS,
    FIELD_TS_EMIT_MS,
)


class TestAsIntMs:
    def test_int_passthrough(self):
        assert as_int_ms(1000) == 1000

    def test_float_truncates(self):
        assert as_int_ms(1000.9) == 1000

    def test_string_parses(self):
        assert as_int_ms("2000") == 2000

    def test_none_returns_default(self):
        assert as_int_ms(None, default=42) == 42

    def test_bad_string_returns_default(self):
        assert as_int_ms("notanumber", default=99) == 99

    def test_bool_treated_as_default(self):
        assert as_int_ms(True, default=0) == 0


class TestNonNegativeDeltaMs:
    def test_normal_delta(self):
        assert non_negative_delta_ms(100, 200) == 100

    def test_reversed_is_zero(self):
        assert non_negative_delta_ms(200, 100) == 0

    def test_equal_is_zero(self):
        assert non_negative_delta_ms(100, 100) == 0

    def test_zero_start_is_zero(self):
        assert non_negative_delta_ms(0, 200) == 0

    def test_none_start_is_zero(self):
        assert non_negative_delta_ms(None, 200) == 0

    def test_none_end_is_zero(self):
        assert non_negative_delta_ms(100, None) == 0


class TestEnsureEpochMsFields:
    def test_copies_ts_ms_to_event_ms(self):
        p = {"ts_ms": 1000, "ts_feature_ms": 2000, "ts_emit_ms": 3000}
        ensure_epoch_ms_fields(p)
        # ts_ms is the fallback; ts_event_ms should be populated
        assert p[FIELD_TS_EVENT_MS] == 1000

    def test_prefers_existing_ts_event_ms(self):
        p = {FIELD_TS_EVENT_MS: 999, "ts_ms": 1000}
        ensure_epoch_ms_fields(p)
        assert p[FIELD_TS_EVENT_MS] == 999

    def test_uses_default_feature_ms(self):
        p = {"ts_ms": 1000}
        ensure_epoch_ms_fields(p, default_feature_ms=5000)
        assert p[FIELD_TS_FEATURE_MS] == 5000

    def test_mutates_in_place_and_returns(self):
        p = {"ts_ms": 1000, "ts_feature_ms": 2000}
        result = ensure_epoch_ms_fields(p)
        assert result is p


class TestComputeContractDeltas:
    def test_redis_to_feature(self):
        p = {
            FIELD_TS_REDIS_READ_MS: 1000,
            FIELD_TS_FEATURE_MS: 1050,
            FIELD_TS_EMIT_MS: 1150,
            FIELD_TS_EVENT_MS: 900,
        }
        d = compute_contract_deltas(p)
        assert d[STAGE_REDIS_TO_FEATURE] == 50
        assert d[STAGE_FEATURE_TO_EMIT] == 100
        assert d[STAGE_END_TO_END_EVENT] == 250

    def test_missing_fields_yields_zero(self):
        p = {}
        d = compute_contract_deltas(p)
        assert d[STAGE_REDIS_TO_FEATURE] == 0
        assert d[STAGE_FEATURE_TO_EMIT] == 0
        assert d[STAGE_END_TO_END_EVENT] == 0

    def test_non_monotonic_yields_zero(self):
        p = {
            FIELD_TS_REDIS_READ_MS: 2000,
            FIELD_TS_FEATURE_MS: 1000,  # earlier than redis_read – non-monotonic
        }
        d = compute_contract_deltas(p)
        assert d[STAGE_REDIS_TO_FEATURE] == 0


class TestLabelSymbol:
    def test_no_allowlist_passthrough(self):
        assert label_symbol("BTCUSDT") == "BTCUSDT"

    def test_in_allowlist(self):
        assert label_symbol("BTCUSDT", allowlist={"BTCUSDT", "ETHUSDT"}) == "BTCUSDT"

    def test_not_in_allowlist_collapse(self):
        assert label_symbol("XYZUSDT", allowlist={"BTCUSDT"}, mode="collapse") == "__other__"

    def test_not_in_allowlist_drop(self):
        assert label_symbol("XYZUSDT", allowlist={"BTCUSDT"}, mode="drop") is None

    def test_empty_symbol_returns_none(self):
        assert label_symbol("", allowlist={"BTCUSDT"}) is None


class TestParseAllowlist:
    def test_basic(self):
        assert parse_allowlist("BTCUSDT,ETHUSDT") == {"BTCUSDT", "ETHUSDT"}

    def test_empty_string(self):
        assert parse_allowlist("") == set()

    def test_none(self):
        assert parse_allowlist(None) == set()

    def test_strips_whitespace(self):
        assert parse_allowlist("BTC , ETH") == {"BTC", "ETH"}
