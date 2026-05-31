"""Tests for latency_contract_exporter_v1._parse_key."""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
from orderflow_services.latency_contract_exporter_v1 import _parse_key


PREFIX = "metrics:latency_contract:last"


class TestParseKey:
    def test_valid_key(self):
        key = f"{PREFIX}:python_worker:feature_to_emit:BTCUSDT"
        result = _parse_key(key, PREFIX)
        assert result == ("python_worker", "feature_to_emit", "BTCUSDT")

    def test_symbol_with_colon_still_parsed(self):
        # symbol could in theory have embedded data; last part is always symbol
        key = f"{PREFIX}:python_worker:end_to_end_event:ETHUSDT"
        result = _parse_key(key, PREFIX)
        assert result == ("python_worker", "end_to_end_event", "ETHUSDT")

    def test_wrong_prefix_returns_none(self):
        key = "other:prefix:python_worker:feature_to_emit:BTCUSDT"
        result = _parse_key(key, PREFIX)
        assert result is None

    def test_too_few_parts_returns_none(self):
        key = f"{PREFIX}:python_worker"
        result = _parse_key(key, PREFIX)
        assert result is None

    def test_empty_service_returns_none(self):
        key = f"{PREFIX}::feature_to_emit:BTCUSDT"
        result = _parse_key(key, PREFIX)
        assert result is None
