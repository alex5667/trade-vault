"""Tests for signal_quality_gating/common/metrics_stage.py

Tests cover:
- fail-open behavior (None host, bad host)
- _tags() correctness
- stage_ms_hist backward compatibility (modern and legacy APIs)
- public API functions (meta_feature_seen_total, candidates_total, veto_total, etc.)

Uses importlib.util to load the file directly to avoid package naming conflicts
with python-worker/common/.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from unittest.mock import MagicMock

import pytest

# Load the specific file from signal_quality_gating, bypassing package shadowing
_sqg = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_ms_file = os.path.join(_sqg, "common", "metrics_stage.py")
_spec = importlib.util.spec_from_file_location("sqg_metrics_stage", _ms_file)
assert _spec is not None
_ms = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ms)  # type: ignore[union-attr]

candidates_total = _ms.candidates_total
dist = _ms.dist
emit_ok_total = _ms.emit_ok_total
feature_missing_total = _ms.feature_missing_total
meta_feature_missing_total = _ms.meta_feature_missing_total
meta_feature_seen_total = _ms.meta_feature_seen_total
stage_counter = _ms.stage_counter
stage_ms_hist = _ms.stage_ms_hist
veto_total = _ms.veto_total
_tags = _ms._tags


# ---------------------------------------------------------------------------
# _tags
# ---------------------------------------------------------------------------

class TestTags:
    def test_empty_args_returns_empty_dict(self) -> None:
        assert _tags() == {}

    def test_kind_only(self) -> None:
        assert _tags("test_kind") == {"kind": "test_kind"}

    def test_symbol_only(self) -> None:
        assert _tags("", "BTCUSDT") == {"symbol": "BTCUSDT"}

    def test_extra_tags(self) -> None:
        t = _tags(stage="scoring", reason="test")
        assert t["stage"] == "scoring"
        assert t["reason"] == "test"

    def test_empty_string_extra_excluded(self) -> None:
        t = _tags(stage="")
        assert "stage" not in t


# ---------------------------------------------------------------------------
# fail-open with None host
# ---------------------------------------------------------------------------

class TestFailOpen:
    def test_none_host_no_exception(self) -> None:
        meta_feature_seen_total(None, schema="s", feature="f")
        meta_feature_missing_total(None, schema="s", feature="f")
        feature_missing_total(None, feature="f")
        candidates_total(None)
        veto_total(None, reason_code="test")
        emit_ok_total(None)
        stage_ms_hist(None, stage="test", ms=1.0)
        dist(None, "metric", 1.0)
        stage_counter(None, "metric")

    def test_invalid_host_no_exception(self) -> None:
        for bad in ("string_host", 42, [], {}):
            stage_ms_hist(bad, stage="test", ms=1.0)  # type: ignore


# ---------------------------------------------------------------------------
# stage_ms_hist — modern API
# ---------------------------------------------------------------------------

class TestStageMsHistModernApi:
    def _make_host(self):
        host = MagicMock()
        mock_metrics = MagicMock()
        host.metrics = mock_metrics
        mock_metrics.observe = MagicMock()
        return host, mock_metrics

    def test_modern_api_calls_observe(self) -> None:
        host, metrics = self._make_host()
        stage_ms_hist(host, stage="scoring", ms=42.5, kind="LONG", symbol="BTCUSDT")
        assert metrics.observe.called
        # Our _obs calls m.observe(name, value=..., tags=...)
        name = metrics.observe.call_args[0][0]
        value = metrics.observe.call_args[1]['value']
        assert name == "scoring"
        assert value == pytest.approx(42.5)

    def test_modern_api_tags_contain_stage(self) -> None:
        host, metrics = self._make_host()
        stage_ms_hist(host, stage="emit", ms=10.0)
        tags = metrics.observe.call_args[1]["tags"]
        assert tags.get("stage") == "emit"


# ---------------------------------------------------------------------------
# stage_ms_hist — legacy API (positional name)
# ---------------------------------------------------------------------------

class TestStageMsHistLegacyApi:
    def _make_host(self):
        host = MagicMock()
        mock_metrics = MagicMock()
        host.metrics = mock_metrics
        mock_metrics.observe = MagicMock()
        return host, mock_metrics

    def test_legacy_api_uses_name_as_metric(self) -> None:
        host, metrics = self._make_host()
        stage_ms_hist(host, "legacy_metric_name", ms=123.0, kind="legacy_kind")
        assert metrics.observe.called
        # name is positional, value and tags are kwargs
        name = metrics.observe.call_args[0][0]
        value = metrics.observe.call_args[1]['value']
        assert name == "legacy_metric_name"
        assert value == pytest.approx(123.0)
        tags = metrics.observe.call_args[1]["tags"]
        assert tags.get("kind") == "legacy_kind"

    def test_both_name_and_stage_prefers_name(self) -> None:
        """name_or_stage positional arg takes precedence over stage= kwarg."""
        host, metrics = self._make_host()
        stage_ms_hist(host, "positional_name", stage="kwarg_stage", ms=1.0)
        name = metrics.observe.call_args[0][0]
        assert name == "positional_name"


# ---------------------------------------------------------------------------
# Public API functions with mock metrics
# ---------------------------------------------------------------------------

class TestPublicApiWithMetrics:
    def _host(self):
        host = MagicMock()
        metrics = MagicMock()
        host.metrics = metrics
        metrics.inc = MagicMock()
        metrics.observe = MagicMock()
        return host, metrics

    def test_candidates_total_increments(self) -> None:
        host, metrics = self._host()
        candidates_total(host, kind="crypto", symbol="ETHUSDT")
        assert metrics.inc.called
        # name is positional, value and tags are kwargs
        name = metrics.inc.call_args[0][0]
        value = metrics.inc.call_args[1]['value']
        assert name == "pipeline_candidates_total"
        assert value == 1

    def test_veto_total_with_reason_code(self) -> None:
        host, metrics = self._host()
        veto_total(host, reason_code="ml_veto")
        assert metrics.inc.called
        tags = metrics.inc.call_args[1]["tags"]
        assert tags.get("reason_code") == "ml_veto"

    def test_dist_calls_observe(self) -> None:
        host, metrics = self._host()
        dist(host, "my_dist", 3.14, kind="test")
        assert metrics.observe.called
        name = metrics.observe.call_args[0][0]
        value = metrics.observe.call_args[1]['value']
        assert name == "my_dist"
        assert value == pytest.approx(3.14)
