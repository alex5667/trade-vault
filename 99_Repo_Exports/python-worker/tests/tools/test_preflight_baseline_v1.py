from __future__ import annotations
"""Tests for preflight_baseline_v1.py.

Covers:
- parse_prometheus_text: Prometheus text exposition parsing
- parse_prom_rules: /api/v1/rules JSON parsing
- parse_compose_book_env: docker-compose.yml key extraction
- run(): fail-open behavior when URLs are unreachable
"""


import json
import os
import sys
import tempfile
import unittest.mock

import pytest

# ---------------------------------------------------------------------------
# Import guard
# ---------------------------------------------------------------------------
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tools"))

try:
    from tools.preflight_baseline_v1 import (
        parse_prometheus_text,
        parse_prom_rules,
        parse_compose_book_env,
        run,
    )
except ImportError:
    try:
        from preflight_baseline_v1 import (  # type: ignore
            parse_prometheus_text,
            parse_prom_rules,
            parse_compose_book_env,
            run,
        )
    except Exception as exc:
        pytest.skip(f"preflight_baseline_v1 not found: {exc}", allow_module_level=True)


# ---------------------------------------------------------------------------
# parse_prometheus_text
# ---------------------------------------------------------------------------

PROM_TEXT_SAMPLE = """\
# HELP book_missing_seq_ema EMA of book missing-seq events (0..1)
# TYPE book_missing_seq_ema gauge
book_missing_seq_ema{symbol="BTCUSDT"} 0.05
book_missing_seq_ema{symbol="ETHUSDT"} 0.0
# HELP dq_level Data quality level
# TYPE dq_level gauge
dq_level{symbol="BTCUSDT"} 0
# HELP dq_veto_total Number of times DQ entered veto-capable state
# TYPE dq_veto_total counter
dq_veto_total{bucket="book_missing_seq_hard",symbol="BTCUSDT"} 1
# HELP tick_gap_n Number of samples in TickGapTracker window
# TYPE tick_gap_n gauge
tick_gap_n{symbol="BTCUSDT"} 500
"""


class TestParsePrometheusText:
    def test_basic_parse(self):
        families = parse_prometheus_text(PROM_TEXT_SAMPLE)
        assert "book_missing_seq_ema" in families
        assert "dq_level" in families
        assert "dq_veto_total" in families
        assert "tick_gap_n" in families

    def test_label_extraction(self):
        families = parse_prometheus_text(PROM_TEXT_SAMPLE)
        btc = [s for s in families["book_missing_seq_ema"] if s["labels"].get("symbol") == "BTCUSDT"]
        assert btc
        assert btc[0]["value"] == pytest.approx(0.05)

    def test_multiple_labels(self):
        families = parse_prometheus_text(PROM_TEXT_SAMPLE)
        dv = families["dq_veto_total"]
        assert len(dv) == 1
        assert dv[0]["labels"].get("bucket") == "book_missing_seq_hard"
        assert dv[0]["value"] == pytest.approx(1.0)

    def test_help_and_type_propagated(self):
        families = parse_prometheus_text(PROM_TEXT_SAMPLE)
        s = families["dq_level"][0]
        assert s["type"] == "gauge"
        assert "Data quality" in s["help"]

    def test_empty_text(self):
        families = parse_prometheus_text("")
        assert families == {}

    def test_comments_only(self):
        families = parse_prometheus_text("# just a comment\n\n# another\n")
        assert families == {}

    def test_no_labels(self):
        text = "# TYPE my_metric counter\nmy_metric 42\n"
        families = parse_prometheus_text(text)
        assert "my_metric" in families
        assert families["my_metric"][0]["value"] == pytest.approx(42.0)


# ---------------------------------------------------------------------------
# parse_prom_rules
# ---------------------------------------------------------------------------

PROM_RULES_JSON = json.dumps({
    "status": "success",
    "data": {
        "groups": [
            {
                "name": "orderflow",
                "file": "/etc/prometheus/rules/orderflow.yml",
                "interval": 60,
                "rules": [
                    {
                        "type": "alerting",
                        "name": "BookMissingSeqHigh",
                        "query": "book_missing_seq_ema > 0.2",
                        "duration": 300,
                        "labels": {"severity": "warning"},
                        "annotations": {},
                        "state": "inactive",
                    },
                    {
                        "type": "recording",
                        "name": "job:book_missing_seq_ema:avg",
                        "query": "avg by (job)(book_missing_seq_ema)",
                        "labels": {},
                        "annotations": {},
                        "state": "",
                    },
                ],
            },
        ]
    },
})


class TestParsePomRules:
    def test_status_success(self):
        result = parse_prom_rules(PROM_RULES_JSON)
        assert result["status"] == "success"

    def test_counts(self):
        result = parse_prom_rules(PROM_RULES_JSON)
        assert result["alert_count"] == 1
        assert result["recording_count"] == 1

    def test_group_name(self):
        result = parse_prom_rules(PROM_RULES_JSON)
        assert result["groups"][0]["name"] == "orderflow"

    def test_rule_fields(self):
        result = parse_prom_rules(PROM_RULES_JSON)
        rule = result["groups"][0]["rules"][0]
        assert rule["name"] == "BookMissingSeqHigh"
        assert rule["type"] == "alerting"
        assert rule["state"] == "inactive"

    def test_invalid_json(self):
        result = parse_prom_rules("{invalid}")
        assert result["status"] == "parse_error"
        assert "error" in result

    def test_empty_groups(self):
        j = json.dumps({"status": "success", "data": {"groups": []}})
        result = parse_prom_rules(j)
        assert result["alert_count"] == 0
        assert result["groups"] == []


# ---------------------------------------------------------------------------
# parse_compose_book_env
# ---------------------------------------------------------------------------

COMPOSE_YAML_CONTENT = """\
version: '3.8'
services:
  scanner-crypto-orderflow:
    image: my-image:latest
    environment:
      SYMBOLS: BTCUSDT,ETHUSDT
      BOOK_MISSING_SEQ_EMA_ALPHA: "0.1"
      DQ_BOOK_VETO_ENABLED: "0"
      DQ_GATE_ENABLE: "1"
      REDIS_URL: redis://redis:6379
  prometheus:
    image: prom/prometheus
    environment:
      GF_LOG_LEVEL: info
"""


class TestParseComposeBookEnv:
    def test_extracts_book_stream_keys(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(COMPOSE_YAML_CONTENT)
            path = f.name
        try:
            result = parse_compose_book_env(path)
            assert result["error"] is None
            svcs = result["services"]
            # Only scanner has book-stream keys
            assert "scanner-crypto-orderflow" in svcs
            env = svcs["scanner-crypto-orderflow"]["env"]
            assert "SYMBOLS" in env
            assert "BOOK_MISSING_SEQ_EMA_ALPHA" in env
            assert "DQ_BOOK_VETO_ENABLED" in env
        finally:
            os.unlink(path)

    def test_excludes_non_book_env(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(COMPOSE_YAML_CONTENT)
            path = f.name
        try:
            result = parse_compose_book_env(path)
            # REDIS_URL should not appear (no book-stream key)
            env = result["services"].get("scanner-crypto-orderflow", {}).get("env", {})
            assert "REDIS_URL" not in env
        finally:
            os.unlink(path)

    def test_missing_file(self):
        result = parse_compose_book_env("/nonexistent/compose.yml")
        assert result["error"] is not None

    def test_empty_path(self):
        result = parse_compose_book_env("")
        assert result["error"] is not None


# ---------------------------------------------------------------------------
# run() integration (fail-open)
# ---------------------------------------------------------------------------

class TestRunFailOpen:
    def test_run_with_unreachable_urls(self, tmp_path):
        """run() should not raise even when all endpoints are unreachable."""
        out_path = str(tmp_path / "baseline.json")
        snap = run(
            metrics_url="http://127.0.0.1:19999/metrics",  # unreachable
            prom_url="http://127.0.0.1:19998",              # unreachable
            compose="/nonexistent/docker-compose.yml",
            out=out_path,
            timeout=1,
        )
        # Must not raise; returns partial snapshot
        assert isinstance(snap, dict)
        assert "snapshot_ts_ms" in snap

    def test_run_writes_output(self, tmp_path):
        out_path = str(tmp_path / "baseline.json")
        run(
            metrics_url="http://127.0.0.1:19999/metrics",
            prom_url="http://127.0.0.1:19998",
            compose="/nonexistent/docker-compose.yml",
            out=out_path,
            timeout=1,
        )
        assert os.path.exists(out_path)
        with open(out_path) as f:
            data = json.load(f)
        assert data["version"] == 1
        assert data["step"] == 0
