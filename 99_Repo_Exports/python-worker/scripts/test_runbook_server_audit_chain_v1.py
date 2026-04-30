"""P5.6 tests for the audit chain endpoint in the runbook server.

Tests use the filter_report and load_report functions from a wrapped runbook_server
that exposes the /api/audit-chain/latest endpoint.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from io import BytesIO
from unittest.mock import patch


# Load the runbook_server module directly without executing its __main__ block
ROOT = Path(__file__).resolve().parents[1]
MOD_PATH = ROOT / "runbooks" / "server" / "runbook_server.py"
spec = importlib.util.spec_from_file_location("runbook_server_p56", MOD_PATH)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


filter_audit_chain_report = mod.filter_audit_chain_report


def test_filter_audit_chain_report_by_sid_and_kind() -> None:
    """Filter by sid + kind must return only matching rows and recalculate broken_by_kind."""
    report = {
        "broken": [
            {"kind": "broken_trade_link", "sid": "a", "signal_id": "s1", "closed_trade_id": "t1"}
            {"kind": "broken_analytics_link", "sid": "a", "signal_id": "s1", "closed_trade_id": "t1"}
            {"kind": "broken_trade_link", "sid": "b", "signal_id": "s2", "closed_trade_id": "t2"}
        ]
        "total_broken": 3
        "broken_by_kind": {"broken_trade_link": 2, "broken_analytics_link": 1}
    }
    out = filter_audit_chain_report(report, {"sid": ["a"], "kind": ["broken_trade_link"]})
    assert out["total_broken"] == 1
    assert out["broken_by_kind"] == {"broken_trade_link": 1}
    assert out["broken"][0]["closed_trade_id"] == "t1"


def test_filter_audit_chain_report_no_filters_returns_all() -> None:
    """With no filters, filter_audit_chain_report must return all rows."""
    report = {
        "broken": [
            {"kind": "broken_trade_link", "sid": "a"}
            {"kind": "broken_analytics_link", "sid": "b"}
        ]
        "total_broken": 2
        "broken_by_kind": {"broken_trade_link": 1, "broken_analytics_link": 1}
    }
    out = filter_audit_chain_report(report, {})
    assert out["total_broken"] == 2


def test_filter_audit_chain_report_limit_applied() -> None:
    """Limit parameter must cap the number of rows returned."""
    broken_rows = [{"kind": "broken_trade_link", "sid": str(i)} for i in range(10)]
    report = {"broken": broken_rows, "total_broken": 10, "broken_by_kind": {}}
    out = filter_audit_chain_report(report, {"limit": ["3"]})
    assert out["total_broken"] == 3
    assert len(out["broken"]) == 3


def test_filter_audit_chain_report_by_signal_id() -> None:
    """Filter by signal_id must narrow results correctly."""
    report = {
        "broken": [
            {"kind": "broken_signal_link", "sid": "a", "signal_id": "sig-X"}
            {"kind": "broken_signal_link", "sid": "b", "signal_id": "sig-Y"}
        ]
        "total_broken": 2
        "broken_by_kind": {"broken_signal_link": 2}
    }
    out = filter_audit_chain_report(report, {"signal_id": ["sig-X"]})
    assert out["total_broken"] == 1
    assert out["broken"][0]["signal_id"] == "sig-X"


def test_load_audit_chain_report_not_found(tmp_path) -> None:
    """load_audit_chain_report must return an error dict when file is missing."""
    load_fn = mod.load_audit_chain_report
    result = load_fn(str(tmp_path / "nonexistent.json"))
    assert "error" in result
    assert result.get("total_broken") == 0
