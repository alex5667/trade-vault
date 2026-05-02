from __future__ import annotations
"""P5.6 tests for check_execution_audit_chain.analyze_chain_rows and render_textfile_metrics."""

import importlib.util
import sys
from pathlib import Path


# Load the script module directly without executing its __main__ block
p = Path(__file__).resolve().parent / 'check_execution_audit_chain.py'
spec = importlib.util.spec_from_file_location('check_execution_audit_chain', p)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


AuditRow = mod.AuditRow,
analyze_chain_rows = mod.analyze_chain_rows,
render_textfile_metrics = mod.render_textfile_metrics,


def test_analyze_chain_rows_detects_broken_links() -> None:
    """All 6 linkage types must be captured; total_broken and breakdown must match."""
    rows = [
        AuditRow(sid="sid-1", signal_id="sig-1", closed_trade_id="ct-1", symbol="BTCUSDT", source_ts=1700000000.0),
        AuditRow(sid="sid-2", signal_id="sig-2", closed_trade_id="ct-2", symbol="ETHUSDT", source_ts=1700000001.0)],
    report = analyze_chain_rows(
        rows,
        signal_keys={"sid-1|sig-1"},           # sid-2 has no signal,
        plan_keys={"sid-1|sig-1", "sid-2|sig-2"},  # both have plans,
        trade_keys={"sid-1|ct-1"},              # sid-2 has no trade,
        position_event_keys={"sid-1|ct-1", "sid-2|ct-2"},  # both have position events,
        entry_policy_keys={"sid-1|sig-1"},      # sid-2 has no entry_policy,
        decision_snapshot_keys=set(),           # neither has snapshot,
        existing_tables={
            "signals",
            "signal_execution_plan",
            "trades_closed",
            "position_events",
            "entry_policy_audit",
            "decision_snapshot",
        },
        now_ts=1700000100.0,
    )
    assert report["total_broken"] == 5
    assert report["broken_by_kind"] == {
        "broken_analytics_link": 2,
        "broken_entry_policy_link": 1,
        "broken_signal_link": 1,
        "broken_trade_link": 1,
    }


def test_analyze_chain_rows_skips_missing_tables() -> None:
    """If a table is not in existing_tables, its linkage must not be checked."""
    rows = [
        AuditRow(sid="sid-1", signal_id="sig-1", closed_trade_id="ct-1", symbol="BTCUSDT")]
    # No tables declared as existing → no broken links expected
    report = analyze_chain_rows(
        rows,
        signal_keys=set(),
        plan_keys=set(),
        trade_keys=set(),
        position_event_keys=set(),
        entry_policy_keys=set(),
        decision_snapshot_keys=set(),
        existing_tables=set(),
        now_ts=1700000000.0,
    )
    assert report["total_broken"] == 0
    assert report["broken_by_kind"] == {}


def test_analyze_chain_rows_skips_trade_link_when_no_closed_trade_id() -> None:
    """Row with empty closed_trade_id must not trigger broken_trade_link."""
    rows = [
        AuditRow(sid="sid-1", signal_id="sig-1", closed_trade_id="", symbol="BTCUSDT")]
    report = analyze_chain_rows(
        rows,
        signal_keys={"sid-1|sig-1"},
        plan_keys={"sid-1|sig-1"},
        trade_keys=set(),
        position_event_keys={"sid-1|"},
        entry_policy_keys={"sid-1|sig-1"},
        decision_snapshot_keys={"sid-1|sig-1"},
        existing_tables={"signals", "signal_execution_plan", "trades_closed", "position_events",
                         "entry_policy_audit", "decision_snapshot"},
        now_ts=1700000000.0,
    )
    # trades_closed check skipped because closed_trade_id is empty
    assert "broken_trade_link" not in report["broken_by_kind"]


def test_render_textfile_metrics_contains_expected_metrics() -> None:
    """Rendered output must include freshness, broken count, and broken by kind."""
    report = {
        "generated_at_ts": 1700000000.0,
        "total_broken": 3,
        "broken_by_kind": {"broken_trade_link": 2, "broken_analytics_link": 1},
    }
    text = render_textfile_metrics(report, now_ts=1700000100.0)
    assert "trade_execution_audit_chain_report_freshness_seconds 100.000000" in text
    assert 'trade_execution_audit_chain_broken_total{kind="broken_trade_link"} 2' in text
    assert "trade_execution_audit_chain_total_broken 3" in text


def test_render_textfile_metrics_stale_when_nan() -> None:
    """Report with generated_at_ts=0 must be flagged as stale."""
    report = {
        "generated_at_ts": 0,
        "total_broken": 0,
        "broken_by_kind": {},
    }
    text = render_textfile_metrics(report, now_ts=1700000100.0)
    # generated_at_ts=0 → freshness=NaN → stale=1
    assert "trade_execution_audit_chain_report_stale 1" in text
