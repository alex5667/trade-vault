from __future__ import annotations

"""Tests for check_execution_consistency.py (P6).

Loads the script via importlib so the test is independent of any package
installation; the script lives at scripts/check_execution_consistency.py
relative to the project root.
"""

import importlib.util
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Module bootstrap — loadable via pytest from any working directory
# ---------------------------------------------------------------------------
SCRIPT = Path(__file__).resolve().parent.parent.parent / 'scripts' / 'check_execution_consistency.py'
assert SCRIPT.exists(), f"Script not found: {SCRIPT}"
SPEC = importlib.util.spec_from_file_location('check_execution_consistency', SCRIPT)
mod = importlib.util.module_from_spec(SPEC)  # type: ignore[arg-type]
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)  # type: ignore[union-attr]
# ---------------------------------------------------------------------------


def test_compare_execution_views_detects_sql_missing_and_state_mismatch():
    """SQL missing row → 'sql_missing' critical mismatch."""
    redis_state = {
        'sid-1': {'sid': 'sid-1', 'symbol': 'BTCUSDT', 'status': 'open', 'fsm_state': 'PROTECTED', 'sl_algo_id': 11}
    }
    stream_latest = {
        'sid-1': {'sid': 'sid-1', 'symbol': 'BTCUSDT', 'status': 'open', 'fsm_state': 'PROTECTED'}
    }
    sql_orders: dict = {}
    mismatches, _stream_suppr, _redis_suppr = mod.compare_execution_views(redis_state, stream_latest, sql_orders, {})
    categories = {m.category for m in mismatches}
    assert 'sql_missing' in categories, f"Expected sql_missing, got: {categories}"


def test_compare_execution_views_no_mismatches_when_consistent():
    """No mismatches when all three views agree."""
    state = {'sid': 'sid-x', 'symbol': 'ETHUSDT', 'status': 'open', 'fsm_state': 'ACTIVE',
              'execution_policy': 'live', 'position_side': 'LONG'}
    mismatches, _stream_suppr, _redis_suppr = mod.compare_execution_views({'sid-x': state}, {'sid-x': state}, {'sid-x': state}, {})
    assert mismatches == [], f"Expected no mismatches, got: {mismatches}"


def test_compare_execution_views_fsm_state_mismatch_is_critical():
    """fsm_state divergence between Redis and SQL must be critical."""
    r_state = {'sid': 'sid-2', 'symbol': 'SOLUSDT', 'status': 'open', 'fsm_state': 'PROTECTED'}
    s_state = {'sid': 'sid-2', 'symbol': 'SOLUSDT', 'status': 'open', 'fsm_state': 'PROTECTED'}
    q_state = {'sid': 'sid-2', 'symbol': 'SOLUSDT', 'status': 'open', 'fsm_state': 'ACTIVE'}
    mismatches, _stream_suppr, _redis_suppr = mod.compare_execution_views({'sid-2': r_state}, {'sid-2': s_state}, {'sid-2': q_state}, {})
    critical = [m for m in mismatches if m.severity == 'critical']
    assert any(m.category == 'fsm_state_mismatch' for m in critical), mismatches


def test_compare_execution_views_single_view_gets_presence_warning():
    """sid found in only one view → 'presence' warning, not hard error."""
    mismatches, _stream_suppr, _redis_suppr = mod.compare_execution_views({'sid-lone': {'sid': 'sid-lone'}}, {}, {}, {})
    assert len(mismatches) == 1
    assert mismatches[0].category == 'presence'
    assert mismatches[0].severity == 'warning'


def test_summarise_mismatches_counts_severity():
    """summarise_mismatches correctly aggregates critical vs warning."""
    mismatches = [
        mod.ConsistencyMismatch('a', 'critical', 'sql_missing', 'x'),
        mod.ConsistencyMismatch('b', 'warning', 'stream_missing', 'y'),
        mod.ConsistencyMismatch('c', 'warning', 'fsm_state_mismatch', 'z'),
    ]
    summary = mod.summarise_mismatches(1, 1, 0, mismatches, stream_scan_count=50000)
    assert summary.critical_mismatches == 1
    assert summary.warning_mismatches == 2
    assert summary.mismatches_total == 3
    assert summary.stream_scan_count == 50000


def test_summarise_mismatches_empty():
    """Zero mismatches → all counters are zero."""
    summary = mod.summarise_mismatches(5, 5, 5, [])
    assert summary.mismatches_total == 0
    assert summary.critical_mismatches == 0
    assert summary.warning_mismatches == 0
    assert summary.redis_state_count == 5


def test_stream_missing_suppressed_for_sql_only_terminated_sid():
    """SQL-only terminated SID (no Redis state, no stream event) must NOT generate
    stream_missing warning. These are closed/historical orders whose stream events
    have legitimately fallen outside the XREVRANGE scan window."""
    # SID exists in SQL but NOT in Redis state and NOT in stream
    sql_orders = {
        'sid-closed': {'sid': 'sid-closed', 'symbol': 'SOLUSDT', 'status': 'closed', 'fsm_state': 'CLOSED'}
    }
    redis_state: dict = {}
    stream_latest: dict = {}
    mismatches, _stream_suppr, suppressed = mod.compare_execution_views(redis_state, stream_latest, sql_orders, {})
    categories = {m.category for m in mismatches}
    # Should NOT produce stream_missing — only a presence warning (1 view only)
    assert 'stream_missing' not in categories, (
        f"stream_missing should be suppressed for SQL-only terminated SID, got: {categories}"
    )
    # SID is only in SQL (1 view) → presence warning expected
    assert 'presence' in categories, f"Expected presence warning, got: {categories}"


def test_stream_missing_reported_for_active_sid_with_redis_state():
    """Active SID present in Redis state AND SQL but with no stream event must
    produce stream_missing warning — this is a true anomaly (executor wrote state
    but never published a stream fact)."""
    active_state = {
        'sid': 'sid-active', 'symbol': 'BTCUSDT', 'status': 'open',
        'fsm_state': 'PROTECTED', 'execution_policy': 'SAFETY_FIRST', 'position_side': 'LONG'
    }
    redis_state = {'sid-active': active_state}
    stream_latest: dict = {}  # no stream event — anomaly
    sql_orders = {'sid-active': active_state}
    mismatches, _stream_suppr, suppressed = mod.compare_execution_views(redis_state, stream_latest, sql_orders, {})
    categories = {m.category for m in mismatches}
    assert 'stream_missing' in categories, (
        f"stream_missing must be reported for active SID with Redis state but no stream event, got: {categories}"
    )
    assert suppressed == 0, f"Nothing should be suppressed here, suppressed={suppressed}"


def test_compare_returns_suppressed_count():
    """compare_execution_views correctly counts suppressed stream_missing entries.

    Suppression rule: SID present in BOTH redis_state AND sql_orders but NOT in
    stream → stream_missing REPORTED (active SID is anomaly).
    Suppression only applies when a SID is in sql_orders only (no Redis state) —
    these are terminated orders with events beyond the scan window.
    """
    # Case A: 2 SQL-only terminated SIDs → single-view presence (before stream check)
    sql_orders = {
        'sid-a': {'sid': 'sid-a', 'symbol': 'ETHUSDT', 'status': 'closed', 'fsm_state': 'CLOSED'},
        'sid-b': {'sid': 'sid-b', 'symbol': 'BTCUSDT', 'status': 'closed', 'fsm_state': 'EXITED'},
    }
    _mismatches, _stream_suppr, suppressed = mod.compare_execution_views({}, {}, sql_orders, {})
    # Both are single-view → presence warning, stream check not reached, suppressed=0
    assert suppressed == 0, f"Single-view SIDs skip stream check, suppressed={suppressed}"

    # Case B: SIDs in BOTH redis_state and sql_orders but NOT in stream
    # → stream_missing REPORTED (active SID anomaly), suppressed=0
    redis_state = {k: v for k, v in sql_orders.items()}
    _mismatches2, suppressed2, _redis_suppr2 = mod.compare_execution_views(redis_state, {}, sql_orders, {})
    assert suppressed2 == 0, (
        f"SIDs with active Redis state must report stream_missing, not suppress. suppressed={suppressed2}"
    )
    stream_missing_active = [m for m in _mismatches2 if m.category == 'stream_missing']
    assert len(stream_missing_active) == 2, (
        f"Expected 2 stream_missing for active SIDs, got: {stream_missing_active}"
    )

    # Case C: SID in sql_orders + stream but NOT in redis_state
    # → redis_state_missing is silently COUNTED (3rd return value), NOT a mismatch category.
    #   Completed orders have their Redis state key expired/cleaned up; their stream events
    #   are still within the scan window. This is suppressed noise, not an anomaly.
    stream_latest = {k: v for k, v in sql_orders.items()}
    _mismatches3, suppressed3, redis_suppr3 = mod.compare_execution_views({}, stream_latest, sql_orders, {})
    assert suppressed3 == 0  # no stream_missing suppression (stream events exist for both)
    assert redis_suppr3 == 2, (
        f"Expected redis_state_missing_suppressed=2 (2 SIDs with stream+SQL but no Redis state), "
        f"got redis_suppr3={redis_suppr3}, mismatches={_mismatches3}"
    )


def test_parse_prefix_allowlist_empty_returns_none():
    """Empty string or whitespace → None (no filter)."""
    assert mod._parse_prefix_allowlist('') is None
    assert mod._parse_prefix_allowlist('   ') is None


def test_parse_prefix_allowlist_parses_csv():
    """Comma-separated string is split into a tuple of prefixes."""
    result = mod._parse_prefix_allowlist('crypto:,ord:, trd: ')
    assert result == ('crypto:', 'ord:', 'trd:')


def test_sid_prefix_allowlist_filters_openflow_sids():
    """SIDs that don't match the allowlist are silently skipped.

    Simulates the real-world case: ``crypto-of:SOLUSDT:…`` SIDs appear only
    in the exec stream but never in Redis state or SQL, causing spurious
    ``presence`` warnings.  With an allowlist that excludes them, no warnings
    should be produced.
    """
    # openflow SID only in stream — would cause presence warning without filter
    of_sid = 'crypto-of:SOLUSDT:1772838746355'
    stream_latest = {of_sid: {'sid': of_sid, 'symbol': 'SOLUSDT'}}

    # executor SID present in all three views — no mismatch expected
    exec_sid = 'crypto:SOLUSDT:9999'
    exec_state = {'sid': exec_sid, 'symbol': 'SOLUSDT', 'status': 'open',
                  'fsm_state': 'PROTECTED', 'execution_policy': 'live', 'position_side': 'LONG'}
    redis_state = {exec_sid: exec_state}
    sql_orders = {exec_sid: exec_state}
    stream_latest[exec_sid] = exec_state

    # Without filter → openflow SID triggers presence warning
    mismatches_no_filter, _, _ = mod.compare_execution_views(
        redis_state, stream_latest, sql_orders, {}, sid_prefix_allowlist=None
    )
    assert any(m.sid == of_sid for m in mismatches_no_filter), (
        f"Without filter, presence warning for openflow SID expected: {mismatches_no_filter}"
    )

    # With allowlist → openflow SID silently skipped, only executor SID checked
    mismatches_filtered, _, _ = mod.compare_execution_views(
        redis_state, stream_latest, sql_orders, {}, sid_prefix_allowlist=('crypto:',)
    )
    of_warnings = [m for m in mismatches_filtered if m.sid == of_sid]
    assert of_warnings == [], (
        f"With allowlist={'crypto:'}, openflow SID should be skipped. Got: {of_warnings}"
    )
    # Executor SID should still be checked (and be clean)
    exec_mismatches = [m for m in mismatches_filtered if m.sid == exec_sid]
    assert exec_mismatches == [], (
        f"Executor SID should have no mismatches: {exec_mismatches}"
    )

