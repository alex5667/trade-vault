"""Tests verifying P5 migration assets exist and contain expected DDL.

These are build-time checks — they do NOT require a live DB.
If the migrations are missing or incomplete, the test fails immediately.
"""
from pathlib import Path


_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / 'db' / 'migrations'


def test_p5_migration_19_exists():
    """Migration 19 must add watchdog table and chain columns."""
    sql_path = _MIGRATIONS_DIR / '20260307_19_execution_journal_contract_p5.sql'
    assert sql_path.exists(), f"Migration not found: {sql_path}"
    sql = sql_path.read_text(encoding='utf-8')
    assert 'execution_watchdog_events' in sql, "Must CREATE execution_watchdog_events"
    assert 'closed_trade_id' in sql, "Must ADD closed_trade_id"
    assert 'entry_policy' in sql, "Must ADD entry_policy"
    assert 'signal_id' in sql, "Must ADD signal_id"
    assert 'ADD COLUMN IF NOT EXISTS' in sql, "Should use ADD COLUMN IF NOT EXISTS"


def test_p5_migration_20_indexes_exist():
    """Migration 20 must create chain join indexes."""
    sql_path = _MIGRATIONS_DIR / '20260307_20_execution_journal_contract_p5_indexes.sql'
    assert sql_path.exists(), f"Migration not found: {sql_path}"
    sql = sql_path.read_text(encoding='utf-8')
    assert 'idx_execution_orders_signal_plan' in sql
    assert 'idx_execution_watchdog_sid_ts' in sql
    assert 'IF NOT EXISTS' in sql, "All indexes should use IF NOT EXISTS"
