from pathlib import Path


def test_retention_sql_contains_partition_helpers():
    sql = (
        Path(__file__).resolve().parents[1]
        / 'migrations'
        / '20260306_05_retention_partitioning.sql'
    ).read_text(encoding='utf-8')
    assert 'ensure_monthly_range_partition' in sql
    assert 'purge_execution_hot_tables' in sql
