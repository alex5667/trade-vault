from pathlib import Path


def test_tca_rollups_sql_contains_hourly_and_daily_rollups():
    sql = Path('services/posttrade/sql/tca_rollups_p6.sql').read_text(encoding='utf-8', errors='replace')
    assert 'tca_fill_metrics_1h_base' in sql
    assert 'mv_tca_fill_metrics_1h_percentiles' in sql
    assert 'mv_tca_fill_metrics_1d_percentiles' in sql
    assert 'percentile_cont(0.95)' in sql
    assert 'realized_spread_1s_neg_share' in sql
