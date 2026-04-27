#!/bin/bash
export ANALYTICS_DB_DSN="postgresql://postgres:12345@localhost:5432/scanner_analytics"
export REPORT_SQL_FILE="$(pwd)/python-worker/tools/trade_diagnostics/sql/trades_window_join_p0.sql"
export REPORT_HOURS=24
export REPORT_MIN_TRADES=0
export REPORT_SEND_TELEGRAM=1
export REPORT_STREAM_FORMAT='payload_json'
export REPORT_REDIS_URL='redis://localhost:6379/0'
echo "Starting python script..."
/usr/bin/python3 -u python-worker/tools/trade_diagnostics/trade_quality_report_v2.py
echo "Python script finished with exit code $?"
