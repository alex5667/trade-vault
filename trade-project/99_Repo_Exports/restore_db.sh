#!/bin/bash
set -e

# Configuration
POSTGRES_CONTAINER="scanner-postgres"
PSQL_CMD="docker exec -i $POSTGRES_CONTAINER psql -U trading"

echo "🔄 Starting Database Restoration..."

# Helper function to pipe SQL file to psql
apply_sql() {
    local db=$1
    local file=$2
    echo "📄 Applying $file to $db..."
    if [ -f "$file" ]; then
        cat "$file" | $PSQL_CMD -d "$db" > /dev/null
    else
        echo "⚠️ Warning: File $file not found!"
    fi
}

# 1. Restore 'trade' database
echo "📊 Restoring 'trade' database..."
apply_sql "trade" "python-worker/migrations/004_create_signal_execution_tables.sql"
apply_sql "trade" "python-worker/migrations/001_add_local_calibration.sql"
apply_sql "trade" "python-worker/migrations/002_add_signal_metrics.sql"
apply_sql "trade" "python-worker/migrations/003_add_signal_quality_tables.sql"
apply_sql "trade" "python-worker/migrations/005_create_experiment_tables.sql"

# 2. Restore 'scanner_analytics' database
echo "📈 Restoring 'scanner_analytics' database..."
apply_sql "scanner_analytics" "python-worker/migrations/006_create_scanner_analytics_tables.sql"
apply_sql "scanner_analytics" "python-worker/migrations/007_create_regime_quantiles_table.sql"
apply_sql "scanner_analytics" "python-worker/migrations/008_add_health_columns_to_trades.sql"
apply_sql "scanner_analytics" "python-worker/migrations/009_add_p90_to_regime_quantiles.sql"
apply_sql "scanner_analytics" "python-worker/migrations/010_refactor_regime_quantiles.sql"
apply_sql "scanner_analytics" "python-worker/migrations/011_update_regime_quantiles_index.sql"
apply_sql "scanner_analytics" "python-worker/regime/signal_family_baseline_schema.sql"
apply_sql "scanner_analytics" "python-worker/migrations/012_finalize_regime_quantiles_schema.sql"
apply_sql "scanner_analytics" "python-worker/migrations/013_fix_regime_and_baseline_schema.sql"
apply_sql "scanner_analytics" "python-worker/migrations/014_create_regime_snapshot.sql"

echo "✅ Database Restoration Completed Successfully!"
