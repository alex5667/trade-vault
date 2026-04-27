#!/bin/bash
# XAUUSD Order Flow - Quick Commands Reference v7.1
# Useful commands for daily operations

# ═══════════════════════════════════════════════════════════════
# SYSTEM STATUS
# ═══════════════════════════════════════════════════════════════

# Check all user services
alias xau-status='systemctl --user status xau-atr xau-labeler orders-router orders-http xau-error-monitor'

# Check Redis streams
alias xau-streams='redis-cli XLEN stream:tick_XAUUSD && redis-cli XLEN stream:book_XAUUSD && redis-cli XLEN notify:telegram && redis-cli LLEN orders:queue && redis-cli XLEN orders:exec'

# Check latest signal
alias xau-signal='redis-cli --raw XREVRANGE notify:telegram + - COUNT 1'

# Check latest execution
alias xau-exec='redis-cli --raw XREVRANGE orders:exec + - COUNT 1'

# Check ATR value
alias xau-atr='redis-cli GET atr:val:XAUUSD:1m'

# Check pivots
alias xau-pivots='redis-cli GET pivots:latest'

# ═══════════════════════════════════════════════════════════════
# SERVICE MANAGEMENT
# ═══════════════════════════════════════════════════════════════

# Start all user services
xau-start() {
    systemctl --user start xau-atr xau-labeler orders-router orders-http xau-error-monitor
    echo "✅ All services started"
}

# Stop all user services
xau-stop() {
    systemctl --user stop xau-atr xau-labeler orders-router orders-http xau-error-monitor
    echo "✅ All services stopped"
}

# Restart all user services
xau-restart() {
    systemctl --user restart xau-atr xau-labeler orders-router orders-http xau-error-monitor
    echo "✅ All services restarted"
}

# View logs
xau-logs() {
    journalctl --user -u "$1" -f
}

# ═══════════════════════════════════════════════════════════════
# ANALYTICS
# ═══════════════════════════════════════════════════════════════

# Export features for today
xau-export-features() {
    cd python-worker
    python3 services/export_features.py \
        --start "$(date -d 'today 00:00' -Iseconds)" \
        --end "$(date -Iseconds)" \
        --out "/tmp/features_$(date +%Y-%m-%d).parquet"
    cd ..
}

# Aggregate execution reports for today
xau-aggregate-exec() {
    cd python-worker
    python3 services/aggregate_exec.py \
        --start "$(date -d 'today 00:00' -Iseconds)" \
        --end "$(date -Iseconds)" \
        --out "/tmp/exec_$(date +%Y-%m-%d).parquet"
    cd ..
}

# Export labels with PnL
xau-export-labels-pnl() {
    cd python-worker
    python3 services/export_labels_pnl.py \
        --labels "$1" \
        --features "$2" \
        --exec "$3" \
        --out "${4:-/tmp/joined_pnl.parquet}"
    cd ..
}

# Calibrate thresholds (PnL-based)
xau-calibrate-pnl() {
    cd python-worker
    python3 calibrate/calibrate_thresholds_pnl.py \
        --data "$1" \
        --out-env "${2:-config/calibrated_gold.env}"
    cd ..
}

# Plot distributions
xau-plot() {
    cd python-worker
    python3 reports/plot_distributions.py \
        --data "$1" \
        --outdir "${2:-./reports_out}" \
        --horizon 60
    cd ..
}

# ═══════════════════════════════════════════════════════════════
# TESTING
# ═══════════════════════════════════════════════════════════════

# Run all tests
xau-test() {
    cd python-worker
    pytest tests/ -v
    cd ..
}

# Run specific test module
xau-test-module() {
    cd python-worker
    pytest "tests/test_$1.py" -v
    cd ..
}

# ═══════════════════════════════════════════════════════════════
# MAINTENANCE
# ═══════════════════════════════════════════════════════════════

# Clean old snapshots
xau-clean-snapshots() {
    redis-cli --scan --pattern "signal:snap:*" | xargs -L 1000 redis-cli DEL
    echo "✅ Cleaned signal snapshots"
}

# Clean old exec reports
xau-clean-exec() {
    COUNT=$(redis-cli XTRIM orders:exec MAXLEN ~ 10000)
    echo "✅ Trimmed orders:exec stream, removed $COUNT entries"
}

# Check queue size
xau-queue-size() {
    echo "Orders queue: $(redis-cli LLEN orders:queue)"
    echo "Exec stream: $(redis-cli XLEN orders:exec)"
    echo "Notify stream: $(redis-cli XLEN notify:telegram)"
    echo "Callbacks stream: $(redis-cli XLEN bot:callbacks)"
}

# ═══════════════════════════════════════════════════════════════
# MONITORING
# ═══════════════════════════════════════════════════════════════

# Watch signals in real-time
xau-watch-signals() {
    while true; do
        clear
        echo "═══ LATEST SIGNALS (last 5) ═══"
        redis-cli XREVRANGE notify:telegram + - COUNT 5
        sleep 5
    done
}

# Monitor execution
xau-watch-exec() {
    journalctl --user -u orders-router -u orders-http -f
}

# Monitor errors
xau-watch-errors() {
    journalctl --user -u xau-error-monitor -f
}

# ═══════════════════════════════════════════════════════════════
# DOCKER
# ═══════════════════════════════════════════════════════════════

# Restart python worker
xau-restart-worker() {
    docker-compose restart python-worker
    echo "✅ Python worker restarted"
}

# View worker logs
xau-worker-logs() {
    docker-compose logs -f python-worker
}

# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLES
# ═══════════════════════════════════════════════════════════════

# After sourcing this file:
# source XAUUSD_COMMANDS.sh

# Examples:
# xau-status                    # Check all services
# xau-signal                    # View latest signal
# xau-export-features           # Export today's features
# xau-aggregate-exec            # Aggregate today's executions
# xau-test                      # Run all tests
# xau-logs orders-router        # View router logs
# xau-watch-signals             # Live signal monitoring

echo "✅ XAUUSD commands loaded! Use 'xau-<tab>' for autocomplete"
