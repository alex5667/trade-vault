#!/bin/bash
# Regime Quantiles Setup and Initial Run
# This script is called by make up to initialize regime quantiles

set -e

echo "📊 Regime Quantiles Setup..."

# Check if we have database connection
ANALYTICS_DSN="${ANALYTICS_DSN:-${POSTGRES_DSN:-postgresql://postgres:postgres@localhost:5432/scanner_analytics}}"

# Run initial quantiles computation
echo "🔢 Computing initial quantiles from historical data..."
cd /home/alex/front/trade/scanner_infra/python-worker

ANALYTICS_DSN="$ANALYTICS_DSN" \
REGIME_BARS_TABLE="${REGIME_BARS_TABLE:-bars_1m}" \
REGIME_Q_LOOKBACK_DAYS="${REGIME_Q_LOOKBACK_DAYS:-30}" \
REGIME_Q_TIMEFRAME="${REGIME_Q_TIMEFRAME:-1m}" \
REGIME_Q_SYMBOLS="${REGIME_Q_SYMBOLS:-BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT}" \
python3 -m tools.update_regime_quantiles_sql || {
    echo "⚠️  Initial quantiles computation failed (may be expected if no historical data yet)"
    exit 0
}

echo "✅ Initial quantiles computed"

# Install systemd timer if running with sudo
if [ "$EUID" -eq 0 ] || sudo -n true 2>/dev/null; then
    echo "🔧 Installing systemd timer for periodic updates..."
    
    cd /home/alex/front/trade/scanner_infra
    
    # Copy systemd files
    sudo cp systemd/regime-quantiles-update.service /etc/systemd/system/ 2>/dev/null || true
    sudo cp systemd/regime-quantiles-update.timer /etc/systemd/system/ 2>/dev/null || true
    
    # Reload and enable
    sudo systemctl daemon-reload 2>/dev/null || true
    sudo systemctl enable regime-quantiles-update.timer 2>/dev/null || true
    sudo systemctl start regime-quantiles-update.timer 2>/dev/null || true
    
    echo "✅ Systemd timer installed and started"
    echo "   Next run: $(systemctl list-timers regime-quantiles-update.timer --no-pager 2>/dev/null | grep regime || echo 'Check with: systemctl list-timers')"
else
    echo "⚠️  Skipping systemd timer installation (requires sudo)"
    echo "   To install manually:"
    echo "   sudo cp systemd/regime-quantiles-update.* /etc/systemd/system/"
    echo "   sudo systemctl daemon-reload"
    echo "   sudo systemctl enable --now regime-quantiles-update.timer"
fi

echo "✅ Regime Quantiles setup complete"
