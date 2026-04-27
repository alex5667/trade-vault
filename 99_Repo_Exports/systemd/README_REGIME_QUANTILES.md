# Regime Quantiles System - Deployment Guide

## Overview
Автоматическая система обновления quantiles для ADX/ATR%, используемых в классификации рыночных режимов (trend/range/thin).

## Components

### 1. SQL Computation Tool
**File**: `python-worker/tools/update_regime_quantiles_sql.py`

Вычисляет quantiles из исторических баров (bars_1m) используя SQL `percentile_cont`:
- ADX: p40, p60, p75
- ATR%: p25, p50, p75

### 2. In-Memory Store
**File**: `python-worker/core/regime_quantiles_store.py`

Кеширует quantiles в памяти с 3-точечной аппроксимацией для быстрого определения quantile position.

### 3. Systemd Automation
**Files**: 
- `systemd/regime-quantiles-update.service`
- `systemd/regime-quantiles-update.timer`

Автоматический запуск каждые 6 часов.

## Installation

### 1. Install systemd files
```bash
sudo cp systemd/regime-quantiles-update.service /etc/systemd/system/
sudo cp systemd/regime-quantiles-update.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

### 2. Configure environment
Edit `/etc/systemd/system/regime-quantiles-update.service` if needed:
```ini
Environment=ANALYTICS_DSN=postgresql://user:pass@host:5432/db
Environment=REGIME_BARS_TABLE=bars_1m
Environment=REGIME_Q_LOOKBACK_DAYS=30
Environment=REGIME_Q_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
```

### 3. Enable and start
```bash
sudo systemctl enable --now regime-quantiles-update.timer
sudo systemctl list-timers | grep regime-quantiles
```

### 4. Manual run (for testing)
```bash
cd /home/alex/front/trade/scanner_infra/python-worker
ANALYTICS_DSN=postgresql://... python3 -m tools.update_regime_quantiles_sql
```

## Verification

### Check timer status
```bash
sudo systemctl status regime-quantiles-update.timer
```

### Check last run
```bash
sudo journalctl -u regime-quantiles-update.service -n 50
```

### Verify DB data
```sql
SELECT symbol, timeframe, atrp_p50, adx_p60, "sampleSize", "updatedAt"
FROM regime_quantiles
ORDER BY "updatedAt" DESC
LIMIT 10;
```

## Integration with Data Processor

The `data_processor.py` will:
1. Load quantiles via `RegimeQuantilesStore`
2. Compute real-time `atr_q` using `approx_quantile_3pt()`
3. Publish `regime:{symbol}` to Redis with TTL for tick-centric services

This enables:
- Consistent regime classification across services
- Quantile-aware contextual strictness (burst/strong_need/AB split)
- Deterministic regime visibility for crypto_orderflow_service and SMT

## Troubleshooting

### No data in regime_quantiles
- Check that `bars_1m` table has `adx14` and `atrp14` columns
- Verify lookback period has sufficient data
- Check SYMBOLS list matches available data

### Timer not running
```bash
sudo systemctl status regime-quantiles-update.timer
sudo systemctl restart regime-quantiles-update.timer
```

### Permission errors
Ensure python worker has read access to systemd environment and DB credentials.
