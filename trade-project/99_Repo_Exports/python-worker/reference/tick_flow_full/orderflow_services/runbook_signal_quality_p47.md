# Runbook: Signal Quality KPIs (P47)

**Service**: `scanner-signal-quality-worker` (via `of_timers_worker`)
**Owner**: Trade Team / Quant

## Overview
Calculates 24h rolling signal quality metrics (Precision@Top5%, ECE, Expectancy R) for all closed trades. Use this to monitor model health and rule performance globally.

## Metrics
- `signal_quality_precision_top5p_24h`: Win rate of the top 5% highest scored signals. Target > 50%.
- `signal_quality_ece_24h`: Calibration error. Target < 0.10.
- `signal_quality_expectancy_r_24h`: Mean R-multiple.
- `signal_quality_staleness_sec`: Time since last update.

## Alerts

### SignalQualityStale
**Trigger**: KPI not updated for > 90 minutes.
**Impact**: Blind to recent model degradation.
**Fix**:
1. Check `of_timers_worker` logs: `docker logs scanner-of-timers-worker --tail 100`
2. Look for errors in `run_signal_quality_kpis`.
3. Try running manually:
   ```bash
   make run-python-tool TOOL=tools.signal_quality_kpi_worker_v1 ARGS="--once"
   ```

### SignalQualityLowWinRate
**Trigger**: Top 5% precision < 45%.
**Impact**: High confidence signals are losing money.
**Action**:
1. Check if market regime changed (high volatility?).
2. Check `ML_DIAGNOSTICS` dashboard.
3. Consider enabling `META_ENFORCE_FREEZE=1` if losses persist.

## Configuration
Env vars in `docker-compose-timers.yml`:
- `ENABLE_SIGNAL_QUALITY_KPI_TIMER=1` (Enabled)
- `SIGNAL_QUALITY_KPI_MODULE=tools.signal_quality_kpi_worker_v1`
- `SIGNAL_QUALITY_KPI_TIMEOUT_SEC=900`
