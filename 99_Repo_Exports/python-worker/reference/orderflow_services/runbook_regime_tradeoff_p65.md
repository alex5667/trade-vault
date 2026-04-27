# Runbook: Regime Tradeoff Alerts (P65)

## Overview
This runbook covers alerts related to Decision Regimes (Ok/Warn/Block) and their impact on Signal Quality.
We monitor:
1.  **Regime Share**: usage of different regimes (are we blocking too much?).
2.  **Signal Quality per Regime**: outcome of trades in each regime (is Block actually avoiding bad trades?).
3.  **Data Integrity**: missing regime tags.

## Alerts

### DecisionFinalStale
**Severity**: Critical
**Trigger**: `decision_last_ts_ms` is > 30m old.
**Cause**:
*   `crypto-orderflow-service` is down or not producing decisions.
*   Redis Stream `decisions:final` is empty.
*   Worker `decision_coverage_kpi_worker` is dead.
**Action**:
1.  Check `crypto-orderflow-service` logs: `docker logs scanner-crypto-orderflow-service`.
2.  Check `decision_coverage_kpi_worker` logs.
3.  Restart services: `make up`.

### DecisionRegimeUnknownHigh
**Severity**: Warning
**Trigger**: > 10% of decisions have `regime="unknown"`.
**Cause**:
*   Payload in `decisions:final` missing `dq_state` or `drift_state`.
*   JSON parsing errors.
**Action**:
1.  Inspect `decisions:final` stream in Redis: `XRANGE decisions:final - + COUNT 5`.
2.  Verify `dq_state` and `drift_state` keys exist in payload.
3.  Check if `crypto-orderflow-service` was updated recently (schema change).

### DecisionRegimeBlockShareHigh
**Severity**: Warning
**Trigger**: > 50% of decisions are Blocked.
**Cause**:
*   Market is extremely volatile (Drift Block).
*   Data feed is broken (DQ Block).
*   Gates are too strict.
**Action**:
1.  Check `decision_coverage_exporter` metrics for breakdown: `decision_breakdown_drift_dq_24h_json`.
2.  If DQ failure -> check Binance feed / ingest.
3.  If Drift failure -> check Market Drift Monitor.

### SignalQualityWarnMuchWorseThanOk
**Severity**: Warning
**Trigger**: Warn trades have much lower expectancy than Ok trades.
**Implication**: The "Warn" signals (Drift/DQ warnings) are correctly identifying risky conditions.
**Action**:
1.  Review if we should promote "Warn" to "Block" for automatic trading.
2.  No immediate fix needed, this is informational for tuning.

### SignalQualityBlockBetterThanOk
**Severity**: Warning
**Trigger**: Blocked trades performing BETTER than Ok trades.
**Implication**: Our gates are **incorrectly** filtering good trades.
**Action**:
1.  **Urgent Analysis**: Verify why these trades were blocked.
2.  Check if `drift_state` or `dq_state` false positives are high.
3.  Consider relaxing the blocking gate (move Block -> Warn).

### SignalQualityEceWarnHigh
**Severity**: Warning
**Trigger**: High Calibration Error in Warn regime.
**Implication**: Probability scores are unreliable when in Warn state.
**Action**:
1.  Do not trust `conf` scores in Warn regime.
2.  Retrain calibration (Platt Scaling) specifically for Warn regime data if possible.
