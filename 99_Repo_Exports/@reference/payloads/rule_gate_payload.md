# Rule-Gate Payload Example (Indicators/Evidence)

This represents the payload of indicators and evidence used for rule-based gating and feature engineering.

```json
{
  "ts_ms": 1707597472000,
  "symbol": "BTCUSDT",
  "direction": "LONG",
  "scenario_v4": "trend_up_v1",
  "indicators": {
    "delta_z": 2.45,
    "ofi_z": 1.82,
    "ofi_stability_score": 0.88,
    "obi": 0.15,
    "obi_z": -0.5,
    "spread_bps": 1.2,
    "expected_slippage_bps": 0.5,
    "exec_risk_norm": 0.12,
    "liq_score": 0.95,
    "book_staleness_ms": 45,
    "pressure": 0.67,
    "triggers_per_min": 12,
    "rule_score": 0.9,
    "rule_have": 4,
    "rule_need": 3,
    "sweep_recent": true,
    "sweep_age_ms": 1500,
    "sweep_kind": "EQH",
    "reclaim_recent": true,
    "reclaim_age_ms": 5000,
    "reclaim_level": 42500.5,
    "absorption_volume": 12.5,
    "fp_edge_absorb": 1,
    "cancel_spike_veto": 0
  }
}
```

## Key Fields
- `delta_z`, `ofi_z`, `obi_z`: Z-scores for volume delta, order flow imbalance, and order book imbalance.
- `sweep_recent`: Boolean indicating if a liquidity sweep was detected within the valid window.
- `reclaim_recent`: Boolean indicating if a level reclaim happened recently for the same direction.
- `rule_score`: Normalized score from the active rule-set.
- `exec_risk_norm`: Normalized execution risk metric.
