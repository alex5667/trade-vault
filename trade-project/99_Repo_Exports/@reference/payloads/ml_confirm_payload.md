# Metrics:ML_Confirm Payload Example

This payload is published to the `metrics:ml_confirm` Redis stream by the ML confirmation gate.

```json
{
  "ts_ms": "1707597472000",
  "sid": "crypto-of:BTCUSDT:1707597472000",
  "symbol": "BTCUSDT",
  "scenario_v4": "trend_up_v1",
  "bucket": "trend",
  "mode": "ENFORCE",
  "enforce": "1",
  "share_used": "1.0",
  "ok_rule": "1",
  "allow": "1",
  "abstain": "0",
  "status": "ALLOW",
  "p_edge": "0.685",
  "p_min": "0.55",
  "p_min_base": "0.55",
  "p_min_hard_floor": "0.0",
  "p_margin": "0.135",
  "conf": "0.37",
  "model_ver": "v8_stack_prod_001",
  "p_edge_chal": "0.642",
  "chal_ver": "v9_beta_test",
  "missing": "0",
  "err": "",
  "latency_us": "1250",
  "latency_ms": "1.25"
}
```

## Key Fields
- `sid`: Signal ID, used to join with `trades:closed`.
- `p_edge`: Predicted probability of a positive outcome (the "edge").
- `p_min`: The threshold required for an "ALLOW" decision.
- `status`: Final decision outcome (`ALLOW`, `BLOCK`, `ABSTAIN_BAND`, etc.).
- `p_edge_chal`: Prediction from the challenger model (for shadow testing).
- `latency_us`: Execution time for the ML inference and decision logic.
