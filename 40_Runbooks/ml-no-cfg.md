---
type: runbook
name: ML No CFG
severity: high
service: ml-confirm-gate
trigger: ERR_NO_CFG / missing model config
tags:
  - runbook
  - ml
  - config
updated_at: 2026-04-18
---

# ML No CFG

## Symptoms
- metrics stream shows missing config / 100% error rate
- status = missing fail-open/fail-closed
- shadow/enforce decisions degrade

## Fast checks
```bash
redis-cli GET cfg:ml_confirm:champion
redis-cli HGETALL cfg:ml_confirm
redis-cli XLEN metrics:ml_confirm
```

## Likely causes
- Redis data loss / key expired
- promotion worker did not write champion cfg
- wrong Redis DB / URL
- schema mismatch vs model artifact

## Safe actions
- confirm champion config exists
- verify model path and schema version alignment
- switch to SHADOW or OFF only if policy allows
- restore config from persistence / backup

## Unsafe actions
- enabling ENFORCE with missing config
- silent fallback without metrics

## Metrics
- ml_confirm_error_rate
- missing_cfg_total
- status_count{status="MISSING_FAILOPEN"}
- metrics_stream_write_errors

## Links
- [[ml-confirm-gate]]
- [[ML Decision]]
