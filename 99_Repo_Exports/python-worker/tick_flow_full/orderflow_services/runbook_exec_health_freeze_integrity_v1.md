# ExecHealth Freeze Integrity (P8)

Purpose: detect control/state deletion, unsigned thaw, or missing signed ack-event.

## Key checks

- `expected_ack_nonce` exists in control/state after autoguard freeze
- Operator thaw must emit signed `manual_ack_thaw` event to `ops:exec_health:freeze_events:v1`
- Control/state disappearance without valid ack-event is a tamper violation
- Thaw recorded in control without a valid HMAC signature is a bypass violation

## Primary metrics / alerts

| Metric | Alert |
|--------|-------|
| `exec_health_freeze_integrity_violation{kind!="none"}` | `OF_ExecHealth_FreezeIntegrity_TamperDetected_Crit` |
| `exec_health_freeze_integrity_pending_ack` | `OF_ExecHealth_FreezeIntegrity_PendingAckStuck_Warn` |
| `exec_health_freeze_integrity_exporter_up == 0` | `OF_ExecHealth_FreezeIntegrity_ExporterStale_Warn` |

## Operator flow

1. `status` — get pending nonce:
   ```
   python orderflow_services/exec_health_freeze_override_v1.py status
   ```
2. `thaw` with nonce:
   ```
   EXEC_HEALTH_ACK_SIGNING_SECRET=<secret> \
   python orderflow_services/exec_health_freeze_override_v1.py thaw \
     --operator <you> --reason "<reason>" --ticket <ticket> \
     --nonce <pending_ack_nonce>
   ```
3. Verify: exporter shows `valid_ack_event_present=1` and `violation{kind="none"}=1`.

## Violation kinds

| Kind | Meaning |
|------|---------|
| `control_missing_pending_ack` | Control hash deleted while ack not yet confirmed |
| `state_missing_pending_ack` | State hash deleted while ack not yet confirmed |
| `control_state_missing_without_valid_ack` | Both hashes gone, trigger event still in stream |
| `thaw_without_valid_ack_event` | Thaw in control/state but no signed ack event found |
| `invalid_ack_event_signature` | Ack event in stream has invalid HMAC |
| `invalid_control_ack_signature` | Thaw in control/state has invalid HMAC |

## ENV

```
EXEC_HEALTH_ACK_SIGNING_SECRET=<strong-secret>
EXEC_HEALTH_FREEZE_EVENT_STREAM=ops:exec_health:freeze_events:v1
EXEC_HEALTH_FREEZE_INTEGRITY_EXPORTER_PORT=9828
EXEC_HEALTH_FREEZE_INTEGRITY_EXPORTER_INTERVAL_S=10
EXEC_HEALTH_FREEZE_INTEGRITY_EVENT_COUNT=100
```
