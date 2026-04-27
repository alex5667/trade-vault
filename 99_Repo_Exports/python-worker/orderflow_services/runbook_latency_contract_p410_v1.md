# P4.10 — latency-contract dual-control exception approval

P4.10 tightens P4.9 so a separate escalation ticket is no longer sufficient for **long** notifier silence overrides.

## Goal

If a deploy-lint purpose already exceeded the rolling silence budget / re-ack limit and the operator asks for a long suppression window, the override now requires:

1. prepare request by operator A
2. approve request by operator B (`B != A`)
3. final `ack` by operator A with the approved `request_id`

The rollout gate is still not disabled.

## New env

| Variable | Default | Purpose |
|---|---|---|
| `LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_PREFIX` | `cfg:orderflow:latency_contract:deploy_lint:silence_approval` | Redis key prefix for approval request objects |
| `LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_APPROVAL_TTL_S` | `604800` (7 days) | TTL for approval request objects |
| `LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_DUAL_CONTROL_MINUTES` | `480` | Silence minutes threshold triggering dual-control gate |

## Workflow

### Step 1 — Prepare (operator A)

```bash
python latency_contract_deploy_lint_silence_v1.py prepare-override \
  --purpose <purpose> \
  --operator alice \
  --ticket JIRA-001 \
  --escalation-ticket ESC-999 \
  --reason "extended maintenance window" \
  --minutes 600
# Returns: request_id (e.g. abc123...)
```

### Step 2 — Approve (operator B, must differ from A)

```bash
python latency_contract_deploy_lint_silence_v1.py approve-override \
  --request-id abc123... \
  --operator bob \
  --reason "reviewed and confirmed"
```

### Step 3 — Ack/commit (operator A with approved request_id)

```bash
python latency_contract_deploy_lint_silence_v1.py ack \
  --purpose <purpose> \
  --operator alice \
  --ticket JIRA-001 \
  --escalation-ticket ESC-999 \
  --reason "extended maintenance window" \
  --minutes 600 \
  --approval-request-id abc123...
```

## Rules

- Operator B (approver) must be different from operator A (requester).
- `--minutes`, `--escalation-ticket`, `--ticket`, and `--purpose` must match between prepare and ack.
- Approval is single-use and consumed after a successful ack.
- Short silences (below `LATENCY_CONTRACT_DEPLOY_LINT_SILENCE_DUAL_CONTROL_MINUTES`) and non-override acks continue to work as in P4.9.

## Audit events

| Event | Description |
|---|---|
| `latency_deploy_lint_override_approval_prepared` | Operator A created a request |
| `latency_deploy_lint_override_approval_approved` | Operator B approved the request |
| `latency_deploy_lint_override_approval_consumed` | Request used in a successful ack |
| `latency_deploy_lint_ack_silence_dual_control_denied` | Ack rejected: missing or invalid approval |

## Metrics

| Metric | Description |
|---|---|
| `latency_contract_deploy_lint_silence_dual_control_required{purpose}` | Current silence requires dual-control metadata |
| `latency_contract_deploy_lint_silence_dual_control_denied_total{purpose}` | Denials due to missing or invalid approval |
| `latency_contract_deploy_lint_silence_dual_control_override_active{purpose}` | Active silence under approved dual-control exception |
| `latency_contract_deploy_lint_silence_approval_pending{purpose}` | Awaiting second approver |
| `latency_contract_deploy_lint_silence_approval_ready{purpose}` | Approved and ready for ack |
| `latency_contract_deploy_lint_silence_approval_age_seconds{purpose}` | Age of latest approval request |
| `latency_contract_deploy_lint_summary_dual_control_pending_total` | Total purposes with pending approval |
| `latency_contract_deploy_lint_summary_dual_control_ready_total` | Total purposes with ready approval |
| `latency_contract_deploy_lint_summary_dual_control_override_gate_active_total` | Total active gate purposes under dual-control silence |
