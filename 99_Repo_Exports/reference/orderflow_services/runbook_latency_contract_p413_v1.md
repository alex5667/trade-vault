# P4.13 — latency-contract semantic approval binding

P4.13 extends the P4.12 drift binding from coarse hashes to a richer semantic
fingerprint so even a still-bound approval is invalidated if the semantic nature
of the drift changes after prepare/approve.

## Binding schema version 2 fields (P4.13)

In addition to the P4.12 coarse binding:

- `bound_gate_reason_code` — the gate reason at prepare time
- `bound_errors_count` — the error count at prepare time
- `bound_details_json` — deterministic canonical JSON of deploy-lint details
- `bound_details_fingerprint` — SHA-1 of `bound_details_json`

## Deterministic details_json construction

`details_json` is serialized with `json.dumps(..., sort_keys=True, separators=(',', ':'))`.

Fields included (all sorted/canonicalised before hashing):

- `compose_file`
- `wrapper_file`
- `unit_file`
- `env_file`
- `missing_runtime_env` (sorted CSV)
- `missing_env_file_vars` (sorted CSV)
- `warning_codes` (sorted CSV)
- `warnings_count`

## Operational effect

During final `ack`:

1. Binding schema version is read from the approval request.
2. If `binding_schema_version >= 2`, `gate_reason_code`, `errors_count`, and
   `details_fingerprint` are compared to the current drift snapshot.
3. If any differ, `binding_mismatch_fields` lists them in the invalidation reason,
   e.g. `dual_control_drift_binding_mismatch:gate_reason_code+details_fingerprint`.
4. Approval is transitioned to `invalidated`.
5. Operators must run a fresh prepare/approve cycle.

## Invalidation snapshot fields

The invalidation record stores the current (post-drift) state at invalidation time:

- `invalidated_gate_reason_code`
- `invalidated_errors_count`
- `invalidated_details_json`
- `invalidated_details_fingerprint`

## Primary metrics

- `latency_contract_deploy_lint_silence_approval_binding_match{purpose}`
- `latency_contract_deploy_lint_silence_approval_details_fingerprint_match{purpose}` (P4.13)
- `latency_contract_deploy_lint_silence_approval_binding_schema_version{purpose}` (P4.13)
- `latency_contract_deploy_lint_silence_approval_invalidated{purpose}`
- `latency_contract_deploy_lint_summary_dual_control_binding_mismatch_total`
- `latency_contract_deploy_lint_summary_dual_control_semantic_binding_mismatch_total` (P4.13)
- `latency_contract_deploy_lint_summary_dual_control_invalidated_gate_active_total`

## Recovery path

1. Operator prepares long override request (drift snapshot captured).
2. Second operator approves.
3. Underlying drift changes (`gate_reason_code`, `errors_count`, OR `details_json` fingerprint changed).
4. Requester attempts final `ack` → denied, approval invalidated automatically.
5. Operator repeats prepare/approve on the fresh drift snapshot.
