# Runbook: Feature Registry contract / schema pinning (P94)

## What it is

Feature Registry defines *training/runtime feature columns* for edge-stack and related ML components.
P94 adds a **contract smoke-check** that ensures:

- `schema_hash` and `feature_cols_hash` are stable and match **pinned** expected values.
- Any code change that alters the registry outputs triggers alert **unless you explicitly update pins**.

This protects you from silent feature drift caused by accidental edits/reorders.

## Where it lives

- Check tool: `orderflow_services.feature_registry_contract_check_v1`
- Exporter: `orderflow_services.feature_registry_contract_exporter_v1`
- Redis metrics: `metrics:feature_registry_contract:last`
- Redis pins (cfg): `cfg:feature_registry:edge_stack`

## Normal state

- Prometheus: `feature_registry_contract_last_success == 1`
- Redis metrics: `status=ok reason=ok`

## Alert: FeatureRegistryContractFailed

### 1) Read last record

```
redis-cli HGETALL metrics:feature_registry_contract:last
```

Check:
- `reason`
- `pins_present`
- `mismatch_schema_hash`, `mismatch_feature_cols_hash`
- `expected_*` vs current `schema_hash` / `feature_cols_hash`

### 2) Decide: expected change or accidental

**Accidental** (most common):
- someone edited `core/feature_registry.py` without version bump
- changed ordering / filtering / time-onehot / scenario onehot flags

**Expected**:
- you intentionally changed feature set for a schema version

### 3) If accidental: rollback / hotfix

- Revert the commit that changed registry outputs.
- Redeploy.
- Confirm `feature_registry_contract_last_success` returns to 1.

### 4) If expected: bump schema version + update pins

Recommended workflow:

1) Bump schema version (e.g. `v4_of` -> `v5_of`) in Feature Registry (explicit new entry).
2) Deploy.
3) Update pins (one-time) **after** the new version is intentionally adopted:

```
python -m orderflow_services.feature_registry_contract_check_v1 --schema-ver v5_of --seed-pin 1
```

Pins are stored in:

```
redis-cli HGETALL cfg:feature_registry:edge_stack
```

### 5) If pins missing

If `pins_present=0` and `reason=pins_missing`:

- Seed pins once (after verifying the current registry is correct):

```
python -m orderflow_services.feature_registry_contract_check_v1 --seed-pin 1
```

Then ensure the hourly smoke-check runs (timers worker / sre_monitor).

## Stale alert

If `feature_registry_contract_last_age_seconds` is too high:

- Ensure the check is running hourly in `services.of_timers_worker`.
- Ensure Redis is reachable for the worker.

## Notes

- Default policy is **require pins** (fail/alert if pins are missing).
- Hashes are full sha256 hex (64 chars) for deterministic detection.
