# Mirror Sync Policy (Source-of-Truth vs Mirrors)

This repository intentionally contains duplicated ("mirrored") implementation trees.
The duplication exists to support different packaging / deployment footprints while
keeping runtime semantics identical (train==serve, contract checks, observability).

## Rule 0 — Source of Truth (SoT)

**SoT tree:** `tick_flow_full/`.

When you change code under `tick_flow_full/`, you **MUST** apply the exact same
functional change to the corresponding mirror under `services/` (and, when
applicable, to `orderflow_services/`).

If you change only one side, the system can silently diverge:

* **train==serve drift** (feature keys/order, gating behavior, thresholds)
* **replay drift** (golden replays stop matching prod)
* **SRE drift** (metrics names/labels and alert semantics)

## What counts as "functional change"

Any of the following must be mirrored 1:1:

* feature extraction / indicator keys / schema versions
* gating / veto logic / risk policy
* time handling (ts_ms normalization, quarantine logic)
* metrics / alerts / exporters
* Redis keys / streams / config flags
* serialization (NDJSON fields, schema_version, ordering)

Formatting-only changes (whitespace, comments) are allowed, but still recommended
to keep mirrored files visually identical.

## Known mirror pairs

### Runtime processors

* `tick_flow_full/services/orderflow/components/tick_processor.py`
  ↔ `services/orderflow/components/tick_processor.py`
* `tick_flow_full/services/orderflow/components/bar_processor.py`
  ↔ `services/orderflow/components/bar_processor.py`

### Runtime state / configuration

* `tick_flow_full/services/orderflow/runtime.py`
  ↔ `services/orderflow/runtime.py`
* `tick_flow_full/services/orderflow/metrics.py`
  ↔ `services/orderflow/metrics.py`

### Gate / feature wiring

* `tick_flow_full/services/ml_confirm_gate.py`
  ↔ `services/ml_confirm_gate.py`
* `tick_flow_full/orderflow_services/feature_registry_contract_check_v1.py`
  ↔ `orderflow_services/feature_registry_contract_check_v1.py`

## Recommended workflow

1. Make the change in **SoT** (`tick_flow_full/...`).
2. Copy the same change into the **mirror** file(s).
3. Run unit tests + contract checks.
4. If the change affects schemas/features: run a small replay smoke (golden replay)
   and verify train==serve equivalence.
