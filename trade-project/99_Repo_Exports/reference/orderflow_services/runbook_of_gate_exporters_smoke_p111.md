# P111 — OF-gate exporters smoke-check (runbook)

## What it is
A lightweight periodic smoke-check that hits `/metrics` of the OF-gate exporters and fails with **exit=2** if one or more targets are unavailable or missing expected metric names.

This is meant to catch wiring regressions early:
- exporter container not running / crashloop
- wrong service name / port
- network/compose changes
- exporter serving empty metrics

## Targets (default)
- `of-gate-archiver-exporter:9152`  (must contain `of_gate_archiver_last_run_ts_ms`)
- `of-gate-dlq-exporter:9154`       (must contain `of_gate_dlq_len`)

Manual run:
- `python -m orderflow_services.of_gate_exporters_smoke_p111`

## Where it runs
Timer worker: `services/of_timers_worker.py` (and `tick_flow_full/services/of_timers_worker.py`).

By default it runs **hourly**.

## What happens on failure
- Sends a notification event to the Telegram notifier stream (with dedup/cooldown).
- (Optional, default ON) sets a **fail-closed auto-apply block**:
  - key prefix: `AUTO_APPLY_BLOCK_PREFIX` (default: `cfg:suggestions:entry_policy:auto_apply_block`)
  - reason suffix: `OF_GATE_EXPORTERS_SMOKE_BLOCK_REASON` (default: `of_gate_exporters_smoke`)
  - keys:
    - `cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke`
    - `cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:meta`
    - `cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:ts_ms`
  - TTL: `OF_GATE_EXPORTERS_SMOKE_BLOCK_TTL_S` (default: `max(3600, OF_GATE_EXPORTERS_SMOKE_COOLDOWN_S)`).

On the next successful run (rc=0), the timers worker clears the block **only if** `meta.owner == of_gate_exporters_smoke` (to avoid clobbering manual blocks).

## Quick triage checklist
1) Confirm which exporters failed:
- Look at the alert payload (`failed=[...]`) or run manually:
  - `python -m orderflow_services.of_gate_exporters_smoke_p111`

2) Check containers / processes:
- `docker ps | egrep 'of-gate-.*exporter'`
- `docker logs --tail=200 <container>`

3) Check endpoints from inside the compose network:
- `curl -sS http://of-gate-archiver-exporter:9152/metrics | head`
- `curl -sS http://of-gate-dlq-exporter:9154/metrics | head`

4) Common root causes
- service renamed in compose but smoke targets not updated
- exporter port changed
- exporter crashed due to missing Redis URL env
- network policy / firewall

## Tuning
Enable/disable:
- `ENABLE_OF_GATE_EXPORTERS_SMOKE_P111=1|0`

Timeout:
- `OF_GATE_EXPORTERS_SMOKE_TIMEOUT_S=30`

Dedup/cooldown:
- `OF_GATE_EXPORTERS_SMOKE_DEDUP=1`
- `OF_GATE_EXPORTERS_SMOKE_COOLDOWN_S=21600`
- `OF_GATE_EXPORTERS_SMOKE_DEDUP_PREFIX=dedup:alert:of_gate_exporters:`

Override targets:
- `OF_GATE_EXPORTERS_SMOKE_TARGETS="archiver=host:port|metric_substr,dlq=..."`

## Manual clear (if needed)
- `DEL cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke`
- `DEL cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:meta`
- `DEL cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:ts_ms`
# P111 — OF-gate exporters smoke-check (runbook)

## What it is
A lightweight periodic smoke-check that hits `/metrics` of the OF-gate exporters and fails with **exit=2** if one or more targets are unavailable or missing expected metric names.

This is meant to catch wiring regressions early:
- exporter container not running / crashloop
- wrong service name / port
- network/compose changes
- exporter serving empty metrics

## Targets (default)
- `of-gate-archiver-exporter:9152`  (must contain `of_gate_archiver_last_run_ts_ms`)
- `of-gate-dlq-exporter:9154`       (must contain `of_gate_dlq_len`)

Manual run:
- `python -m orderflow_services.of_gate_exporters_smoke_p111`

## Where it runs
Timer worker: `services/of_timers_worker.py` (and `tick_flow_full/services/of_timers_worker.py`).

By default it runs **hourly**.

## What happens on failure
- Sends a notification event to the Telegram notifier stream (with dedup/cooldown).
- (Optional, default ON) sets a **fail-closed auto-apply block**:
  - key prefix: `AUTO_APPLY_BLOCK_PREFIX` (default: `cfg:suggestions:entry_policy:auto_apply_block`)
  - reason suffix: `OF_GATE_EXPORTERS_SMOKE_BLOCK_REASON` (default: `of_gate_exporters_smoke`)
  - keys:
    - `cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke`
    - `cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:meta`
    - `cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:ts_ms`
  - TTL: `OF_GATE_EXPORTERS_SMOKE_BLOCK_TTL_S` (default: `max(3600, OF_GATE_EXPORTERS_SMOKE_COOLDOWN_S)`).

On the next successful run (rc=0), the timers worker clears the block **only if** `meta.owner == of_gate_exporters_smoke` (to avoid clobbering manual blocks).

## Quick triage checklist
1) Confirm which exporters failed:
- Look at the alert payload (`failed=[...]`) or run manually:
  - `python -m orderflow_services.of_gate_exporters_smoke_p111`

2) Check containers / processes:
- `docker ps | egrep 'of-gate-.*exporter'`
- `docker logs --tail=200 <container>`

3) Check endpoints from inside the compose network:
- `curl -sS http://of-gate-archiver-exporter:9152/metrics | head`
- `curl -sS http://of-gate-dlq-exporter:9154/metrics | head`

4) Common root causes
- service renamed in compose but smoke targets not updated
- exporter port changed
- exporter crashed due to missing Redis URL env
- network policy / firewall

## Tuning
Enable/disable:
- `ENABLE_OF_GATE_EXPORTERS_SMOKE_P111=1|0`

Timeout:
- `OF_GATE_EXPORTERS_SMOKE_TIMEOUT_S=30`

Dedup/cooldown:
- `OF_GATE_EXPORTERS_SMOKE_DEDUP=1`
- `OF_GATE_EXPORTERS_SMOKE_COOLDOWN_S=21600`
- `OF_GATE_EXPORTERS_SMOKE_DEDUP_PREFIX=dedup:alert:of_gate_exporters:`

Override targets:
- `OF_GATE_EXPORTERS_SMOKE_TARGETS="archiver=host:port|metric_substr,dlq=..."`

## Manual clear (if needed)
- `DEL cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke`
- `DEL cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:meta`
- `DEL cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:ts_ms`
# P111 — OF-gate exporters smoke-check (runbook)

## What it is
A lightweight periodic smoke-check that hits `/metrics` of the OF-gate exporters and fails with **exit=2** if one or more targets are unavailable or missing expected metric names.

This is meant to catch wiring regressions early:
- exporter container not running / crashloop
- wrong service name / port
- network/compose changes
- exporter serving empty metrics

## Targets (default)
- `of-gate-archiver-exporter:9152`  (must contain `of_gate_archiver_last_run_ts_ms`)
- `of-gate-dlq-exporter:9154`       (must contain `of_gate_dlq_len`)

Manual run:
- `python -m orderflow_services.of_gate_exporters_smoke_p111`

## Where it runs
Timer worker: `services/of_timers_worker.py` (and `tick_flow_full/services/of_timers_worker.py`).

By default it runs **hourly**.

## What happens on failure
- Sends a notification event to the Telegram notifier stream (with dedup/cooldown).
- (Optional, default ON) sets a **fail-closed auto-apply block**:
  - key prefix: `AUTO_APPLY_BLOCK_PREFIX` (default: `cfg:suggestions:entry_policy:auto_apply_block`)
  - reason suffix: `OF_GATE_EXPORTERS_SMOKE_BLOCK_REASON` (default: `of_gate_exporters_smoke`)
  - keys:
    - `cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke`
    - `cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:meta`
    - `cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:ts_ms`
  - TTL: `OF_GATE_EXPORTERS_SMOKE_BLOCK_TTL_S` (default: `max(3600, OF_GATE_EXPORTERS_SMOKE_COOLDOWN_S)`).

On the next successful run (rc=0), the timers worker clears the block **only if** `meta.owner == of_gate_exporters_smoke` (to avoid clobbering manual blocks).

## Quick triage checklist
1) Confirm which exporters failed:
- Look at the alert payload (`failed=[...]`) or run manually:
  - `python -m orderflow_services.of_gate_exporters_smoke_p111`

2) Check containers / processes:
- `docker ps | egrep 'of-gate-.*exporter'`
- `docker logs --tail=200 <container>`

3) Check endpoints from inside the compose network:
- `curl -sS http://of-gate-archiver-exporter:9152/metrics | head`
- `curl -sS http://of-gate-dlq-exporter:9154/metrics | head`

4) Common root causes
- service renamed in compose but smoke targets not updated
- exporter port changed
- exporter crashed due to missing Redis URL env
- network policy / firewall

## Tuning
Enable/disable:
- `ENABLE_OF_GATE_EXPORTERS_SMOKE_P111=1|0`

Timeout:
- `OF_GATE_EXPORTERS_SMOKE_TIMEOUT_S=30`

Dedup/cooldown:
- `OF_GATE_EXPORTERS_SMOKE_DEDUP=1`
- `OF_GATE_EXPORTERS_SMOKE_COOLDOWN_S=21600`
- `OF_GATE_EXPORTERS_SMOKE_DEDUP_PREFIX=dedup:alert:of_gate_exporters:`

Override targets:
- `OF_GATE_EXPORTERS_SMOKE_TARGETS="archiver=host:port|metric_substr,dlq=..."`

## Manual clear (if needed)
- `DEL cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke`
- `DEL cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:meta`
- `DEL cfg:suggestions:entry_policy:auto_apply_block:of_gate_exporters_smoke:ts_ms`
