# P107 — OFInputs exporters smoke-check (runbook)

## What it is
A lightweight periodic smoke-check that hits `/metrics` of the OFInputs exporters and fails with **exit=2** if one or more targets are unavailable or missing expected metric names.

This is meant to catch wiring regressions early:
- exporter container not running / crashloop
- wrong service name / port
- network/compose changes
- exporter serving empty metrics

## Where it runs
Timer worker: `services/of_timers_worker.py` (and `tick_flow_full/services/of_timers_worker.py`).

By default it runs **hourly** (see the scheduling section in the timers worker).

## What happens on failure
- Sends a notification event to the Telegram notifier stream (with dedup/cooldown).
- (Optional, default ON) sets a **fail-closed auto-apply block**:
  - key: `cfg:suggestions:entry_policy:auto_apply_block:of_inputs_exporters_smoke`
  - meta: contains `owner=of_inputs_exporters_smoke`, `rc`, `failed[]`, `sig`, `module`.
  - TTL: `OF_INPUTS_EXPORTERS_SMOKE_BLOCK_TTL_S` (default: `max(3600, OF_INPUTS_EXPORTERS_SMOKE_COOLDOWN_S)`).

On the next successful run (rc=0), the timers worker will clear the block **only if** the meta owner matches `of_inputs_exporters_smoke`.

## Quick triage checklist
1) Confirm which exporters failed:
- Look at the alert payload (`failed=[...]`) or run manually:
  - `python -m orderflow_services.of_inputs_exporters_smoke_p107`

2) Check containers / processes:
- `docker ps | egrep 'of-inputs-.*exporter'`
- `docker logs --tail=200 <container>`

3) Check endpoints from inside the compose network:
- `curl -sS http://of-inputs-v3-circuit-exporter:9164/metrics | head`
- `curl -sS http://of-inputs-dlq-exporter:9158/metrics | head`
- `curl -sS http://of-inputs-archiver-exporter:9156/metrics | head`
- `curl -sS http://of-inputs-dlq-db-exporter:9157/metrics | head`

4) Common root causes
- service renamed in compose but smoke targets not updated
- exporter port changed
- exporter crashed due to missing Redis URL env
- firewall / network policy

## Tuning
- Enable/disable:
  - `ENABLE_OF_INPUTS_EXPORTERS_SMOKE_P107=1|0`
- Timeout:
  - `OF_INPUTS_EXPORTERS_SMOKE_TIMEOUT_S=30`
- Dedup/cooldown:
  - `OF_INPUTS_EXPORTERS_SMOKE_DEDUP=1`
  - `OF_INPUTS_EXPORTERS_SMOKE_COOLDOWN_S=21600`
- Override targets:
  - `OF_INPUTS_EXPORTERS_SMOKE_TARGETS="v3=host:port|metric_substr,dlq=..."`

## Auto-apply block controls
- Toggle:
  - `OF_INPUTS_EXPORTERS_SMOKE_BLOCK_AUTO_APPLY=1|0`
- Reason suffix (Redis key suffix):
  - `OF_INPUTS_EXPORTERS_SMOKE_BLOCK_REASON=of_inputs_exporters_smoke`
- TTL:
  - `OF_INPUTS_EXPORTERS_SMOKE_BLOCK_TTL_S=21600`

Manual clear (if needed):
- Delete the 3 keys (prefix may differ if `AUTO_APPLY_BLOCK_PREFIX` overridden):
  - `DEL cfg:suggestions:entry_policy:auto_apply_block:of_inputs_exporters_smoke`
  - `DEL cfg:suggestions:entry_policy:auto_apply_block:of_inputs_exporters_smoke:meta`
  - `DEL cfg:suggestions:entry_policy:auto_apply_block:of_inputs_exporters_smoke:ts_ms`
