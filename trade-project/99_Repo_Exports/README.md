> [!IMPORTANT]
> Для завершения настройки Ollama необходимо вручную выполнить `docker exec ollama ollama pull llama3.1`, когда сетевое соединение станет стабильным.

# v10: Path-based Triple-Barrier labeling from ticks/1s-bars

This step produces labels from *price path* after decision time, not from POSITION_CLOSED.
Outputs per sid:
- tb_outcome: TP_HIT | SL_HIT | TIMEOUT | NO_TICKS
- mae_bps, mfe_bps, mae_r, mfe_r, adverse_proxy
- y_edge = 1 if TP_HIT and adverse_proxy <= ADV_MAX else 0

Requires: access to tick/1s-bar series in Redis (or NDJSON capture).

Files:
- python-worker/core/triple_barrier.py
- python-worker/tools/export_ticks_window_ndjson_v1.py
- python-worker/tools/label_triple_barrier_from_ticks_v1.py
- python-worker/tools/build_dataset_from_inputs_outcomes_v4_tb.py
- python-worker/tests/test_triple_barrier.py

Minimal run:
1) export ticks -> /tmp/ticks_24h.ndjson
2) label -> /tmp/tb_labels.ndjson
3) build dataset -> /tmp/ml_dataset_tb.parquet

## Tools

### Tick time autotune

Compute recommended tick time policy knobs from recent tick stream.

- Run: `python -m tools.tick_time_autotune --hours 6`

### OF gate missing leg report

Aggregate which confirmation leg is most often missing (ok=0 and have<need) from `metrics:of_gate`.

- Run: `python -m tools.of_gate_missing_leg_report --hours 24 --top 20`
- Per symbol: `python -m tools.of_gate_missing_leg_report --hours 24 --by-symbol`\n## Cost-aware model policy
Default usage:
- **Gemini Flash + Fast** for bounded/local tasks
- premium model + **Planning** only for architecture, ambiguous RCA, ML/replay redesign, breaking contracts, or multi-subsystem changes

Recommended operator flow:
1. Start with `TRADE:` or a `/trade-fast-*` workflow in Flash/Fast.
2. Escalate to `/trade-pro-*` only if explicit triggers fire.
3. Let Flash produce the first diff/test/checklist where possible, then use premium for review of risky decisions.

Quick routing matrix:

| Task type | Default lane | Mode | Escalate when |
|---|---|---|---|
| Small code fix | Flash | Fast | touches >2 subsystems |
| Contract check | Flash | Fast | breaking change suspected |
| Log triage | Flash | Fast | root cause remains ambiguous |
| Test generation | Flash | Fast | test plan requires cross-service redesign |
| New signal | Flash first | Fast -> Planning | regime / ML / execution redesign |
| Incident RCA | Premium | Planning | production ambiguity or unclear cause |
| Architecture | Premium | Planning | always |
| Schema lifecycle / retention | Premium | Planning | always |


## Cost-aware workflows added
Flash-first workflows:
- `/trade-fast-fix <scope>`
- `/trade-fast-contract-check <scope>`
- `/trade-fast-test-gen <scope>`
- `/trade-fast-log-triage <scope>`
- `/trade-fast-doc-update <scope>`

Premium workflows:
- `/trade-pro-architecture <scope>`
- `/trade-pro-incident <scope>`
- `/trade-pro-rollout-review <scope>`
- `/trade-pro-ml-gate-review <scope>`
- `/trade-pro-schema-change <scope>`\n

## Cost-aware model policy
Default usage:
- **Gemini Flash + Fast** for bounded/local tasks
- premium model + **Planning** only for architecture, ambiguous RCA, ML/replay redesign, breaking contracts, or multi-subsystem changes

Recommended operator flow:
1. Start with `TRADE:` or a `/trade-fast-*` workflow in Flash/Fast.
2. Escalate to `/trade-pro-*` only if explicit triggers fire.
3. Let Flash produce the first diff/test/checklist where possible, then use premium for review of risky decisions.

Quick routing matrix:

| Task type | Default lane | Mode | Escalate when |
|---|---|---|---|
| Small code fix | Flash | Fast | touches >2 subsystems |
| Contract check | Flash | Fast | breaking change suspected |
| Log triage | Flash | Fast | root cause remains ambiguous |
| Test generation | Flash | Fast | test plan requires cross-service redesign |
| New signal | Flash first | Fast -> Planning | regime / ML / execution redesign |
| Incident RCA | Premium | Planning | production ambiguity or unclear cause |
| Architecture | Premium | Planning | always |
| Schema lifecycle / retention | Premium | Planning | always |


## Cost-aware workflows added
Flash-first workflows:
- `/trade-fast-fix <scope>`
- `/trade-fast-contract-check <scope>`
- `/trade-fast-test-gen <scope>`
- `/trade-fast-log-triage <scope>`
- `/trade-fast-doc-update <scope>`

Premium workflows:
- `/trade-pro-architecture <scope>`
- `/trade-pro-incident <scope>`
- `/trade-pro-rollout-review <scope>`
- `/trade-pro-ml-gate-review <scope>`
- `/trade-pro-schema-change <scope>`

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.

### Full CI/CD rollout workflow for notification schema

The repository now includes a governed rollout workflow:

```text
.github/workflows/news-notification-schema-rollout.yml
```

Stages:

```text
observe
-> decision
-> proposal
-> approve
-> apply
-> verify
```

Workflow properties:
- manual trigger via `workflow_dispatch`
- explicit approval gate
- dry-run by default
- apply only when `approved=true`
- artifacts at every stage

Key artifacts:
- `notification-rollout-decision`
- `notification-rollout-proposal`
- `notification-rollout-apply`
- `notification-rollout-verify`

This is the governed rollout pipeline above the proposal/apply tools.
