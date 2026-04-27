# News pipeline patch tests (pytest)

These tests validate the contract introduced by the patch:

- Calendar agg uses `event_ts_ms` (or `next_ts_ms`) as source-of-truth; `event_tminus_sec` is legacy/debug only.
- `NewsEnricherSync.attach(..., now_ts_ms=...)` is deterministic (tick-time) and computes `event_tminus_sec` from `event_ts_ms`.
- `NewsGate.decide()` returns unified `GateDecision` (hard block + soft risk factor).

## Prereqs
1) Apply the patch (e.g. `news_patch_v1.zip`) to your repo.
2) Install pytest:

```bash
pip install -U pytest
```

## Run
From repo root:

```bash
pytest -q
```
