# News pipeline patch tests (pytest)

## Prereqs
- Apply the `news_patch_v1.zip` patch (or later) to your repo.
- Python: `pytest` installed.

## Run
From repo root:

```bash
pytest -q
```

## What is covered
- `NewsGate.decide()` hard-block + soft-factor basics
- `NewsEnricherSync.attach(..., now_ts_ms=...)` deterministic calendar tminus and forex->fx mapping
