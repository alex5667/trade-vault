# tests/e2e

End-to-end proofs across the control loop: collector→stream→worker→DB→API→UI→approval→adapter→status.

## `run_e2e.py` — Phase 2-3 (collector → worker) ✅ implemented
Runs the real `collector-manual-trends` producer and `worker-trend-rank` consumer
against a Redis at `REDIS_EVENTS_URL`. Verifies: 3 produced → 3 ranked (with scores),
idempotent dedupe on re-run (0 produced), poison message quarantined, DLQ clean.

```bash
make e2e          # spins a throwaway redis on :6390, runs, tears down
# or against running infra (redis-events on :6380):
make up && REDIS_EVENTS_URL=redis://localhost:6380/0 python3 tests/e2e/run_e2e.py
```

Proven path: `social:trend:candidates → worker-trend-rank → social:trend:ranked`,
with quarantine + DLQ wiring from `social_streams`.

Later phases extend this to DB → API → UI → approval → adapter → status.
