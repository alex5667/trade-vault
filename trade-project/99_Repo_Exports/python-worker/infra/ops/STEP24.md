# Step 24: Tick gate stream reaper (XAUTOCLAIM safety net)

## Why
The tick-gate aggregator reads results from `ops:tick_quality_gate` using a Redis Stream consumer group.
If a consumer crashes while holding pending entries (PEL), the group can stall.
This reaper periodically auto-claims stale PEL entries and ACKs them (optionally).

## Deploy (docker compose)
1) Apply Step 24 diff.
2) Start with:

```bash
docker compose -f docker-compose-crypto-orderflow.yml -f docker-compose-tick-gate-aggregator.yml -f docker-compose-tick-gate-reaper.yml up -d --build tick-gate-reaper
```

## Prometheus
Add scrape snippet:
`python-worker/infra/observability/prometheus/scrape_tick_gate_reaper.yml`

And alert rules:
`python-worker/infra/observability/tick_gate_reaper_alerts.yml`

## Recommended env
```
TICK_GATE_REDIS_STREAM=ops:tick_quality_gate
TICK_GATE_REDIS_GROUP=tick_gate_agg
TICK_GATE_REAPER_IDLE_MS=300000
TICK_GATE_REAPER_CLAIM_COUNT=200
TICK_GATE_REAPER_INTERVAL_S=15
TICK_GATE_REAPER_ACK_ONLY=1
```

## Notes
- `ACK_ONLY=1` is safe if gate events are append-only audit records and do not need reprocessing.
- If you prefer to reprocess instead of ack: set `ACK_ONLY=0` and let the aggregator consume claimed items.
  (In that case, your aggregator must not assume monotonicity from XREADGROUP.)
