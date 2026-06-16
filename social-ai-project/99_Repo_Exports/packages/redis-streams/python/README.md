# social_streams (Python) — Redis Streams helpers (Epic 3)

Shared producer/consumer helpers for collectors and workers. Stream names live in
`../streams.py` (mirrored in `../streams.ts`).

## Components
| Module | Purpose | Epic ref |
|---|---|---|
| `envelope.py` | `EventEnvelope` v1 — generic transport envelope, dedupe keys | SOC-010 |
| `registry.py` | validate payload against `schemas/<name>.json` (jsonschema; degrades gracefully) | SOC-015 |
| `producer.py` | `StreamProducer` — schema-validated, idempotent XADD with MAXLEN + dedupe | SOC-020 |
| `consumer.py` | `StreamConsumer` — consumer group, ACK, retry, **PEL recovery** (XAUTOCLAIM) | SOC-021/022 |
| `consumer.py` | DLQ writer + quarantine writer (poison-row isolation) | SOC-023/024 |
| `replay.py` | `ReplayRunner` — deterministic replay of a stream range | SOC-025 |
| `metrics.py` | Prometheus stream metrics (optional, no-op if absent) | §18.2 |

## Message flow (consumer)
```
parse envelope ──invalid/schema-bad──▶ quarantine + ACK   (poison isolation)
handler ok                          ──▶ ACK
handler raises Drop                 ──▶ DLQ + ACK          (permanent)
handler raises Retry/other          ──▶ leave pending; PEL recovery re-delivers;
                                        after max_attempts ──▶ DLQ + ACK
```

## Install & test
```bash
pip install -e .[dev]
pytest -q            # 14 unit tests (envelope, dedupe, schema→quarantine, Drop→DLQ, retry→DLQ, PEL recovery, replay)
```

## Usage
```python
import redis
from social_streams import StreamProducer, StreamConsumer, Retry, Drop, make_dedupe_key

r = redis.Redis.from_url("redis://localhost:6380/0", decode_responses=True)

# produce
StreamProducer(r).produce("social:trend:candidates", "trend_candidate.v1", payload,
                          source="manual", dedupe_key=make_dedupe_key(platform, trend_id))

# consume
def handle(env):  # env: EventEnvelope
    ...           # raise Retry(...) for transient, Drop(...) for permanent
StreamConsumer(r, "social:trend:candidates", "trend-rank", "c1", handler=handle).run()
```
