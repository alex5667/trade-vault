# Redis Streams Contract (Phase 1)

Canonical social:* streams — names, schemas, ownership, consumer groups, retry policy, DLQ/quarantine, retention.

## Transport Envelope (v1)

Every message is a Redis Streams entry with a single field `data` containing a JSON-serialised `EventEnvelope`:

```json
{
  "envelope": "v1",
  "id": "<uuid-hex>",
  "schema": "<schema-name>",
  "occurred_at_ms": 1700000000000,
  "produced_at_ms": 1700000000100,
  "source": "<service>",
  "producer": "<instance>",
  "env": "dev|staging|prod",
  "tenant_id": null,
  "correlation_id": null,
  "causation_id": null,
  "dedupe_key": "<sha1>",
  "trace_id": null,
  "attempts": 0,
  "payload": { ... }
}
```

**Invariants:**
- `envelope` is always `"v1"` — do not increment without versioning the whole transport layer.
- `occurred_at_ms` and `produced_at_ms` are UTC epoch milliseconds.
- `dedupe_key` is set by the producer; consumer does not enforce uniqueness, producer does (via Redis SET NX EX).
- `payload` is validated against `schemas/<schema>.json` by the consumer before dispatch.
- Bad `schema` / `payload` → **quarantine** (not DLQ). Bad time/skew → **quarantine**.
- Drop exception in handler → **DLQ**. Transient error after max_attempts → **DLQ**.

---

## Stream Manifest

| Stream | Schema | Owner | Consumer group(s) | DLQ | Quarantine | maxlen | Retention |
|---|---|---|---|---|---|---|---|
| `social:tiktok:raw` | `social_event.v1` | collectors/go-gateway | `worker-trend-rank` | ✓ | ✓ | 100k | 30d |
| `social:instagram:raw` | `social_event.v1` | collectors/go-gateway | `worker-trend-rank` | ✓ | ✓ | 100k | 30d |
| `social:youtube:raw` | `social_event.v1` | collector-youtube | `worker-trend-rank` | ✓ | ✓ | 100k | 30d |
| `social:ads:raw` | `social_event.v1` | collectors | `worker-trend-rank` | ✓ | ✓ | 100k | 30d |
| `social:owned:raw` | `social_event.v1` | collectors | `worker-trend-rank` | ✓ | ✓ | 100k | 30d |
| `social:trend:candidates` | `trend_candidate.v1` | collector-manual-trends | `worker-trend-rank` | ✓ | ✓ | 100k | 7d |
| `social:trend:ranked` | `trend_candidate.v1` | worker-trend-rank | `worker-content-plan` | ✓ | ✓ | 100k | 7d |
| `social:brief:results` | `content_brief.v1` | worker-content-plan | `worker-policy-critic` | ✓ | ✓ | 100k | 7d |
| `social:policy:results` | `policy_decision.v1` | worker-policy-critic | `api-review`, `worker-publish` | ✓ | ✓ | 100k | 30d |
| `social:review:queue` | `publish_job.v1` | api (human approval) | `worker-publish` | ✓ | ✓ | 100k | 30d |
| `social:publish:requests` | `publish_job.v1` | api/go-gateway | `youtube-publisher`, `tiktok-publisher`, `instagram-publisher` | ✓ | ✓ | 100k | 30d |
| `social:publish:status` | *(raw JSON)* | platform-adapters | `worker-outcome-attribution` | ✓ | ✓ | 100k | 30d |
| `social:outcome:24h` | `outcome_event.v1` | worker-outcome-attribution | `worker-governors`, `worker-replay` | ✓ | ✓ | 100k | 90d |
| `social:dlq` | *(raw)* | social_streams (infra) | operator / replay | ✗ | ✗ | 100k | 30d |
| `social:quarantine` | *(raw + reason)* | social_streams (infra) | operator / replay | ✗ | ✗ | 100k | 30d |

---

## Consumer Group Conventions

- Every consumer creates its group via `XGROUP CREATE ... MKSTREAM` on startup (idempotent, BUSYGROUP ignored).
- **PEL recovery**: `XAUTOCLAIM` with `min_idle_ms=60_000` reclaims stuck/dead consumer entries.
- **Retry limit**: 5 attempts per message (configurable `max_attempts`). After that → DLQ.
- **Attempt tracking**: Redis hash `social:attempts:{stream}:{group}` keyed by message ID; cleared on ACK.
- **Retry backoff**: passive — PEL recovery fires on next poll cycle (60s default idle threshold).

---

## Data Quality Gates (Phase 1)

Applied by `StreamConsumer._process()` before dispatching to the handler:

1. **Envelope parse**: bad JSON or wrong `envelope` version → **quarantine** (`envelope_invalid`)
2. **Schema validation**: unknown schema or payload violations → **quarantine** (`schema_invalid`)
3. **Time quality gate**:
   - `occurred_at_ms <= 0` → quarantine (`bad_occurred_at_ms`)
   - `produced_at_ms <= 0` → quarantine (`bad_produced_at_ms`)
   - `occurred_at_ms > now + 60s` → quarantine (`future_skew`)
   - `occurred_at_ms < now - 7d` → quarantine (`stale_event`)
   - `produced_at_ms < occurred_at_ms` → quarantine (`produced_before_occurred`)

Metric: `social_time_quality_failed_total{stream, reason}`

---

## Replay Policy

- `ReplayRunner` re-produces archived envelopes with `source="replay"`.
- Replay events pass through the full quality gate.
- Replay does **not** hit live platform APIs (publishers check `source` and skip in replay mode — Phase 3+).
- Replay preserves `occurred_at_ms`; updates `produced_at_ms` to replay time.
- Dedupe key is preserved → idempotent re-play into the same stream won't re-produce duplicates.

---

## Adding a New Stream

1. Add stream name to `STREAMS` in `Makefile`.
2. Add a JSON Schema to `schemas/<schema>.v1.json`.
3. Add a row to this manifest.
4. Add contract test in `tests/contract/`.
5. Register consumer group name in the producing service's startup.
