---
name: trade-go-redis-ingest
description: Use this skill for Go services that ingest exchange data and publish to Redis in the trade project: websocket consumers, klines/ticks/orderbook ingestion, reconnection logic, heartbeats, backpressure, payload contracts, low-latency publishing, and Go net/http or goroutine/channel design. Relevant for prompts about Go workers, Binance streams, Redis publish/XADD, ping/pong, reconnect, low latency, determinism.
---

# Trade Go Redis Ingest

## Goal
Design or improve Go ingestion services that are low-latency, deterministic, resilient, and observable.

## Use this skill for
- Exchange WebSocket consumers
- Tick/kline/orderbook parsing
- Redis Pub/Sub or Streams publishing
- Reconnect/backoff logic
- Symbol subscription management
- Hot-path performance work in Go

## Design rules
- Keep hot-path allocations low and measurable.
- Do not mix parsing, validation, and publishing in one opaque function.
- Use explicit structs for exchange payloads and internal envelopes.
- Preserve source timestamps and add ingest timestamps separately.
- Make reconnect logic bounded, observable, and jittered.
- Distinguish transient network errors from schema/data errors.

## Preferred architecture
1. Reader loop
2. Decode/validate stage
3. Normalize/envelope stage
4. Publish stage
5. Metrics/logging stage

## Redis contract guidance
- Define channel/stream names explicitly.
- Define payload schema with field names and units.
- Prefer versioned payloads for Streams when contracts may evolve.
- For Streams, define retention/maxlen explicitly and justify it.

## Reliability rules
- Heartbeat/ping watchdog
- Reconnect with exponential backoff + cap + jitter
- Re-subscribe on reconnect
- Idempotent shutdown path
- Dead-letter/quarantine for malformed messages if needed

## Performance workflow
- Measure current p50/p95/p99 latency and allocations
- Change one hotspot at a time
- Re-measure and report before/after

## Tests required
- Unit: payload decode/normalize
- Integration: Redis publish path
- Fault injection: disconnect/reconnect, malformed frames, slow Redis
- Load: burst message handling and lag budget

## Observability
Include at minimum:
- messages_received_total
- messages_published_total
- decode_errors_total
- reconnects_total
- publish_latency_ms or us
- end_to_end_lag_ms
- queue/backpressure depth if channels are used

## Output style
Prefer concrete file diffs, interfaces, config keys, and benchmark guidance.\n

## Default lane
Assume **Gemini Flash + Fast mode** by default for this skill. Escalate only when the triggers below fire.

## Scope rules
- keep work bounded to reader/decoder/publisher/reconnect hot paths
- prefer local fixes and benchmark-backed improvements

## Escalate to premium if
- the change alters cross-service contracts or end-to-end sequencing policy
- root cause spans Go, Redis, and downstream consumers
- a full ingest architecture redesign is needed

## Token discipline
- Read the smallest relevant set of files first.
- Prefer concise diffs, checklists, and test cases over long theory.
- Reuse existing repository patterns before proposing redesign.
- Avoid repository-wide scans unless an escalation trigger fires.

# Language Preferences
**CRITICAL REQUIREMENT:** You must always communicate and respond to the user in Russian (на русском языке), regardless of the language of the prompt or standard instructions. All explanations, plans, and output text must be in Russian.
