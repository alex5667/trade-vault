---
type: runbook
name: Stale Book
severity: high
service: book / gates
trigger: book_stale veto
tags:
  - runbook
  - data-quality
  - book
updated_at: 2026-04-18
---

# Stale Book

## Symptoms
- repeated `book_stale` veto
- OBI / depth metrics freeze
- execution disabled by gates

## Fast checks
- compare `now_ms - book_ts_ms`
- inspect book_rate_hz
- inspect websocket reconnect counters

## Likely causes
- book stream stalled
- exchange depth feed degraded
- parser stopped updating runtime
- clock skew

## Safe actions
- verify `stream:book_<symbol>` receiving messages
- compare exchange event time vs ingest time
- isolate one symbol before broader restart

## Unsafe actions
- overriding stale gate in production
- using last known book for live execution

## Metrics
- book_age_ms
- book_rate_hz
- ws_reconnects_total
- gate_veto_total{reason="book_stale"}

## Links
- [[stream_book_symbol]]
- [[pre-publish-gates]]
