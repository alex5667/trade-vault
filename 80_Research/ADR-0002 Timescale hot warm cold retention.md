---
type: adr
adr_id: ADR-0002
title: Timescale hot warm cold retention
status: accepted
date: 2026-04-18
tags: [adr, timescale, retention, analytics]
updated_at: 2026-04-18
---

# ADR-0002 Timescale hot warm cold retention

## Context
Operational analytics needs fast queries for recent incidents and model evaluation, but indefinite raw retention raises storage cost and query variance.

## Decision
Adopt hot / warm / cold strategy:
- **hot:** recent raw operational data and recent trade analytics
- **warm:** compressed historical data plus continuous aggregates
- **cold:** exported archives / snapshots for long-term study

## Consequences
### Positive
- predictable latency for current dashboards
- simpler retention control
- cheaper long-range storage

### Risks
- queries must target correct tier
- replay coverage of very old data requires archive workflow
- continuous aggregate freshness must be monitored

## Required controls
- explicit retention policy per hypertable
- continuous aggregate lag monitoring
- restore / replay docs for cold archive access

## Links
- [[Service SLOs]]
- [[Execution Metrics]]
- [[Metrics Quickstart]]
