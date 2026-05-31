---
type: decision_log
title: Decision Log
tags: [research, decision-log]
updated_at: 2026-04-18
---

# Decision Log

## 2026-04-18 — Keep Redis Streams as reliable transport on critical path
- **Decision:** critical market / signal path remains on Redis Streams + consumer groups.
- **Why:** replayability, lag visibility, pending handling, idempotent processing.
- **Alternatives rejected:** Pub/Sub for critical path.
- **Rollback trigger:** if stream overhead becomes dominant and a proven durable alternative exists.
- **Related ADR:** [[ADR-0001 Redis Streams for reliable trading path]]

## 2026-04-18 — ML changes must go through SHADOW before ENFORCE
- **Decision:** every new model / threshold set goes SHADOW first.
- **Why:** protects production from blind block-rate spikes and schema mismatch surprises.
- **Success metric:** stable missing ratio, latency, and counterfactual allow/block review.
- **Related ADR:** [[ADR-0003 Shadow before Enforce for ML Gate]]

## 2026-04-18 — Storage policy favors Timescale hot / warm / cold separation
- **Decision:** keep recent searchable operational analytics hot, older data aggregated / archived.
- **Why:** predictable query latency and retention control.
- **Related ADR:** [[ADR-0002 Timescale hot warm cold retention]]
