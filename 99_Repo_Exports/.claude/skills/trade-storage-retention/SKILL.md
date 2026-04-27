---
name: trade-storage-retention
description: Use this skill for data-lifecycle decisions in the trade project: Redis stream maxlen and TTL policy, Timescale retention and compression, archives, replay-input lifecycle, and storage budget trade-offs.
---

# Trade Storage Retention

## Goal
Define or review retention, archival, and compression policies that keep hot paths lean while preserving replay, audit, and investigation capability.

## Default lane
Use claude-haiku-4-5 for bounded edits to one retention setting or one dataset policy. Escalate to claude-opus-4-6 when retention affects replay trust, investigations, or multiple storage layers at once.

## Use this skill for
- Redis stream retention policy
- Timescale retention and compression policy
- archive tier definitions
- replay-input lifecycle rules
- storage cost vs recoverability trade-offs
- migration safety for retention changes

## Required output
1. Goal
2. Facts
3. Assumptions
4. Risks
5. Retention matrix
6. Migration / rollback plan
7. Metrics and alerts

## Scope rules
- Separate hot, warm, and archive storage responsibilities.
- State dataset classes explicitly: ticks, books, signals, trades, replay inputs, metrics.
- Always describe impact on replay, RCA, audits, and feature generation.
- Prefer reversible retention changes and staged rollouts.

## Escalate to claude-sonnet-4-6/opus-4-6 if
- retention changes span Redis and Postgres together
- replay or ML datasets may become incomplete
- archive strategy or lifecycle policy is being redesigned
- deletion/compression could affect incident forensics

## Token discipline
- Read only the current retention settings, relevant schema/storage files, and the datasets touched by the request first.
- Avoid full-repository storage discussion unless lifecycle ownership is unclear.
