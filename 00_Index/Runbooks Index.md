---
type: index
title: Runbooks Index
tags: [index, runbook, operations]
updated_at: 2026-04-18
---

# Runbooks Index

## Data quality
- [[stale-book]]
- [[atr-bad]]
- [[microbar-xlen-low]]

## Control plane / configs
- [[ml-no-cfg]]

## Transport / infra
- [[redis-lag]]
- [[ws-reconnect-storm]]

## When to page
Page only on action signals:
- sustained stale data on critical symbols
- consumer lag breaching SLO with user impact
- execution bridge unavailable
- ML gate stuck in global error / mismatch state
- repeated reconnect storm with data loss risk

## Shared checks
- current symbol scope
- start / end ts_ms
- affected streams
- did quarantine / freeze engage
- did publish / execution continue fail-open or fail-closed
