---
type: index
title: Research Index
tags: [research, experiments, ab-tests, adr]
updated_at: 2026-04-18
---

# Research Index

## Registers
- [[Hypotheses Backlog]]
- [[Experiments Register]]
- [[AB Tests Register]]
- [[Decision Log]]
- [[ADR Index]]

## Research workflow
1. Create hypothesis with success metric and kill criterion
2. Link affected services / streams / configs
3. Run offline replay or shadow experiment first
4. Move to A/B / canary only after replay is stable
5. Record decision and next action

## Hard rules
- no experiment without explicit metric
- no production change without rollback trigger
- no silent schema changes
- all time references in `ts_ms` or explicit UTC dates
- offline replay preferred before live rollout

## Recommended tags
- `hypothesis`
- `experiment`
- `ab-test`
- `decision`
- `adr`
- `shadow`
- `enforce`
- `execution`
- `dq`
