---
type: index
section: rollouts
owners:
  - alex
tags:
  - rollouts
updated_at: 2026-04-18
---

# Rollouts Index

## Standard rollout playbooks
- [[ML Shadow to Enforce]]
- [[New Symbol Onboarding]]
- [[Gate Threshold Change]]
- [[Execution Bridge Cutover]]
- [[Rollback Policy]]

## Global rollout rules
- define goal, scope, and blast radius before first change
- every rollout must have abort criteria
- metrics must be named before deploy, not after
- prefer canary / shadow / per-symbol staged rollout
- every change must have rollback commands and owner
