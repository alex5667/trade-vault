---
name: trade-exchange-adapter
description: Use this skill for exchange-specific adapter work in the trade project: venue payload normalization, symbol metadata, sequencing, checksums, contract spec mapping, reconnect quirks, and safe onboarding of new exchange feeds.
---

# Trade Exchange Adapter

## Goal
Design, review, or fix exchange-specific adapters without leaking venue quirks into shared signal, API, or storage layers.

## Default lane
Use claude-haiku-4-5 for bounded fixes in one adapter or one payload shape. Escalate to claude-opus-4-6 for onboarding a new venue, redesigning normalization, or any change that may affect contracts across services.

## Use this skill for
- venue payload normalization
- symbol and contract metadata mapping
- exchange-specific timestamp/sequence semantics
- checksum and orderbook consistency handling
- reconnect/recovery behavior unique to one venue
- safe addition of new market-data sources

## Required output
1. Goal
2. Facts
3. Assumptions
4. Risks
5. Contract and normalization rules
6. Tests
7. Metrics and rollout notes

## Scope rules
- Prefer isolating venue-specific logic inside the adapter layer.
- State canonical normalized fields explicitly.
- Preserve shared downstream contracts unless a reviewed breaking change is intended.
- Always call out exchange-specific edge cases: sequence resets, partial snapshots, symbol remaps, precision, and filters.

## Escalate to claude-sonnet-4-6/opus-4-6 if
- a new venue is being added
- normalized contracts may change downstream
- adapter behavior affects strategy semantics
- multiple services must be updated together

## Token discipline
- Read only the touched adapter, its nearest tests, and the downstream contract definitions first.
- Avoid broad repository scans unless adapter ownership or canonical schema is unclear.
