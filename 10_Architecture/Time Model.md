---
type: architecture
title: Time Model
project: trade
tags: [time, data-quality, determinism]
updated_at: 2026-04-18
---

# Time Model

## Purpose
Сделать время в системе детерминированным, измеримым и безопасным для replay.

## Canonical time rules
- Основная форма времени: **epoch milliseconds**
- Источник event-time: `ts_ms` из market event
- Источник ingest-time: server wall clock at receive time
- Processing-time должен быть отделён от event-time
- В логах и payload допускается отдельный человекочитаемый UTC timestamp, но не вместо `ts_ms`

## Required fields
- `event_ts_ms`
- `ingest_ts_ms`
- `process_ts_ms` when needed
- `age_ms`
- `lag_ms`

## Time failure classes
1. **stale**
   - event сильно старый относительно ingest
2. **future**
   - event из будущего
3. **reordered**
   - допускается малое опоздание в пределах окна reorder
4. **backwards**
   - observed monotonicity breaks
5. **gap**
   - missing intervals / broken continuity

## Policies
### Accept
- tick попадает в допустимое окно skew
- age and skew below configured threshold

### Sanitize
- mark quality low
- exclude from delta / model if policy says so

### Quarantine
- send sample / record to quarantine stream
- increment dedicated metrics

### Freeze
- если несколько bad-time событий подряд по symbol
- stop taking decision-critical actions until recovery streak restored

## Invariants
- никогда не подменять event time незаметно
- любое принудительное исправление времени должно иметь reason code
- no mixed units in one contract: ms and sec cannot coexist silently
- timezone only for display / reporting, not for core matching

## Replay implications
Для replay нужны:
- исходный `event_ts_ms`
- реальный порядок поступления
- ingest or sequence metadata
- информация о quarantine / drops

Без этого reproduction of decision path считается неполным.

## Metrics
- `tick_age_ms`
- `tick_skew_ms`
- `ticks_dropped_total{reason=stale|future|backwards}`
- `time_quarantine_total`
- `symbol_freeze_total{cause=bad_time}`
- `recovery_streak_current`

## Alerts
- stale ratio spike
- future ticks > baseline
- repeated freeze on same symbol
- monotonicity breaks above threshold
- abnormal ingest lag across many symbols

## Linked notes
- [[Data Quality Model]]
- [[python-crypto-orderflow-service]]
