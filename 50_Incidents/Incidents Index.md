---
type: index
section: incidents
owners:
  - alex
tags:
  - incidents
updated_at: 2026-04-18
---

# Incidents Index

## Open / recurring patterns
- [[RCA-2026-04-18-ml-no-cfg]]
- [[RCA-2026-04-18-time-skew-freeze]]
- [[RCA-2026-04-18-redis-stream-lag]]
- [[RCA-2026-04-18-ws-reconnect-storm]]

## Incident policy
- Every incident should capture exact `start_ts_ms` / `end_ts_ms`
- Use epoch ms in all timelines
- Record impacted symbols, streams, consumers, and user-visible effects
- Separate **facts**, **assumptions**, and **follow-up actions**
- Every RCA should map at least one alert gap and one prevention item

## Cross-links
- [[Service SLOs]]
- [[Data Quality Metrics]]
- [[Redis Stream Health]]
- [[ML Shadow to Enforce]]
