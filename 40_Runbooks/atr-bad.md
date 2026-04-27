---
type: runbook
name: ATR Bad
severity: medium
service: gates / risk
trigger: atr_unavailable or atr_bad alerts
tags:
  - runbook
  - atr
  - risk
updated_at: 2026-04-18
---

# ATR Bad

## Symptoms
- `atr_unavailable`
- wide percentage of `ATR bad`
- stops not generated

## Fast checks
- verify ATR cache/source freshness
- compare current ATR vs recent history
- inspect missing candle gaps

## Likely causes
- HTF candles missing
- bad source selection
- reset after restart without bootstrap
- overly strict sanity thresholds

## Safe actions
- inspect ATR source freshness and fallback chain
- quarantine symbols with bad ATR
- keep execution closed until ATR valid

## Unsafe actions
- trading without stop model
- forcing constant ATR in volatile regime

## Metrics
- atr_age_ms
- atr_bad_pct
- atr_source_selected
- dq_veto_total{reason="atr_unavailable"}

## Links
- [[Time Model]]
- [[Data Quality Model]]
- [[pre-publish-gates]]
