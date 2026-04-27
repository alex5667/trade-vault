# P5/Px Operator Hardening

## Added operator hardening controls

- unified Grafana provisioning update for unified ops and risk-drift drilldown dashboards
- Alertmanager route split for `domain=risk-drift` → dedicated `trade-risk-drift` receiver
- auto-silence helper (`auto_silence_risk_drift_storm.py`) for repeated mismatch storms
- SQL consistency check (`check_risk_mismatch_summary_archive_consistency.py`) between `risk_mismatch_summary_mv` and `risk_mismatch_quarantine_ledger_archive`
- retention freshness/textfile export for archive purge (`purge_risk_mismatch_hot_tables.py`)
- operator score hardening: archive consistency mismatch penalises merged score (up to -10 pts)

## Recommended rollout

1. Enable new Alertmanager route split first.
2. Run archive consistency checker in dry observation mode.
3. Keep auto-silence helper in `DRY_RUN=1` for at least several alert cycles.
4. After validation, wire systemd timers and compose helpers.

## ENV variables added

```
ALERTMANAGER_EXTERNAL_URL
RISK_DRIFT_AUTOSILENCE_DRY_RUN
RISK_DRIFT_AUTOSILENCE_QUARANTINE_THRESHOLD
RISK_DRIFT_AUTOSILENCE_MISMATCH_RATE_THRESHOLD
RISK_DRIFT_AUTOSILENCE_DURATION_SEC
RISK_DRIFT_AUTOSILENCE_REPORT_PATH
RISK_MISMATCH_ARCHIVE_CONSISTENCY_TEXTFILE_PATH
RISK_MISMATCH_RETENTION_TEXTFILE_PATH
```

## New API endpoints (runbook server)

- `GET /api/risk-drift-autosilence/latest` → `latest_risk_drift_autosilence.json`
- `GET /api/risk-mismatch-archive-consistency/latest` → `latest_risk_mismatch_archive_consistency.json`

## New Prometheus alerts

- `TradeRiskMismatchArchiveConsistencyStale` – checker report stale
- `TradeRiskMismatchArchiveConsistencyMismatch` – MV diverges from hot+archive
- `TradeRiskMismatchRetentionStale` – purge pipeline stale

## Timer schedule

| Service | Interval |
|---------|---------|
| `trade-risk-drift-autosilence` | every 20 min |
| `trade-risk-mismatch-archive-consistency` | every 30 min |
| `trade-risk-mismatch-retention` | daily (via existing purge timer) |
