# Runbook: Enforce Bucket Ops P90

**Dashboard:** `orderflow_services/grafana/enforce_bucket_ops_p90.json`

---

## Alert: OF_EnforceStateExporterStale_Crit

**Conditions:** `(time()*1000 - of_enforce_state_exporter_poll_ts_ms) > 30m`

**Meaning:** The exporter process loop has not emitted a liveness heartbeat for >30 minutes.
All enforce-bucket metrics (residuals, promoter report, block state) are stale and **MUST NOT** be used for operational decisions.

**Steps:**
1. `docker logs scanner-enforce-bucket-state-exporter --tail 100` — check for panic/exception
2. `docker inspect scanner-enforce-bucket-state-exporter | grep Status` — check container state
3. Restart if unhealthy: `docker restart scanner-enforce-bucket-state-exporter`
4. If persistent — check Redis connectivity (`REDIS_URL` env) and report-file path (`/var/lib/trade/of_reports/out/enforce/`)
5. Verify scrape in Prometheus: `up{job="enforce_bucket_state_p77"}`

---

## Alert: OF_AutoApplyBlockedByEnforceBucket_Warn / _Crit

**Conditions:** `of_auto_apply_block_active{source="enforce_bucket_promoter"} > 0` for 10m (warn) / 1h (crit)

**Meaning:** The promoter raised an auto-apply block, halting automated bucket promotion.
`cause` label specifies the block reason (`rollback`, `slo_freeze`, `manual`, `unknown`).

**Steps:**
1. Check promoter status report: `cat /var/lib/trade/of_reports/out/enforce/promoter/enforce_bucket_promoter_status.json | python3 -m json.tool`
2. Check Redis block key: `redis-cli get cfg:suggestions:entry_policy:auto_apply_block:enforce_bucket_promoter`
3. Check rollback status: `of_enforce_promoter_rollback_active` on the dashboard
4. If cause=`rollback`: rollback was triggered due to residual regression — let it cool down or investigate at step 5
5. If cause=`slo_freeze`: an SLO bucket freeze is blocking promotion — check freezer alert below
6. To manually unblock (with team approval): `redis-cli del cfg:suggestions:entry_policy:auto_apply_block:enforce_bucket_promoter`

---

## Alert: OF_EnforceBucketSLOFreezerActive_Warn

**Conditions:** `of_enforce_freezer_block_active > 0` for 15m

**Meaning:** The SLO freezer has detected a degradation event and frozen auto-apply for a specific sym/bucket.

**Steps:**
1. Check freezer status: `cat /var/lib/trade/of_reports/out/enforce/freezer/enforce_bucket_slo_freezer_status.json | python3 -m json.tool`
2. Verify the underlying SLO metric is recovering (check OK rate / exec rate in of_gate dashboard)
3. The freezer auto-clears when the degradation resolves — no manual action usually needed
4. To force unfreeze: stop the freezer container or manually clear the freeze key in Redis (team approval required)

---

## Alert: OF_ExecSlipStatsRefreshStale_Warn / _Crit

**Conditions:** `of_exec_slip_stats_refresh_last_ok_age_sec > 3h (warn) / 6h (crit)`

**Meaning:** The `exec_slip_stats_refresh` job has not successfully refreshed the `mv_exec_slippage_eval_1h_stats` materialized view. DB-based residual validation in the promoter and exporter is degraded.

**Steps:**
1. Check timer logs: `docker logs scanner-of-exec-slip-stats-refresh-timer --tail 100`
2. Check DB connectivity: verify `ANALYTICS_DB_DSN` is reachable from the timer container
3. Manually trigger a refresh if urgent:
   ```bash
   docker exec scanner-of-exec-slip-stats-refresh-timer \
     python3 -m tools.exec_slip_stats_refresh_v1 --once
   ```
4. Check the MV in DB: `SELECT count(*), max(t) FROM mv_exec_slippage_eval_1h_stats;`
5. If MV is missing: re-run the SQL migration to create it

---

## Prod Checklist

- [ ] Exporter running and scraping (`of_enforce_state_exporter_poll_ts_ms` staleness ≈ 0–20s)
- [ ] `ENFORCE_STATE_EXPORTER_SYMBOLS` set (for per-sym coefficient metrics)
- [ ] All 4 alert rule files connected in `prometheus.yml`:
  - `orderflow_services/prometheus_alerts_enforce_bucket_promoter_v1.yml`
  - `orderflow_services/prometheus_alerts_enforce_bucket_promoter_rollback_v1.yml`
  - `orderflow_services/prometheus_alerts_exec_slip_residual_validation_p86.yml`
  - `orderflow_services/prometheus_alerts_enforce_bucket_state_exporter_p90.yml`
- [ ] Grafana dashboard imported (`enforce_bucket_ops_p90.json`, uid=`enforce-bucket-ops-p90`)
