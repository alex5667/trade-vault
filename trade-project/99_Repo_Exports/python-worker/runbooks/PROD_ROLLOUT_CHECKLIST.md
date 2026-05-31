# Production rollout checklist

## Preconditions
- Canary rollout completed successfully.
- SQL backfill finished and dry-run of repair tooling shows no pending critical fixes.
- Alertmanager routes are live for `trade-execution`, `trade-dq`, `trade-risk`, and `trade-ops`.
- Runbook server is reachable by on-call and links to the latest health report.
- `scripts/check_execution_consistency.py --report-path ...` returns `0` or approved `1`.

## Phase 1 — safety-first only
- Set `EXEC_FORCE_SAFETY_FIRST=1`.
- Set `EXEC_MAKER_TP_ENABLE=0`.
- Enable `TRADE_DQ_HARD_VETO_ENABLE=1`.
- Enable `TRADE_RISK_ENGINE_V2_ENABLE=1`.
- Verify stable metrics for the first observation window.

## Phase 2 — maker enablement for approved symbols
- Set `EXEC_FORCE_SAFETY_FIRST=0`.
- Keep `EXEC_DEGRADED_MODE_DISABLE_MAKER=1`.
- Add only approved Tier A symbols to maker policy.
- Verify watchdog fallback rate and maker fill ratio.

## Phase 3 — full production
- Expand maker symbols only after a reviewed change request.
- Keep degraded-mode force-safety flags documented and ready.
- Keep repair and quarantine tooling available but manual by default.

## Post-rollout verification
- `execution_orders` row count keeps pace with `orders:exec` growth.
- no new `sql_missing` or `fsm_state_mismatch` critical mismatches.
- textfile metrics are scraped and visible in Prometheus/Grafana.
- runbook server serves the latest JSON report and checklist pages.
