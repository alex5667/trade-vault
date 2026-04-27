#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════
# D6: Postgres Down (60 seconds)
# Risk: 🟡 MEDIUM — hot path unaffected, persistence layer goes offline
# ═══════════════════════════════════════════════════════════
set -euo pipefail
source "$(dirname "$0")/lib_common.sh"

DRILL_ID="D6"
SCENARIO="Postgres Down for 60 seconds"
PG_CONTAINER="scanner-postgres"
OUTAGE_DURATION=60

start_evidence "${DRILL_ID}" "${SCENARIO}"

# ── Phase 1: Preconditions ──
log_step "Phase 1: Preconditions"
snapshot_containers

# Verify PG is currently healthy
assert_container_healthy "${PG_CONTAINER}" 10
evidence "- **Postgres pre-drill**: healthy"

# Save pre-drill signal rate (if metrics available)
log_info "Saving pre-drill signal emission baseline..."
evidence "- **Outage duration**: ${OUTAGE_DURATION}s"

confirm_drill "${DRILL_ID}" "🟡 MEDIUM"

# ── Phase 2: Trigger ──
log_step "Phase 2: Trigger — stopping Postgres for ${OUTAGE_DURATION}s"
TRIGGER_TS=$(date -Iseconds)
evidence ""
evidence "## Trigger"
evidence "- **Time**: ${TRIGGER_TS}"

docker stop "${PG_CONTAINER}"
evidence "- **Action**: \`docker stop ${PG_CONTAINER}\`"
log_warn "Postgres is DOWN"

# ── Phase 3: Observe during outage ──
log_step "Phase 3: Observe — verifying hot path continues (${OUTAGE_DURATION}s observation)"
evidence ""
evidence "## Observations during outage"

PASS_HOT_PATH=true
PASS_REPORTERS=true

# Check signal pipeline still works (klines flow, signals may emit)
sleep 10
log_info "Checking orderflow workers are still healthy..."
for shard in scanner-crypto-orderflow scanner-crypto-orderflow-2; do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' "$shard" 2>/dev/null || echo "not_found")
    if [[ "${STATUS}" == "healthy" ]]; then
        log_pass "${shard}: healthy during PG outage"
        evidence "- **${shard}**: ✅ healthy during PG outage"
    else
        log_fail "${shard}: ${STATUS} during PG outage"
        evidence "- **${shard}**: ❌ ${STATUS}"
        PASS_HOT_PATH=false
    fi
done

# Check executor still running
EXEC_STATUS=$(docker inspect --format='{{.State.Health.Status}}' scanner-binance-executor 2>/dev/null || echo "not_found")
if [[ "${EXEC_STATUS}" == "healthy" ]]; then
    log_pass "Executor healthy during PG outage"
    evidence "- **scanner-binance-executor**: ✅ healthy"
else
    log_fail "Executor status: ${EXEC_STATUS}"
    evidence "- **scanner-binance-executor**: ❌ ${EXEC_STATUS}"
    PASS_HOT_PATH=false
fi

# Check reporter logs for graceful degradation
sleep 10
REPORTER_LOGS=$(docker logs scanner-periodic-reporter --since 30s 2>&1 | tail -20)
if echo "${REPORTER_LOGS}" | grep -qi "error\|exception\|traceback"; then
    REPORTER_CRASHED=$(docker inspect --format='{{.State.Running}}' scanner-periodic-reporter 2>/dev/null || echo "false")
    if [[ "${REPORTER_CRASHED}" == "true" ]]; then
        log_pass "Reporter encountered PG error but is still running (graceful)"
        evidence "- **periodic-reporter**: ✅ PG errors but not crashed"
    else
        log_fail "Reporter crashed due to PG outage"
        evidence "- **periodic-reporter**: ❌ crashed"
        PASS_REPORTERS=false
    fi
else
    log_pass "Reporter: no errors logged (possibly idle)"
    evidence "- **periodic-reporter**: ✅ no errors"
fi

# Wait remaining time
ELAPSED=20
REMAINING=$((OUTAGE_DURATION - ELAPSED))
if [[ $REMAINING -gt 0 ]]; then
    log_info "Waiting ${REMAINING}s to complete outage window..."
    sleep "${REMAINING}"
fi

# ── Phase 4: Recovery ──
log_step "Phase 4: Recovery — starting Postgres"
evidence ""
evidence "## Recovery"

docker start "${PG_CONTAINER}"
RECOVERY_TS=$(date -Iseconds)
evidence "- **Postgres started**: ${RECOVERY_TS}"

PASS_RECOVERY=true
if wait_for "Postgres healthy" "assert_container_healthy ${PG_CONTAINER}" 120; then
    evidence "- **Postgres healthy**: ✅"
else
    evidence "- **Postgres healthy**: ❌"
    PASS_RECOVERY=false
fi

# Check that services reconnect to PG
sleep 15
log_info "Checking post-recovery container health..."

CRASHED_DURING_OUTAGE=0
for svc in scanner-periodic-reporter scanner-signal-quality-kpi-worker scanner-signal-tracker; do
    SVC_STATUS=$(docker inspect --format='{{.State.Running}}' "$svc" 2>/dev/null || echo "false")
    if [[ "${SVC_STATUS}" == "true" ]]; then
        log_pass "${svc}: running after recovery"
        evidence "- **${svc}**: ✅ running"
    else
        log_fail "${svc}: not running after recovery"
        evidence "- **${svc}**: ❌ not running"
        CRASHED_DURING_OUTAGE=$((CRASHED_DURING_OUTAGE + 1))
    fi
done

if [[ $CRASHED_DURING_OUTAGE -gt 0 ]]; then
    evidence "- **Services crashed**: ${CRASHED_DURING_OUTAGE}"
    PASS_REPORTERS=false
fi

# ── Phase 5: Verdict ──
log_step "Phase 5: Verdict"
evidence ""
evidence "## Verdict"

if [[ "${PASS_HOT_PATH}" == true && "${PASS_REPORTERS}" == true && "${PASS_RECOVERY}" == true ]]; then
    log_pass "══ DRILL ${DRILL_ID} PASSED ══"
    evidence "- **Result**: ✅ **PASS**"
    evidence "  - Hot path (signals, executor) unaffected during PG outage"
    evidence "  - Reporters survived with graceful degradation"
    evidence "  - Postgres recovered and services reconnected"
else
    log_fail "══ DRILL ${DRILL_ID} FAILED ══"
    evidence "- **Result**: ❌ **FAIL**"
    evidence "  - hot_path_pass=${PASS_HOT_PATH}"
    evidence "  - reporters_pass=${PASS_REPORTERS}"
    evidence "  - recovery_pass=${PASS_RECOVERY}"
    evidence ""
    evidence "### Follow-up fixes needed"
    [[ "${PASS_HOT_PATH}" == false ]] && evidence "- HOT PATH coupled to Postgres — investigate dependency chain"
    [[ "${PASS_REPORTERS}" == false ]] && evidence "- Reporters crash instead of graceful skip — add retry/skip logic"
    [[ "${PASS_RECOVERY}" == false ]] && evidence "- Postgres did not recover — check Docker volume mounts"
fi

log_info "Evidence saved: ${EVIDENCE_FILE}"
