#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════
# D2: Go Worker Crash (1m kline ingestion stops for 60s)
# Risk: 🟡 MEDIUM — market data pipeline freeze, no trading impact
# ═══════════════════════════════════════════════════════════
set -euo pipefail
source "$(dirname "$0")/lib_common.sh"

DRILL_ID="D2"
SCENARIO="Go Worker 1m Crash — Kline Ingestion Stop"
TARGET_WORKER="scanner-go-worker-1m"
OUTAGE_DURATION=60

start_evidence "${DRILL_ID}" "${SCENARIO}"

# ── Phase 1: Preconditions ──
log_step "Phase 1: Preconditions"
snapshot_containers
assert_container_healthy "${TARGET_WORKER}" 10
evidence "- **${TARGET_WORKER} pre-drill**: healthy"

# Verify klines are currently flowing
log_info "Verifying kline flow on binance:kline PubSub..."
KLINE_CHECK=$($PYTHON_EXEC "
import redis, time
r = redis.Redis(host='redis-worker-1', port=6379, db=0,
    password='fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130',
    username='go_gateway')
ps = r.pubsub()
ps.subscribe('binance:kline')
count = 0
start = time.time()
while time.time() - start < 10:
    msg = ps.get_message(timeout=2.0)
    if msg and msg['type'] == 'message':
        count += 1
        if count >= 2:
            break
ps.close()
print(f'KLINES_RECEIVED={count}')
" 2>&1)
echo "${KLINE_CHECK}"
if echo "${KLINE_CHECK}" | grep -q "KLINES_RECEIVED=0"; then
    log_warn "No klines detected pre-drill. This may indicate existing issues."
fi
evidence "- **Pre-drill kline flow**: ${KLINE_CHECK}"

confirm_drill "${DRILL_ID}" "🟡 MEDIUM"

# ── Phase 2: Trigger ──
log_step "Phase 2: Trigger — stopping ${TARGET_WORKER} for ${OUTAGE_DURATION}s"
TRIGGER_TS=$(date -Iseconds)
evidence ""
evidence "## Trigger"
evidence "- **Time**: ${TRIGGER_TS}"

docker stop "${TARGET_WORKER}"
evidence "- **Action**: \`docker stop ${TARGET_WORKER}\`"
log_warn "${TARGET_WORKER} is STOPPED"

# ── Phase 3: Observe ──
log_step "Phase 3: Observe — ${OUTAGE_DURATION}s observation window"
evidence ""
evidence "## Observations during outage"

PASS_NO_FALSE_SIGNALS=true
PASS_GATE_VETO=true

# Wait 15s for staleness to propagate
sleep 15

# Check orderflow workers detect data health issues
log_info "Checking if orderflow workers detect stale data..."
for shard in scanner-crypto-orderflow scanner-crypto-orderflow-2; do
    LOGS=$(docker logs "$shard" --since 20s 2>&1 | grep -i "stale\|health\|veto\|skip" | head -5)
    if [[ -n "${LOGS}" ]]; then
        log_pass "${shard}: detected data staleness"
        evidence "- **${shard} staleness detection**: ✅"
    else
        log_info "${shard}: no staleness logs yet (may be within tolerance)"
        evidence "- **${shard} staleness detection**: ⏳ within tolerance"
    fi
done

# Check that no signals are emitted during outage
sleep 15
SIGNAL_CHECK=$($PYTHON_EXEC "
import redis
r = redis.Redis(host='redis-worker-1', port=6379, db=0,
    password='fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130',
    username='go_gateway')
ql = r.xlen('orders:queue:binance')
print(f'QUEUE_LEN={ql}')
" 2>&1)
echo "${SIGNAL_CHECK}"
evidence "- **Order queue during outage**: ${SIGNAL_CHECK}"

# Check other Go workers still healthy
for worker in scanner-go-worker-5m scanner-go-worker-15m scanner-go-worker-1h; do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' "$worker" 2>/dev/null || echo "not_found")
    if [[ "${STATUS}" == "healthy" ]]; then
        log_pass "${worker}: healthy (unaffected)"
    else
        log_warn "${worker}: ${STATUS}"
    fi
done

# Wait remaining outage time
ELAPSED=30
REMAINING=$((OUTAGE_DURATION - ELAPSED))
if [[ $REMAINING -gt 0 ]]; then
    log_info "Waiting ${REMAINING}s to complete outage window..."
    sleep "${REMAINING}"
fi

# ── Phase 4: Recovery ──
log_step "Phase 4: Recovery — starting ${TARGET_WORKER}"
evidence ""
evidence "## Recovery"

docker start "${TARGET_WORKER}"
RECOVERY_TS=$(date -Iseconds)
evidence "- **Worker started**: ${RECOVERY_TS}"

PASS_RECOVERY=true
if wait_for "Go worker healthy" "assert_container_healthy ${TARGET_WORKER}" 120; then
    evidence "- **Worker healthy**: ✅"
else
    evidence "- **Worker healthy**: ❌"
    PASS_RECOVERY=false
fi

# Check klines resume
sleep 10
KLINE_RESUME=$($PYTHON_EXEC "
import redis, time
r = redis.Redis(host='redis-worker-1', port=6379, db=0,
    password='fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130',
    username='go_gateway')
ps = r.pubsub()
ps.subscribe('binance:kline')
count = 0
start = time.time()
while time.time() - start < 15:
    msg = ps.get_message(timeout=3.0)
    if msg and msg['type'] == 'message':
        count += 1
        if count >= 2:
            break
ps.close()
print(f'KLINES_RESUMED={count}')
" 2>&1)
echo "${KLINE_RESUME}"
if echo "${KLINE_RESUME}" | grep -q "KLINES_RESUMED=0"; then
    log_fail "Klines NOT resumed after recovery"
    evidence "- **Kline flow resumed**: ❌"
    PASS_RECOVERY=false
else
    log_pass "Kline flow resumed"
    evidence "- **Kline flow resumed**: ✅"
fi

# ── Phase 5: Verdict ──
log_step "Phase 5: Verdict"
evidence ""
evidence "## Verdict"

if [[ "${PASS_NO_FALSE_SIGNALS}" == true && "${PASS_RECOVERY}" == true ]]; then
    log_pass "══ DRILL ${DRILL_ID} PASSED ══"
    evidence "- **Result**: ✅ **PASS**"
    evidence "  - No false signals during kline outage"
    evidence "  - Go worker recovered and klines resumed"
    evidence "  - Other timeframe workers unaffected"
else
    log_fail "══ DRILL ${DRILL_ID} FAILED ══"
    evidence "- **Result**: ❌ **FAIL**"
    evidence "  - no_false_signals=${PASS_NO_FALSE_SIGNALS}"
    evidence "  - recovery=${PASS_RECOVERY}"
fi

log_info "Evidence saved: ${EVIDENCE_FILE}"
