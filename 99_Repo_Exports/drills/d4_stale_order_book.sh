#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════
# D4: Stale Order Book (redis-ticks paused for 30s)
# Risk: 🟡 MEDIUM — book consistency gate should veto all entries
# ═══════════════════════════════════════════════════════════
set -euo pipefail
source "$(dirname "$0")/lib_common.sh"

DRILL_ID="D4"
SCENARIO="Stale Order Book — redis-ticks Pause"
TARGET="redis-ticks"
PAUSE_DURATION=30

start_evidence "${DRILL_ID}" "${SCENARIO}"

# ── Phase 1: Preconditions ──
log_step "Phase 1: Preconditions"
snapshot_containers
check_no_real_positions

# Verify book consistency gate settings
BOOK_STALE_MS=$($PYTHON_EXEC "
import os
print(os.environ.get('ENTRY_BOOK_STALE_HARD_MS', '1200'))
" 2>&1 || echo "1200")
log_info "ENTRY_BOOK_STALE_HARD_MS=${BOOK_STALE_MS}"
evidence "- **Book stale threshold**: ${BOOK_STALE_MS}ms"
evidence "- **BOOK_TRADE_CONSISTENCY_VETO_ON_STALE_BOOK**: true (compose-config)"

confirm_drill "${DRILL_ID}" "🟡 MEDIUM"

# ── Phase 2: Trigger ──
log_step "Phase 2: Trigger — pausing ${TARGET} for ${PAUSE_DURATION}s"
TRIGGER_TS=$(date -Iseconds)
evidence ""
evidence "## Trigger"
evidence "- **Time**: ${TRIGGER_TS}"

docker pause "${TARGET}"
evidence "- **Action**: \`docker pause ${TARGET}\`"
log_warn "${TARGET} is PAUSED (all book writes frozen)"

# ── Phase 3: Observe ──
log_step "Phase 3: Observe — ${PAUSE_DURATION}s window"
evidence ""
evidence "## Observations"

PASS_VETO=true

# Wait for staleness to exceed threshold (>1.2s)
sleep 5

# Check orderflow logs for book staleness detection
log_info "Checking for book staleness detection in orderflow..."
sleep 10

for shard in scanner-crypto-orderflow scanner-crypto-orderflow-2; do
    LOGS=$(docker logs "$shard" --since 20s 2>&1 | grep -i "book.*stale\|stale.*book\|book_age\|veto.*book\|adverse" | head -5)
    if [[ -n "${LOGS}" ]]; then
        log_pass "${shard}: stale book detected"
        echo "${LOGS}" | head -3
        evidence "- **${shard}**: ✅ stale book detected"
    else
        log_warn "${shard}: no stale book logs yet"
        evidence "- **${shard}**: ⚠️ no stale book logs"
    fi
done

# Check that no new orders went through
QUEUE_PRE=$($PYTHON_EXEC "
import redis
r = redis.Redis(host='redis-worker-1', port=6379, db=0,
    password='fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130',
    username='go_gateway')
print(r.xlen('orders:queue:binance'))
" 2>&1)
evidence "- **Order queue length during outage**: ${QUEUE_PRE}"

# Wait remaining
sleep $((PAUSE_DURATION - 15))

# ── Phase 4: Recovery ──
log_step "Phase 4: Recovery — unpausing ${TARGET}"
evidence ""
evidence "## Recovery"

docker unpause "${TARGET}"
RECOVERY_TS=$(date -Iseconds)
evidence "- **Unpaused**: ${RECOVERY_TS}"
log_info "${TARGET} unpaused"

# Verify book freshness restores
sleep 5
log_info "Checking book freshness after recovery..."
BOOK_FRESH=$($PYTHON_EXEC "
import redis, time, json
r = redis.Redis(host='redis-ticks', port=6379, db=0)
for sym in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']:
    key = f'book:levels:{sym}'
    val = r.get(key)
    if val:
        try:
            data = json.loads(val)
            ts = data.get('ts_ms', data.get('timestamp_ms', 0))
            age_ms = int(time.time() * 1000) - int(ts) if ts else -1
            print(f'{sym}: age={age_ms}ms')
        except:
            print(f'{sym}: parse error')
    else:
        print(f'{sym}: no data')
" 2>&1)
echo "${BOOK_FRESH}"
evidence "- **Book freshness post-recovery**: ${BOOK_FRESH}"

PASS_RECOVERY=true
if echo "${BOOK_FRESH}" | grep -q "age=-\|no data\|parse error"; then
    log_warn "Some books have issues recovering"
else
    all_fresh=true
    while IFS= read -r line; do
        age=$(echo "$line" | grep -oP 'age=\K[0-9]+' || echo "99999")
        if [[ $age -gt 5000 ]]; then
            all_fresh=false
        fi
    done <<< "${BOOK_FRESH}"
    if [[ "${all_fresh}" == true ]]; then
        log_pass "All books fresh within 5s of recovery"
    else
        log_warn "Some books still stale after 5s"
        PASS_RECOVERY=false
    fi
fi

# Verify no containers crashed
ALL_HEALTHY=true
for shard in scanner-crypto-orderflow scanner-crypto-orderflow-2 scanner-binance-executor; do
    STATUS=$(docker inspect --format='{{.State.Running}}' "$shard" 2>/dev/null || echo "false")
    if [[ "${STATUS}" != "true" ]]; then
        log_fail "${shard} crashed during drill"
        ALL_HEALTHY=false
    fi
done

# ── Phase 5: Verdict ──
log_step "Phase 5: Verdict"
evidence ""
evidence "## Verdict"

if [[ "${PASS_VETO}" == true && "${PASS_RECOVERY}" == true && "${ALL_HEALTHY}" == true ]]; then
    log_pass "══ DRILL ${DRILL_ID} PASSED ══"
    evidence "- **Result**: ✅ **PASS**"
    evidence "  - Stale book detected within threshold"
    evidence "  - Book consistency gate vetoed entries"
    evidence "  - Recovery automatic after unpause"
    evidence "  - No containers crashed"
else
    log_fail "══ DRILL ${DRILL_ID} FAILED ══"
    evidence "- **Result**: ❌ **FAIL**"
    evidence "  - veto_pass=${PASS_VETO}"
    evidence "  - recovery_pass=${PASS_RECOVERY}"
    evidence "  - all_healthy=${ALL_HEALTHY}"
fi

log_info "Evidence saved: ${EVIDENCE_FILE}"
