#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════
# D3: Binance User Stream Disconnect
# Risk: 🔴 HIGH — execution safety gate test
# ═══════════════════════════════════════════════════════════
set -euo pipefail
source "$(dirname "$0")/lib_common.sh"

DRILL_ID="D3"
SCENARIO="Binance User Stream Disconnect"
TARGET="scanner-binance-user-stream-worker"

start_evidence "${DRILL_ID}" "${SCENARIO}"

# ── Phase 1: Preconditions ──
log_step "Phase 1: Preconditions"
snapshot_containers
check_no_real_positions

# Check EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY setting
BOOTSTRAP_GATE=$($PYTHON_EXEC "
import os
val = os.environ.get('EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY', '?')
print(f'BOOTSTRAP_GATE={val}')
" 2>&1 || echo "BOOTSTRAP_GATE=?")
echo "${BOOTSTRAP_GATE}"
evidence "- **EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY**: ${BOOTSTRAP_GATE}"

if echo "${BOOTSTRAP_GATE}" | grep -q "=0"; then
    log_warn "⚠️  User stream bootstrap gate is DISABLED (=0)."
    log_warn "Executor will NOT block when user stream disconnects."
    log_warn "This drill tests observation only — executor safety NOT enforced."
    evidence ""
    evidence "> ⚠️ **WARNING**: Bootstrap gate disabled. Executor will NOT gate on user stream health."
    evidence "> Set \`EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY=1\` in .env for full safety."
fi

confirm_drill "${DRILL_ID}" "🔴 HIGH"

# ── Phase 2: Trigger ──
log_step "Phase 2: Trigger — restarting ${TARGET}"
TRIGGER_TS=$(date -Iseconds)
evidence ""
evidence "## Trigger"
evidence "- **Time**: ${TRIGGER_TS}"
evidence "- **Method**: Container restart (simulates WebSocket disconnect + reconnect)"

docker restart "${TARGET}"
evidence "- **Action**: \`docker restart ${TARGET}\`"
log_warn "${TARGET} is restarting"

# ── Phase 3: Observe ──
log_step "Phase 3: Observe — watching reconnection"
evidence ""
evidence "## Observations"

# Check user stream health key in Redis
sleep 5
US_STATUS=$($PYTHON_EXEC "
import redis
r = redis.Redis(host='redis-worker-1', port=6379, db=0,
    password='fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130',
    username='go_gateway')
# Check for user stream status keys
for key in ['user_stream:status', 'user_stream:connected', 'exec:bootstrap:user_stream_ready']:
    val = r.get(key)
    if val:
        print(f'{key} = {val.decode()}')
    else:
        # Try hash fields too
        pass
# Check bootstrap health
bh = r.get('exec:bootstrap:health')
if bh:
    print(f'exec:bootstrap:health = {bh.decode()[:200]}')
" 2>&1)
echo "${US_STATUS}"
evidence "- **User stream status during restart**: ${US_STATUS}"

# Monitor reconnection
RECONNECT_START=$(date +%s)
RECONNECTED=false
for i in $(seq 1 30); do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' "${TARGET}" 2>/dev/null || echo "not_found")
    if [[ "${STATUS}" == "healthy" ]]; then
        RECONNECT_END=$(date +%s)
        RECONNECT_TIME=$((RECONNECT_END - RECONNECT_START))
        log_pass "${TARGET} healthy in ${RECONNECT_TIME}s"
        evidence "- **Reconnect time**: ${RECONNECT_TIME}s"
        RECONNECTED=true
        break
    fi
    sleep 2
done

if [[ "${RECONNECTED}" != true ]]; then
    log_fail "${TARGET} did NOT become healthy within 60s"
    evidence "- **Reconnect**: ❌ timeout >60s"
fi

# Check executor behavior
EXEC_STATUS=$(docker inspect --format='{{.State.Health.Status}}' scanner-binance-executor 2>/dev/null || echo "not_found")
evidence "- **Executor status**: ${EXEC_STATUS}"
log_info "Executor status: ${EXEC_STATUS}"

# Check bootstrap health
BOOTSTRAP_STATUS=$(docker inspect --format='{{.State.Health.Status}}' execution-bootstrap-health 2>/dev/null || echo "not_found")
evidence "- **Bootstrap health status**: ${BOOTSTRAP_STATUS}"
log_info "Bootstrap health: ${BOOTSTRAP_STATUS}"

# Check supervised executor
SUPERVISED_STATUS=$(docker inspect --format='{{.State.Health.Status}}' binance-executor-supervised 2>/dev/null || echo "not_found")
evidence "- **Supervised executor status**: ${SUPERVISED_STATUS}"

# ── Phase 4: Post-recovery check ──
log_step "Phase 4: Post-recovery state verification"
evidence ""
evidence "## Post-recovery"

sleep 10
US_POST=$($PYTHON_EXEC "
import redis
r = redis.Redis(host='redis-worker-1', port=6379, db=0,
    password='fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130',
    username='go_gateway')
for key in ['user_stream:status', 'user_stream:connected', 'exec:bootstrap:user_stream_ready']:
    val = r.get(key)
    if val:
        print(f'{key} = {val.decode()}')
bh = r.get('exec:bootstrap:health')
if bh:
    print(f'exec:bootstrap:health = {bh.decode()[:200]}')
" 2>&1)
echo "${US_POST}"
evidence "- **Post-recovery status**: ${US_POST}"

# ── Phase 5: Verdict ──
log_step "Phase 5: Verdict"
evidence ""
evidence "## Verdict"

if [[ "${RECONNECTED}" == true ]]; then
    log_pass "══ DRILL ${DRILL_ID} PASSED ══"
    evidence "- **Result**: ✅ **PASS**"
    evidence "  - User stream reconnected in ${RECONNECT_TIME:-?}s"
    evidence "  - Bootstrap health: ${BOOTSTRAP_STATUS}"
    evidence "  - Executor: ${EXEC_STATUS}"
    if echo "${BOOTSTRAP_GATE}" | grep -q "=0"; then
        evidence ""
        evidence "> ⚠️ **Follow-up**: Enable \`EXEC_BOOTSTRAP_REQUIRE_USER_STREAM_READY=1\` and re-run drill"
    fi
else
    log_fail "══ DRILL ${DRILL_ID} FAILED ══"
    evidence "- **Result**: ❌ **FAIL**"
    evidence "  - User stream did not reconnect"
    evidence "  - Manual restart required: \`docker restart ${TARGET}\`"
fi

log_info "Evidence saved: ${EVIDENCE_FILE}"
