#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════
# D1: Redis Worker-1 Restart (30 seconds)
# Risk: 🔴 HIGH — all workers affected, cache lost temporarily
# ═══════════════════════════════════════════════════════════
set -euo pipefail
source "$(dirname "$0")/lib_common.sh"

DRILL_ID="D1"
SCENARIO="Redis Worker-1 Restart"
TARGET="redis-worker-1"

start_evidence "${DRILL_ID}" "${SCENARIO}"

# ── Phase 1: Preconditions ──
log_step "Phase 1: Preconditions"
snapshot_containers
check_no_real_positions

# Save consumer group baseline
log_info "Saving consumer group baseline..."
CG_BASELINE=$($PYTHON_EXEC "
import redis
r = redis.Redis(host='redis-worker-1', port=6379, db=0,
    password='fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130',
    username='go_gateway')
for stream in ['orders:exec', 'events:trades', 'notify:telegram']:
    try:
        groups = r.xinfo_groups(stream)
        for g in groups:
            nm = g.get('name', '?')
            pending = g.get('pending', '?')
            print(f'{stream}|{nm}|{pending}')
    except Exception as e:
        print(f'{stream}|ERROR|{e}')
" 2>&1)
echo "${CG_BASELINE}"
evidence "- **Consumer groups pre-drill**:"
evidence "\`\`\`"
evidence "${CG_BASELINE}"
evidence "\`\`\`"

confirm_drill "${DRILL_ID}" "🔴 HIGH"

# ── Phase 2: Trigger ──
log_step "Phase 2: Trigger — restarting ${TARGET}"
TRIGGER_TS=$(date -Iseconds)
evidence ""
evidence "## Trigger"
evidence "- **Time**: ${TRIGGER_TS}"

docker restart "${TARGET}"
evidence "- **Action**: \`docker restart ${TARGET}\`"
log_warn "${TARGET} is restarting"

# ── Phase 3: Observe during restart ──
log_step "Phase 3: Observe — waiting for Redis to come back"
evidence ""
evidence "## Observations"

# Time how long Redis is down
RESTART_START=$(date +%s)

# Wait for Redis to accept connections
REDIS_BACK=false
for i in $(seq 1 30); do
    if docker exec redis-worker-1 redis-cli PING 2>/dev/null | grep -q PONG; then
        RESTART_END=$(date +%s)
        DOWNTIME=$((RESTART_END - RESTART_START))
        log_pass "Redis back online in ${DOWNTIME}s"
        evidence "- **Redis downtime**: ${DOWNTIME}s"
        REDIS_BACK=true
        break
    fi
    sleep 2
done

if [[ "${REDIS_BACK}" != true ]]; then
    log_fail "Redis did NOT come back within 60s"
    evidence "- **Redis downtime**: >60s ❌"
    evidence ""
    evidence "## Verdict"
    evidence "- **Result**: ❌ **FAIL** — Redis did not recover"
    log_info "Evidence saved: ${EVIDENCE_FILE}"
    exit 1
fi

# Wait for workers to reconnect
sleep 15

# Check Go workers (circuit breaker recovery)
log_info "Checking Go worker reconnection..."
GO_HEALTHY=true
for worker in scanner-go-worker-1m scanner-go-worker-5m scanner-go-worker-15m; do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' "$worker" 2>/dev/null || echo "not_found")
    if [[ "${STATUS}" == "healthy" ]]; then
        log_pass "${worker}: healthy"
    else
        log_warn "${worker}: ${STATUS}"
        GO_HEALTHY=false
    fi
done
evidence "- **Go workers reconnected**: ${GO_HEALTHY}"

# Check Python workers
log_info "Checking Python worker reconnection..."
PY_HEALTHY=true
PY_LOGS=$(docker logs scanner-python-worker --since 30s 2>&1 | tail -10)
if docker inspect --format='{{.State.Health.Status}}' scanner-python-worker 2>/dev/null | grep -q healthy; then
    log_pass "Python worker: healthy"
    evidence "- **Python worker reconnected**: ✅"
else
    log_warn "Python worker still recovering"
    PY_HEALTHY=false
    evidence "- **Python worker reconnected**: ⚠️ still recovering"
fi

# Check consumer groups survived
sleep 15
log_info "Checking consumer groups post-restart..."
CG_POST=$($PYTHON_EXEC "
import redis
r = redis.Redis(host='redis-worker-1', port=6379, db=0,
    password='fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130',
    username='go_gateway')
for stream in ['orders:exec', 'events:trades', 'notify:telegram']:
    try:
        groups = r.xinfo_groups(stream)
        for g in groups:
            nm = g.get('name', '?')
            pending = g.get('pending', '?')
            print(f'{stream}|{nm}|{pending}')
    except Exception as e:
        print(f'{stream}|ERROR|{e}')
" 2>&1)
echo "${CG_POST}"
evidence "- **Consumer groups post-drill**:"
evidence "\`\`\`"
evidence "${CG_POST}"
evidence "\`\`\`"

CG_INTACT=true
if echo "${CG_POST}" | grep -q "ERROR"; then
    CG_INTACT=false
fi

# Check signal flow resumes
sleep 30
log_info "Checking signal flow resumed..."
KLINE_FLOW=$($PYTHON_EXEC "
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
print(f'KLINES_FLOWING={count}')
" 2>&1)
echo "${KLINE_FLOW}"
SIGNAL_RESUMED=true
if echo "${KLINE_FLOW}" | grep -q "KLINES_FLOWING=0"; then
    SIGNAL_RESUMED=false
fi
evidence "- **Signal flow resumed**: ${KLINE_FLOW}"

# ── Phase 4: Verdict ──
log_step "Phase 4: Verdict"
evidence ""
evidence "## Verdict"

if [[ "${REDIS_BACK}" == true && "${GO_HEALTHY}" == true && "${CG_INTACT}" == true && "${SIGNAL_RESUMED}" == true ]]; then
    log_pass "══ DRILL ${DRILL_ID} PASSED ══"
    evidence "- **Result**: ✅ **PASS**"
    evidence "  - Redis recovered in ${DOWNTIME}s"
    evidence "  - Go circuit breakers recovered"
    evidence "  - Consumer groups intact"
    evidence "  - Signal flow resumed"
else
    log_fail "══ DRILL ${DRILL_ID} FAILED ══"
    evidence "- **Result**: ❌ **FAIL**"
    evidence "  - redis_back=${REDIS_BACK} (${DOWNTIME:-?>60}s)"
    evidence "  - go_healthy=${GO_HEALTHY}"
    evidence "  - cg_intact=${CG_INTACT}"
    evidence "  - signal_resumed=${SIGNAL_RESUMED}"
fi

log_info "Evidence saved: ${EVIDENCE_FILE}"
