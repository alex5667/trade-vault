#!/usr/bin/env bash
# Common library for failure drills
set -euo pipefail

DRILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EVIDENCE_DIR="${DRILL_DIR}/evidence"
mkdir -p "${EVIDENCE_DIR}"

REDIS_CLI="docker exec redis-worker-1 redis-cli"
PYTHON_EXEC="docker exec scanner-python-worker python3 -c"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()  { echo -e "${BLUE}[INFO]${NC} $(date +%H:%M:%S) $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $(date +%H:%M:%S) $*"; }
log_pass()  { echo -e "${GREEN}[PASS]${NC} $(date +%H:%M:%S) $*"; }
log_fail()  { echo -e "${RED}[FAIL]${NC} $(date +%H:%M:%S) $*"; }
log_step()  { echo -e "${BLUE}[STEP]${NC} $(date +%H:%M:%S) === $* ==="; }

# Start evidence file
start_evidence() {
    local drill_id="$1"
    local scenario="$2"
    EVIDENCE_FILE="${EVIDENCE_DIR}/${drill_id}_$(date +%Y%m%d_%H%M%S).md"
    {
        echo "# Drill Evidence: ${drill_id} — ${scenario}"
        echo ""
        echo "- **Date/Time**: $(date -Iseconds)"
        echo "- **Operator**: $(whoami)"
        echo ""
    } > "${EVIDENCE_FILE}"
    log_info "Evidence file: ${EVIDENCE_FILE}"
}

# Append to evidence
evidence() {
    echo "$*" >> "${EVIDENCE_FILE}"
}

# Check no active exchange positions (virtual TP_POLICY_ARMED with release_reason=repair_worker_flat_no_orders are OK)
check_no_real_positions() {
    log_step "Precondition: checking for real open positions"
    local result
    result=$(docker exec scanner-python-worker python3 -c "
import redis, json
r = redis.Redis(host='redis-worker-1', port=6379, db=0,
    password='fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130',
    username='go_gateway')
active = list(r.scan_iter('orders:active_symbol_sid:*', count=100))
real_count = 0
for k in active:
    val = r.get(k)
    if val:
        data = json.loads(val)
        # Skip released/flat positions
        if data.get('guard_status') == 'released' and 'flat' in data.get('release_reason', ''):
            continue
        real_count += 1
        print(f'REAL: {k.decode()} state={data.get(\"state\")} side={data.get(\"side\")}')
print(f'REAL_COUNT={real_count}')
" 2>&1)
    echo "${result}"
    local count
    count=$(echo "${result}" | grep "REAL_COUNT=" | sed 's/REAL_COUNT=//')
    if [[ "${count}" -gt 0 ]]; then
        log_fail "Found ${count} real open positions. ABORTING drill."
        evidence "- **Precondition**: FAILED — ${count} real open positions"
        exit 1
    fi
    log_pass "No real open positions detected"
    evidence "- **Precondition**: PASSED — no real open positions"
}

# Snapshot running containers
snapshot_containers() {
    log_step "Saving container snapshot"
    local snap
    snap=$(docker ps --format "{{.Names}}\t{{.Status}}" 2>/dev/null | sort)
    local healthy_count
    healthy_count=$(echo "${snap}" | grep -c "healthy" || true)
    local total_count
    total_count=$(echo "${snap}" | wc -l)
    log_info "Containers: ${total_count} total, ${healthy_count} healthy"
    evidence "- **Containers pre-drill**: ${total_count} total, ${healthy_count} healthy"
}

# Check a container is healthy
assert_container_healthy() {
    local name="$1"
    local timeout="${2:-60}"
    local elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        local status
        status=$(docker inspect --format='{{.State.Health.Status}}' "$name" 2>/dev/null || echo "not_found")
        if [[ "$status" == "healthy" ]]; then
            log_pass "${name} is healthy (${elapsed}s)"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    log_fail "${name} NOT healthy after ${timeout}s (status=${status})"
    return 1
}

# Confirm drill execution with user
confirm_drill() {
    local drill_id="$1"
    local risk="$2"
    echo ""
    echo -e "${YELLOW}╔══════════════════════════════════════════════════════╗${NC}"
    echo -e "${YELLOW}║  DRILL: ${drill_id}  RISK: ${risk}                         ║${NC}"
    echo -e "${YELLOW}╚══════════════════════════════════════════════════════╝${NC}"
    echo ""
    if [[ "${AUTO_EXECUTE:-0}" == "1" ]]; then
        log_info "Auto-executing drill (AUTO_EXECUTE=1)"
        confirm="EXECUTE"
    else
        read -r -p "Type 'EXECUTE' to proceed: " confirm
    fi
    if [[ "${confirm}" != "EXECUTE" ]]; then
        log_warn "Drill cancelled by operator"
        evidence "- **Result**: CANCELLED by operator"
        exit 0
    fi
}

# Wait for condition with timeout
wait_for() {
    local description="$1"
    local check_cmd="$2"
    local timeout="${3:-60}"
    local elapsed=0
    log_info "Waiting for: ${description} (timeout: ${timeout}s)"
    while [[ $elapsed -lt $timeout ]]; do
        if eval "${check_cmd}" > /dev/null 2>&1; then
            log_pass "${description} — achieved in ${elapsed}s"
            evidence "- **${description}**: ${elapsed}s"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    log_fail "${description} — timeout after ${timeout}s"
    evidence "- **${description}**: TIMEOUT (${timeout}s)"
    return 1
}
