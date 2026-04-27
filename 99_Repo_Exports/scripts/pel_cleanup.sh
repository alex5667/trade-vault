#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/pel_cleanup.sh
#
# Purpose: Remove zombie consumers (idle > IDLE_THRESHOLD_MS) from Redis Stream
#          consumer groups to prevent WorkerLagP99High after make up/restart.
#
# Problem: Each container restart creates a new consumer_id (PID-based).
#          Old consumers accumulate a PEL (Pending Entry List) of unACKed ticks.
#          _pel_sweeper_loop XAUTOCLAIM reclaims these → worker receives ticks
#          with event_ts_ms from days/weeks ago → lag_ms spikes → p99 > 100ms.
#
# Usage:
#   bash scripts/pel_cleanup.sh
#   bash scripts/pel_cleanup.sh --dry-run
#   bash scripts/pel_cleanup.sh --idle-ms 120000   # custom idle threshold
#
# Safe to run while workers are live. Cleans only idle consumers.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Config ───────────────────────────────────────────────────────────────────
REDIS_CONTAINER="${REDIS_TICKS_CONTAINER:-redis-ticks}"
IDLE_THRESHOLD_MS="${PEL_IDLE_THRESHOLD_MS:-60000}"   # 60s default
DRY_RUN=0
VERBOSE=0

# Symbols → groups to clean (extend as needed)
declare -A STREAM_GROUPS=(
  ["stream:tick_BTCUSDT"]="crypto-of:BTCUSDT"
  ["stream:tick_ETHUSDT"]="crypto-of:ETHUSDT"
  ["stream:tick_SOLUSDT"]="crypto-of:SOLUSDT"
  ["stream:tick_XRPUSDT"]="crypto-of:XRPUSDT"
  ["stream:tick_BNBUSDT"]="crypto-of:BNBUSDT"
  ["stream:tick_AVAXUSDT"]="crypto-of:AVAXUSDT"
  ["stream:tick_DOGEUSDT"]="crypto-of:DOGEUSDT"
  ["stream:tick_LINKUSDT"]="crypto-of:LINKUSDT"
)

# ── Argument parsing ──────────────────────────────────────────────────────────
for arg in "$@"; do
  case "$arg" in
    --dry-run)    DRY_RUN=1 ;;
    --verbose)    VERBOSE=1 ;;
    --idle-ms=*)  IDLE_THRESHOLD_MS="${arg#*=}" ;;
  esac
done

REDIS="docker exec ${REDIS_CONTAINER} redis-cli"

# ── Helpers ───────────────────────────────────────────────────────────────────
log()     { echo "[pel_cleanup] $*"; }
verbose() { [ "$VERBOSE" -eq 1 ] && echo "  [verbose] $*" || true; }

redis_cmd() {
  if [ "$DRY_RUN" -eq 1 ]; then
    verbose "DRY-RUN: $REDIS $*"
    return 0
  fi
  $REDIS "$@" > /dev/null 2>&1
}

# ── Main ──────────────────────────────────────────────────────────────────────
[ "$DRY_RUN" -eq 1 ] && log "DRY-RUN mode: no changes will be made"
log "Idle threshold: ${IDLE_THRESHOLD_MS}ms | Redis container: ${REDIS_CONTAINER}"
echo ""

TOTAL_CONSUMERS=0
TOTAL_PEL_PENDING=0
TOTAL_ZOMBIES=0

for STREAM in "${!STREAM_GROUPS[@]}"; do
  GROUP="${STREAM_GROUPS[$STREAM]}"

  # Check if stream/group exists
  if ! $REDIS EXISTS "$STREAM" > /dev/null 2>&1; then
    verbose "Stream $STREAM not found, skipping"
    continue
  fi

  # Get total consumers + pending before
  STATS=$($REDIS XINFO GROUPS "$STREAM" 2>/dev/null | \
    awk -v grp="$GROUP" '
      /^name$/{getline n}
      /^consumers$/{getline c}
      /^pending$/{getline p}
      n==grp{print c" "p; exit}
    ' || echo "0 0")
  CONSUMERS=$(echo "$STATS" | awk '{print $1}')
  PENDING=$(echo "$STATS" | awk '{print $2}')

  CONSUMERS="${CONSUMERS:-0}"
  PENDING="${PENDING:-0}"

  log "[$GROUP] consumers=${CONSUMERS} pending=${PENDING}"

  TOTAL_CONSUMERS=$((TOTAL_CONSUMERS + CONSUMERS))
  TOTAL_PEL_PENDING=$((TOTAL_PEL_PENDING + PENDING))

  # Get zombie consumers
  ZOMBIES=$($REDIS XINFO CONSUMERS "$STREAM" "$GROUP" 2>/dev/null | \
    awk -v thr="$IDLE_THRESHOLD_MS" '
      /^name$/{getline n}
      /^pending$/{getline p}
      /^idle$/{getline i; if(i+0 >= thr+0) print n"|"p"|"i}
    ' || true)

  if [ -z "$ZOMBIES" ]; then
    verbose "  No zombie consumers to clean"
    continue
  fi

  COUNT=0
  PEL_CLEANED=0

  while IFS='|' read -r CONSUMER CPENDING CIDLE; do
    [ -z "$CONSUMER" ] && continue
    COUNT=$((COUNT + 1))
    TOTAL_ZOMBIES=$((TOTAL_ZOMBIES + 1))

    # ACK all pending entries for this consumer
    if [ "${CPENDING:-0}" -gt 0 ]; then
      PENDING_IDS=$($REDIS XPENDING "$STREAM" "$GROUP" - + 200 "$CONSUMER" 2>/dev/null | \
        awk '{print $1}' | grep -E "^[0-9]+-" || true)
      
      if [ -n "$PENDING_IDS" ]; then
        ID_COUNT=$(echo "$PENDING_IDS" | grep -c "^[0-9]" || echo 0)
        PEL_CLEANED=$((PEL_CLEANED + ID_COUNT))
        verbose "  ACK $ID_COUNT pending from $CONSUMER (idle=${CIDLE}ms)"
        
        if [ "$DRY_RUN" -eq 0 ]; then
          echo "$PENDING_IDS" | xargs -r $REDIS XACK "$STREAM" "$GROUP" > /dev/null 2>&1 || true
        fi
      fi
    fi

    verbose "  DELETE consumer: $CONSUMER (idle=${CIDLE}ms pending=${CPENDING})"
    redis_cmd XGROUP DELCONSUMER "$STREAM" "$GROUP" "$CONSUMER" || true
  done <<< "$ZOMBIES"

  log "  → Removed $COUNT zombie consumers | ACKed $PEL_CLEANED pending entries"
done

echo ""
log "── Summary ─────────────────────────────────────────────────────"
log "Total streams processed : ${#STREAM_GROUPS[@]}"
log "Total consumers before  : ${TOTAL_CONSUMERS}"
log "Total PEL pending before: ${TOTAL_PEL_PENDING}"
log "Total zombies removed   : ${TOTAL_ZOMBIES}"
[ "$DRY_RUN" -eq 1 ] && log "(DRY-RUN: no actual changes were made)"
log "──────────────────────────────────────────────────────────────── "
