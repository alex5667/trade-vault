#!/bin/bash
# Start the Antigravity Task Watcher on the host.
# Watches Redis for new tasks and writes them to tasks/inbox.md.
#
# Usage:
#   bash scripts/start_task_watcher.sh
#
# The watcher sends desktop notifications when new tasks arrive.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Load project .env (for Redis password)
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

REDIS_PASS="${GO_GATEWAY_REDIS_PASS:-}"
REDIS_PORT="${REDIS_EXTERNAL_PORT:-6379}"
INBOX_KEY="${ANTIGRAVITY_INBOX_KEY:-antigravity:inbox}"
OUTPUT="${ANTIGRAVITY_INBOX_FILE:-tasks/inbox.md}"

REDIS_URL="redis://go_gateway:${REDIS_PASS}@127.0.0.1:${REDIS_PORT}/0"

echo "🔭 Starting Antigravity Task Watcher..."
echo "   output: ${OUTPUT}"
echo "   redis:  127.0.0.1:${REDIS_PORT}"
echo "   key:    ${INBOX_KEY}"

exec python3 scripts/antigravity_task_watcher.py \
    --redis-url "$REDIS_URL" \
    --inbox-key "$INBOX_KEY" \
    --output "$OUTPUT" \
    --poll-interval 5
