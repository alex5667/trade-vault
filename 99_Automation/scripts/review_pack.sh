#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: review_pack.sh <pack.md> [task] [model]"
  exit 1
fi

PACK="$1"
TASK="${2:-Сделай production review в формате: Goal / Facts / Assumptions / Risks / Plan / Tests / Metrics/Alerts / Rollout/Rollback.}"
MODEL="${3:-deepseek-r1:14b}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/review_pack.py" "$PACK" "$TASK" --model "$MODEL"
