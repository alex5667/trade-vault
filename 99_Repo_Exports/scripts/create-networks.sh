#!/usr/bin/env bash
# create-networks.sh — idempotent creation of segmented Docker networks
# Called from `make up` before any compose operations.
set -euo pipefail

NETWORKS=(
  "scanner-infra:172.24.0.0/20"
  "scanner-core:172.24.16.0/20"
  "scanner-timers:172.24.32.0/20"
  "scanner-ops:172.24.48.0/20"
)

for entry in "${NETWORKS[@]}"; do
  NAME="${entry%%:*}"
  SUBNET="${entry##*:}"
  if docker network ls --format '{{.Name}}' | grep -qx "$NAME"; then
    echo "✅ $NAME already exists"
  else
    docker network create --subnet="$SUBNET" "$NAME" >/dev/null
    echo "🔧 Created $NAME ($SUBNET)"
  fi
done

# scanner-network is now managed by Docker Compose (name: scanner-network)
# No need to create it externally — compose handles DNS aliases automatically.

if docker network ls --format '{{.Name}}' | grep -qx "scanner_infra_default"; then
  echo "✅ scanner_infra_default already exists"
else
  docker network create scanner_infra_default >/dev/null
  echo "🔧 Created scanner_infra_default"
fi
