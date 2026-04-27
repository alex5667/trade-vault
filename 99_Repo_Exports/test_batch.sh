#!/bin/bash
DC_WITH_TRAILING="docker compose -f docker-compose.yml -f docker-compose.auto-apply-guard-exporter.yml -f docker-compose.tp-trailing.yml -f docker-compose.mt5-executor.yml -f docker-compose.tb-labeler.yml"
SERVICES=$($DC_WITH_TRAILING --profile default config --services 2>/dev/null)
echo "$SERVICES" | xargs -n 15 > .build_batches.tmp
while read -r batch; do
    echo "batch: $batch"
done < .build_batches.tmp
rm -f .build_batches.tmp
