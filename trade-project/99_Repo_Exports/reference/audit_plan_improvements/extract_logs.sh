#!/usr/bin/env bash
mkdir -p /home/alex/front/trade/scanner_infra/reference/audit_plan_improvements/logs
docker logs --since 30d binance-executor > /home/alex/front/trade/scanner_infra/reference/audit_plan_improvements/logs/executor_30d.log 2>/dev/null || true
docker logs --since 30d binance-executor 2>&1 \
  | grep -E "ERROR|WARN|-4120|-1021|-2021|HTTP 503|HTTP 429|HTTP 418" \
  > /home/alex/front/trade/scanner_infra/reference/audit_plan_improvements/logs/executor_30d_errors.log || true
echo "[OK] Logs export"
