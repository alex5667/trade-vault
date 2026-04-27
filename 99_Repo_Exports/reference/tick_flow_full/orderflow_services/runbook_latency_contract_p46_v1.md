# P4.6 — latency-contract deploy-lint exporter service + notifier

## Что добавлено
- systemd service для `latency_contract_deploy_lint_exporter_v1`
- oneshot service + timer для `latency_contract_deploy_lint_notifier_v1`
- Redis ops-event stream + Telegram summary при persistent deploy-lint drift

## Units
- `trade-latency-contract-deploy-lint-exporter.service`
- `trade-latency-contract-deploy-lint-notifier.service`
- `trade-latency-contract-deploy-lint-notifier.timer`

## Рекомендуемый запуск
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now trade-latency-contract-deploy-lint-exporter.service
sudo systemctl enable --now trade-latency-contract-deploy-lint-notifier.timer
```

## Redis keys / streams
- state: `metrics:latency_contract:deploy_lint:last:<purpose>`
- summary: `metrics:latency_contract:deploy_lint:summary:last`
- notifier state: `metrics:latency_contract:deploy_lint:notifier:last`
- ops stream: `ops:latency_contract:events:v1`
- telegram stream: `notify:telegram` or env override
