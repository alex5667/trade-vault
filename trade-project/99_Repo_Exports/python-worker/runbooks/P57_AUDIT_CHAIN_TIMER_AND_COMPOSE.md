# P5.7 — Execution audit-chain timer / compose wiring

## Цель
Сделать production-эксплуатацию P5.6 полной:
- checker запускается периодически без ручного участия
- JSON report и `.prom` публикуются в стабильные SoT-пути
- runbook server читает тот же JSON report
- compose и systemd используют один и тот же output contract

## SoT пути
| Артефакт | Путь |
|---|---|
| JSON report | `/var/lib/trade/runbooks/latest_execution_audit_chain.json` |
| Prometheus textfile | `/var/lib/node_exporter/textfile_collector/latest_execution_audit_chain.prom` |

---

## Вариант A — systemd (рекомендовано для host / node_exporter)

### Файлы
- `deploy/systemd/execution-audit-chain-check.service`
- `deploy/systemd/execution-audit-chain-check.timer`
- `deploy/systemd/execution-audit-runbook-server.service`
- `deploy/systemd/execution-audit-chain.env.example`

### Установка
```bash
sudo install -D -m 0644 deploy/systemd/execution-audit-chain-check.service \
    /etc/systemd/system/execution-audit-chain-check.service
sudo install -D -m 0644 deploy/systemd/execution-audit-chain-check.timer \
    /etc/systemd/system/execution-audit-chain-check.timer
sudo install -D -m 0644 deploy/systemd/execution-audit-runbook-server.service \
    /etc/systemd/system/execution-audit-runbook-server.service
sudo install -D -m 0644 deploy/systemd/execution-audit-chain.env.example \
    /etc/default/execution-audit-chain
# Создать директории SoT
sudo mkdir -p /var/lib/trade/runbooks /var/lib/node_exporter/textfile_collector
sudo systemctl daemon-reload
sudo systemctl enable --now execution-audit-chain-check.timer
sudo systemctl enable --now execution-audit-runbook-server.service
```

### Проверка
```bash
systemctl status execution-audit-chain-check.timer --no-pager
systemctl list-timers --all | grep execution-audit-chain
journalctl -u execution-audit-chain-check.service -n 100 --no-pager
curl -s http://127.0.0.1:8777/api/audit-chain/latest | jq .
```

---

## Вариант B — docker-compose (profiles: ops)

### Новые файлы
- `scripts/run_execution_audit_chain_scheduler.py` — long-running scheduler wrapper
- Два сервиса в `docker-compose-timers.yml`:
  - `execution-audit-chain-checker` (profile `ops`)
  - `execution-audit-runbook-server` (profile `ops`)

### Что важно
1. Scheduler **не** дублирует checker-логику — он загружает и вызывает тот же `check_execution_audit_chain.py`.
2. JSON и `.prom` пишутся в bind-mounted директории:
   - `./runtime/execution_audit_chain` → `/var/lib/trade/runbooks`
   - `./runtime/node_exporter_textfile` → `/var/lib/node_exporter/textfile_collector`
3. Если node_exporter живёт в другом compose/host stack — он должен читать ту же директорию textfile collector.

### Запуск
```bash
# Создать runtime директории (один раз)
mkdir -p runtime/execution_audit_chain runtime/node_exporter_textfile

# Запустить только ops-сервисы
COMPOSE_PROFILES=ops docker compose -f docker-compose-timers.yml up -d \
    execution-audit-chain-checker execution-audit-runbook-server

# Проверить
docker logs scanner-execution-audit-chain-checker --tail 50
curl -s http://127.0.0.1:8777/api/audit-chain/latest | jq .
```

---

## Метрики / алерты
`.prom` пишет те же P5.6 метрики — новый формат не вводился.
SoT rules bundle: `python-worker/monitoring/prometheus_rules_execution_p56_audit_chain.yml`

---

## Диагностика

### 1) JSON есть, а `.prom` нет или пустой
```
проверить права на ./runtime/node_exporter_textfile
проверить EXEC_AUDIT_REPORT_PROM в env контейнера
```

### 2) report stale — freshness > 900s
```
docker ps | grep execution-audit-chain-checker   # должен быть Up
docker logs scanner-execution-audit-chain-checker --tail 100
# Вероятные причины: невалидный TRADES_DB_DSN, ошибка SQL
```

### 3) runbook endpoint возвращает {} или 404
```
# Убедиться, что EXEC_AUDIT_REPORT_JSON у server и checker совпадает
# Убедиться, что bind mount ./runtime/execution_audit_chain видится обоим
```

### 4) systemd timer не запускается
```
systemctl list-timers --all | grep execution-audit
journalctl -u execution-audit-chain-check --since "1h ago"
```

---

## Rollback
- **compose**: `COMPOSE_PROFILES=ops docker compose -f docker-compose-timers.yml stop execution-audit-chain-checker execution-audit-runbook-server`
- **systemd**: `sudo systemctl disable --now execution-audit-chain-check.timer execution-audit-runbook-server.service`
- JSON и `.prom` можно оставить как last-known snapshot — они не мешают работе
