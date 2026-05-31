# P6 systemd units

Install the files in this directory under `/etc/systemd/system/` and provide an
environment file at `/etc/trade/trade-execution.env` (see `config/env.execution-p6.example`).

## Quickstart

```bash
# Copy units
sudo cp trade-execution-backfill.service   /etc/systemd/system/
sudo cp trade-execution-backfill.timer     /etc/systemd/system/
sudo cp trade-execution-healthcheck.service /etc/systemd/system/
sudo cp trade-execution-healthcheck.timer  /etc/systemd/system/
sudo cp trade-runbook-server.service       /etc/systemd/system/

# Create env file (copy example and fill in real DSN / URLs)
sudo mkdir -p /etc/trade
sudo cp config/env.execution-p6.example /etc/trade/trade-execution.env
# vim /etc/trade/trade-execution.env  ← set EXECUTION_JOURNAL_DSN

# Enable & start
sudo systemctl daemon-reload
sudo systemctl enable --now trade-execution-healthcheck.timer
sudo systemctl enable --now trade-execution-backfill.timer
sudo systemctl enable --now trade-runbook-server.service
```

## Timer behaviour

| Timer | Cadence | On restart |
|-------|---------|-----------|
| `trade-execution-healthcheck.timer` | every 1 min (first: boot+2min) | Persistent=true |
| `trade-execution-backfill.timer`    | every 15 min                   | Persistent=true |

The healthcheck timer writes JSON reports into `${RUNBOOK_REPORT_DIR}` so the
static runbook server can display the latest consistency status at `/api/health/latest`.

## Checking status

```bash
systemctl status trade-execution-healthcheck.timer
journalctl -u trade-execution-healthcheck.service -n 20
systemctl status trade-runbook-server.service
curl http://localhost:18080/healthz
```
