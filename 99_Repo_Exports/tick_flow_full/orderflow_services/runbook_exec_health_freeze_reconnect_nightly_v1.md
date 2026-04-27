# ExecHealth Nightly Reconnect Chaos Smoke (P17)

## Goal

Run the P16 reconnect chaos smoke automatically every night on staging / infra host so the self-healing path is exercised regularly, not only during incident response.

## What the nightly job validates

Repairable reconnect drift:
- representative writer client returns with expected `CLIENT SETNAME`
- representative audit client returns with expected `CLIENT SETINFO LIB-NAME`
- representative bootstrap client also recovers when enabled
- recovery event is emitted
- `recovery_total` in heal-state increases

Non-repairable path:
- `wrong_user` does **not** self-heal
- scenario remains on violation path

## Files

- Compose job:
  - `orderflow_services/deploy/docker-compose.exec-health-freeze-reconnect-nightly-v1.yml`
- systemd service:
  - `orderflow_services/deploy/systemd/exec-health-freeze-reconnect-nightly.service`
- systemd timer:
  - `orderflow_services/deploy/systemd/exec-health-freeze-reconnect-nightly.timer`
- Wrapper:
  - `orderflow_services/deploy/systemd/run-exec-health-freeze-reconnect-nightly-v1.sh`
- Runner:
  - `orderflow_services/exec_health_freeze_reconnect_nightly_smoke_v1.py`

## Manual run

```bash
python -m orderflow_services.exec_health_freeze_reconnect_nightly_smoke_v1
```

Expected outputs:
- report JSON: `/var/lib/trade/exec_health_reconnect_smoke/latest_report.json`
- textfile metric: `/var/lib/node_exporter/textfile_collector/exec_health_freeze_reconnect_smoke.prom`

## systemd install

```bash
sudo cp orderflow_services/deploy/systemd/exec-health-freeze-reconnect-nightly.service /etc/systemd/system/
sudo cp orderflow_services/deploy/systemd/exec-health-freeze-reconnect-nightly.timer /etc/systemd/system/
```

Copy wrapper too:

```bash
sudo install -m 0755 orderflow_services/deploy/systemd/run-exec-health-freeze-reconnect-nightly-v1.sh \
  /opt/scanner_infra/orderflow_services/deploy/systemd/run-exec-health-freeze-reconnect-nightly-v1.sh
```

Create environment file `/etc/default/exec-health-freeze-reconnect-nightly`:

```bash
EXEC_HEALTH_REPO_ROOT=/opt/scanner_infra
EXEC_HEALTH_RECONNECT_SMOKE_REPORT_DIR=/var/lib/trade/exec_health_reconnect_smoke
EXEC_HEALTH_RECONNECT_SMOKE_TEXTFILE_DIR=/var/lib/node_exporter/textfile_collector
REDIS_URL=redis://exec_health_freeze_writer:<pass>@redis-worker-1:6379/0
EXEC_HEALTH_REDIS_AUDIT_URL=redis://exec_health_freeze_audit:<pass>@redis-worker-1:6379/0
EXEC_HEALTH_REDIS_BOOTSTRAP_URL=redis://exec_health_freeze_bootstrap:<pass>@redis-worker-1:6379/0
EXEC_HEALTH_REDIS_WRONG_USER_URL=redis://default:<pass>@redis-worker-1:6379/0
```

Reload and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now exec-health-freeze-reconnect-nightly.timer
```

## Troubleshooting

Inspect last timer runs:

```bash
systemctl list-timers --all | grep exec-health-freeze-reconnect-nightly
journalctl -u exec-health-freeze-reconnect-nightly.service -n 200 --no-pager
```

Inspect last report:

```bash
cat /var/lib/trade/exec_health_reconnect_smoke/latest_report.json
cat /var/lib/node_exporter/textfile_collector/exec_health_freeze_reconnect_smoke.prom
```

- alert if nightly smoke did not succeed within 36h
- alert if last run failed
- alert if rollout/apply gate is latched ACTIVE

## Rollout Gate and manual ack (P18)

If any nightly smoke run fails, it latches the **Rollout Gate** in Redis.
This gate blocks `ACL apply` and `commit-thaw` operations to prevent deployment of potentially broken self-healing configurations.

To check gate status:
```bash
python -m orderflow_services.exec_health_freeze_reconnect_rollout_gate_v1 status
```

If the failure was investigated and resolved (or determined to be a false positive), the operator must manually acknowledge (clear) the gate:
```bash
python -m orderflow_services.exec_health_freeze_reconnect_rollout_gate_v1 ack \
  --operator bob \
  --reason "investigated: transient redis-worker-1 networking issue resolved"
```

Only after this `ack` will the `ACL apply` and `commit-thaw` commands become available again.
