# Confidence Score Guardrails - Two-Phase Promo Setup

This directory contains systemd units to orchestrate the Confidence Score Guardrails with a two-phase promotion system (Stage -> Promote) for safety gates.

## Components

1.  **Stage Phase**: `conf-score-guardrails-stage.service` + `.timer`.
    - Runs the apply script in `--stage 1` mode every 3 hours.
    - Writes candidate overrides to `staged.json` and a staged Redis prefix (`cfg:crypto_of:overrides_staged:`).
    - Does **not** affect live trading.
2.  **Promote Phase**: `conf-score-guardrails-promote.service` + `.timer`.
    - Periodically checks health gates (freshness, no degradation, ECE/Brier margins, min sample size).
    - If healthy, promotes the staged bundle to `current.json` and applies overrides to live Redis keys (`cfg:crypto_of:overrides:`).
3.  **Exporter**: `conf-score-guardrails-exporter.service` (Optional, as Docker exporter might be used).
    - Exposes state metrics and bundle/promotion status to Prometheus (port 9135).

## Setup

1.  **Copy files**:
    ```bash
    sudo cp deploy/systemd/conf-score-guardrails-* /etc/systemd/system/
    ```

2.  **Configure Environment**:
    ```bash
    # Create or use an existing env file
    # Example: sudo nano /etc/default/conf-score-guardrails.env
    ```

3.  **Reload Daemon**:
    ```bash
    sudo systemctl daemon-reload
    ```

4.  **Enable and Start**:
    ```bash
    # If running outside Docker:
    # sudo systemctl enable --now conf-score-guardrails-stage.timer
    # sudo systemctl enable --now conf-score-guardrails-promote.timer
    ```

## Manual Operations

### Force Promote
To manually trigger a promotion check:
```bash
sudo systemctl start conf-score-guardrails-promote.service
```

### Rollback
To perform a rollback manually:
```bash
python3 -m orderflow_services.conf_score_guardrails_bundle_rollback_v1 \
    --bundle-dir /path/to/bundles \
    --target prev \
    --apply 1 \
    --promote-pointer 1
```
