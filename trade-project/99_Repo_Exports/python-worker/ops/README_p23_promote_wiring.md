# P23: Model Promotion Wiring (Integrated)

This module provides infrastructure for managing promoted models, including retention policies and health checks.

## Features Added

1. **Retention (`cleanup_promoted_models_v1.py`)**
   - Automatically cleans up old model artifacts in `META_PROMOTE_DIR`.
   - Default policy: Keep last 80 models, or any model newer than 14 days.
   - Runs daily via `meta-promote-retention-timer` service.

2. **Health Check (`meta_promote_dir_check_v1.py`)**
   - Monitors `META_PROMOTE_DIR` for existence, writability, and free space.
   - Exports Prometheus metrics to node_exporter textfile collector.
   - Runs every minute via `meta-promote-check-timer` service.

3. **Alerts (`alerts_meta_promote.yml`)**
   - `MetaPromoteDirNotOk`: Triggers if directory check fails.
   - `MetaPromoteDirLowFreeSpace`: Triggers if free space < 5%.

## Manual Execution

To run retention manually (dry-run):
```bash
docker-compose -f docker-compose-timers.yml run --rm meta-promote-retention-timer python3 tools/cleanup_promoted_models_v1.py --dry-run
```

To run health check manually:
```bash
docker-compose -f docker-compose-timers.yml run --rm meta-promote-check-timer python3 tools/meta_promote_dir_check_v1.py --out /dev/stdout
```

## Configuration

Settings correspond to `docker-compose-timers.yml` environment variables:
- `META_PROMOTE_DIR`: Path to promoted models directory.
- `META_PROMOTE_MODEL=1`: Feature flag for nightly pipeline.
