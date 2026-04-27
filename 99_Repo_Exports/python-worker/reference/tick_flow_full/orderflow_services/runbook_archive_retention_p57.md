# Runbook: P57 Archive Retention & Dataset Fallback

## Overview
P57 introduces managed retention for Replay Input archives and enhances the ML Dataset Builder to use these archives when Redis history is insufficient.

## Components
1.  **Retention Tool**: `ml_analysis.tools.archive_inventory_prune_v1`
2.  **Archive Reader**: `ml_analysis.tools.replay_inputs_reader_v1`
3.  **Fallback Dataset Builder**: `ml_analysis.tools.build_edge_stack_dataset_fallback_v1`

## Operational Tasks

### 1. Manual Archive Maintenance
If disk space is low, you can manually trigger a prune:
```bash
docker exec scanner-of-timers-worker python3 -m ml_analysis.tools.archive_inventory_prune_v1 \
  --dir /var/lib/trade/of_inputs_archive \
  --retention-days 14 \
  --max-gb 50
```

### 2. Verifying Inventory
Check `manifest.json` in the archive directory or run:
```bash
docker exec scanner-of-timers-worker python3 -m ml_analysis.tools.archive_inventory_prune_v1 \
  --dir /var/lib/trade/of_inputs_archive --dry-run
```

### 3. Rebuilding Dataset from Archives
To build a large dataset that goes beyond Redis window (e.g., last 7 days):
```bash
docker exec scanner-edge-stack-v1-dataset-timer python3 -m ml_analysis.tools.build_edge_stack_dataset_fallback_v1 \
  --archive_dir /var/lib/trade/of_inputs_archive \
  --signals_count 1000000 \
  --closes_count 1000000
```

## Configuration (ENV)
- `ARCHIVE_DIR`: Path to archives (default: `/var/lib/trade/of_inputs_archive`)
- `ARCHIVE_RETENTION_DAYS`: Auto-delete after X days (default: 30)
- `ARCHIVE_MAX_TOTAL_GB`: Hard cap on disk usage (default: 100)

## Troubleshooting
- **No data in dataset**: Check if `sid` (Signal ID) is present in both Redis streams (`signals:of:inputs` and `trades:closed`).
- **Archive reader slow**: Ensure archives are gzipped and the reader has sufficient RAM if reading very large files.
- **Disk Full**: The pruning task runs daily. If disk fills up faster, decrease `ARCHIVE_MAX_TOTAL_GB`.
