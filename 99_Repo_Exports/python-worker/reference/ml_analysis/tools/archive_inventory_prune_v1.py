import argparse
import json
import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_dir_size_gb(directory: Path) -> float:
    """Calculate total size of directory in GB."""
    total_size = sum(f.stat().st_size for f in directory.glob('**/*') if f.is_file())
    return total_size / (1024**3)

def prune_archives(
    archive_dir: str,
    retention_days: int = 30,
    keep_last_days: int = 3,
    max_total_gb: float = 100.0,
    dry_run: bool = False
) -> dict[str, Any]:
    """Prune old archives based on age and total size occupancy."""
    path = Path(archive_dir)
    if not path.exists():
        logger.error(f"Directory {archive_dir} does not exist")
        return {"error": "not_found"}

    now = datetime.now(UTC)
    cutoff_date = now - timedelta(days=retention_days)
    keep_last_cutoff = now - timedelta(days=keep_last_days)

    files = sorted(list(path.glob('*.ndjson*')), key=os.path.getmtime)
    deleted_count = 0
    deleted_size_gb = 0.0

    # Prune by age
    for f in files:
        mtime = datetime.fromtimestamp(f.stat().st_mtime, UTC)
        if mtime < cutoff_date:
            # Check if it's protected by keep_last (shouldn't be if retention > keep_last)
            if mtime > keep_last_cutoff:
                continue

            size_gb = f.stat().st_size / (1024**3)
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Pruning old file: {f.name} (age: {(now - mtime).days} days, size: {size_gb:.4f} GB)")
            if not dry_run:
                f.unlink()
            deleted_count += 1
            deleted_size_gb += size_gb

    # Re-evaluate files after age-based pruning
    files = sorted(list(path.glob('*.ndjson*')), key=os.path.getmtime)
    current_size_gb = get_dir_size_gb(path)

    # Prune by total size if still exceeding limit
    if current_size_gb > max_total_gb:
        logger.info(f"Directory size {current_size_gb:.2f} GB exceeds limit {max_total_gb:.2f} GB. Pruning more...")
        for f in files:
            mtime = datetime.fromtimestamp(f.stat().st_mtime, UTC)
            # Never prune files from 'keep_last' window unless strictly necessary?
            # Usually we respect keep_last as emergency buffer.
            if mtime > keep_last_cutoff:
                logger.warning(f"Skipping {f.name} - inside protected 'keep_last' window")
                continue

            size_gb = f.stat().st_size / (1024**3)
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Pruning for size: {f.name} ({size_gb:.4f} GB)")
            if not dry_run:
                f.unlink()
            deleted_count += 1
            deleted_size_gb += size_gb
            current_size_gb -= size_gb

            if current_size_gb <= max_total_gb:
                break

    return {
        "deleted_count": deleted_count,
        "deleted_size_gb": deleted_size_gb,
        "remaining_size_gb": current_size_gb
    }

def update_manifest(archive_dir: str):
    """Create/update a manifest file with list of archives and their time ranges."""
    path = Path(archive_dir)
    manifest_path = path / "manifest.json"

    files = sorted(list(path.glob('*.ndjson*')), key=os.path.getmtime)
    inventory = []

    for f in files:
        if f.name == "manifest.json":
            continue
        mtime = datetime.fromtimestamp(f.stat().st_mtime, UTC)
        inventory.append({
            "name": f.name,
            "size_bytes": f.stat().st_size,
            "mtime": mtime.isoformat()
        })

    manifest = {
        "updated_at": datetime.now(UTC).isoformat(),
        "total_files": len(inventory),
        "total_size_gb": sum(i['size_bytes'] for i in inventory) / (1024**3),
        "inventory": inventory
    }

    # Atomic write
    tmp_path = manifest_path.with_suffix(".tmp")
    with open(tmp_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    os.replace(tmp_path, manifest_path)
    logger.info(f"Manifest updated: {len(inventory)} files, {manifest['total_size_gb']:.2f} GB")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Archive Inventory and Prune Tool")
    parser.add_argument("--dir", required=True, help="Archive directory")
    parser.add_argument("--retention-days", type=int, default=30, help="Days to keep files")
    parser.add_argument("--keep-last-days", type=int, default=3, help="Days to always keep (protection)")
    parser.add_argument("--max-gb", type=float, default=100.0, help="Max total archive size in GB")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually delete files")

    args = parser.parse_args()

    logger.info(f"Starting maintenance on {args.dir}")
    stats = prune_archives(
        args.dir,
        retention_days=args.retention_days,
        keep_last_days=args.keep_last_days,
        max_total_gb=args.max_gb,
        dry_run=args.dry_run
    )

    if "error" not in stats:
        logger.info(f"Maintenance finished. Deleted: {stats['deleted_count']} files ({stats['deleted_size_gb']:.4f} GB). Remaining: {stats['remaining_size_gb']:.2f} GB")
        update_manifest(args.dir)
