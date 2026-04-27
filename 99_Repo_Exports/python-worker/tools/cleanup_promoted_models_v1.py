import os
import sys
import argparse
import logging
import glob
import shutil
import time
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def cleanup_promoted_models(promote_dir, keep_last=80, keep_days=14, dry_run=False):
    """
    Cleans up old promoted model artifacts in promote_dir.
    
    Retention policy:
    1. Keep at least 'keep_last' most recent files (sorted by mtime).
    2. Keep files newer than 'keep_days'.
    
    Everything else is deleted.
    Safe deletion: only deletes files matching 'meta_model_*' pattern and strictly inside promote_dir.
    """
    if not os.path.exists(promote_dir):
        logger.error(f"Promote directory does not exist: {promote_dir}")
        return False

    if not os.path.isdir(promote_dir):
        logger.error(f"Promote path is not a directory: {promote_dir}")
        return False

    # 1. Gather all artifacts
    # Pattern: meta_model_<schema>_YYYYmmdd_HHMMSS_sha12.json 
    # We'll just match meta_model_*.json to be safe but flexible
    pattern = os.path.join(promote_dir, "meta_model_*.json")
    files = glob.glob(pattern)
    
    if not files:
        logger.info(f"No promoted models found in {promote_dir}. Nothing to clean.")
        return True

    # Sort by modification time, newest first
    files_sorted = sorted(files, key=os.path.getmtime, reverse=True)
    
    logger.info(f"Found {len(files_sorted)} total artifacts in {promote_dir}")

    # 2. Apply keep-last policy
    # The first 'keep_last' files are safe
    safe_files = set(files_sorted[:keep_last])
    logger.info(f"Keeping top {len(safe_files)} files based on keep-last={keep_last}")

    # 3. Apply keep-days policy for the rest
    now = time.time()
    cutoff_time = now - (keep_days * 86400)
    
    files_to_delete = []
    
    for fpath in files_sorted:
        if fpath in safe_files:
            continue
            
        mtime = os.path.getmtime(fpath)
        if mtime >= cutoff_time:
            # File is recent enough, keep it
            continue
        
        # If we are here, it's not in top-N and it's older than keep-days -> delete
        files_to_delete.append(fpath)

    logger.info(f"Identified {len(files_to_delete)} files for deletion (keep-last={keep_last}, keep-days={keep_days})")

    if not files_to_delete:
        logger.info("No files to delete.")
        return True

    # 4. Perform deletion
    deleted_count = 0
    errors = 0
    
    for fpath in files_to_delete:
        base_name = os.path.basename(fpath)
        
        # Double check safety: must be in promote_dir (already handled by glob but just in case)
        if os.path.dirname(fpath) != promote_dir:
            logger.warning(f"SKIPPING suspicious path not directly in promote_dir: {fpath}")
            continue
            
        if dry_run:
            logger.info(f"[DRY-RUN] Would delete: {base_name}")
            deleted_count += 1
        else:
            try:
                os.remove(fpath)
                logger.info(f"Deleted: {base_name}")
                deleted_count += 1
            except Exception as e:
                logger.error(f"Failed to delete {fpath}: {e}")
                errors += 1

    logger.info(f"Cleanup complete. Deleted: {deleted_count}, Errors: {errors}")
    return errors == 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup old promoted model artifacts.")
    
    # Defaults from requirements
    default_dir = os.environ.get("META_PROMOTE_DIR", "/var/lib/trade/meta_promote")
    
    parser.add_argument("--promote-dir", default=default_dir, help="Directory containing promoted models")
    parser.add_argument("--keep-last", type=int, default=80, help="Minimum number of recent artifacts to keep (default: 80)")
    parser.add_argument("--keep-days", type=int, default=14, help="Retention period in days (default: 14)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be deleted without deleting")
    
    args = parser.parse_args()
    
    logger.info(f"Starting cleanup with: promote_dir={args.promote_dir}, keep_last={args.keep_last}, keep_days={args.keep_days}, dry_run={args.dry_run}")
    
    success = cleanup_promoted_models(
        promote_dir=args.promote_dir,
        keep_last=args.keep_last,
        keep_days=args.keep_days,
        dry_run=args.dry_run
    )
    
    if not success:
        sys.exit(1)
